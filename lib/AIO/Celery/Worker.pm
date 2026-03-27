package AIO::Celery::Worker::Semaphore;
use Moo;
use Types::Standard qw(Int ArrayRef);
use Future;
use namespace::clean;

has max_concurrent => (is => 'ro', isa => Int, required => 1);
has _current       => (is => 'rw', isa => Int, default  => 0);
has _waiters       => (is => 'ro', default    => sub { [] });

sub acquire {
    my ($self) = @_;
    if ($self->_current < $self->max_concurrent) {
        $self->_current($self->_current + 1);
        return Future->done;
    }
    my $f = Future->new;
    push @{$self->_waiters}, $f;
    return $f;
}

sub release {
    my ($self) = @_;
    if (@{$self->_waiters}) {
        my $f = shift @{$self->_waiters};
        $f->done unless $f->is_ready;
    } else {
        $self->_current($self->_current - 1);
    }
}

sub active_count {
    my ($self) = @_;
    return $self->_current;
}

package AIO::Celery::Worker;
use strict;
use warnings;
use Future::AsyncAwait;
use Future;
use IO::Async::Loop;
use JSON::MaybeXS qw(encode_json decode_json);
use Log::Any qw($log);
use Carp qw(croak);
use POSIX qw(strftime);
use Scalar::Util qw(blessed);

use AIO::Celery::Request;
use AIO::Celery::Task;
use AIO::Celery::Context qw($CURRENT_TASK_ID $CURRENT_ROOT_ID);
use AIO::Celery::State qw(
    set_current_app get_current_app
    %RUNNING_TASKS $TASKS_SINCE_LAST_GC
);
use AIO::Celery::Exceptions;
use AIO::Celery::Utils qw(generate_consumer_tag);

sub _find_app_instance {
    my ($locator) = @_;

    # Format: Module::Name:variable
    my ($module, $varname) = split(/:/, $locator, 2);
    croak "Invalid app locator '$locator'. Expected Module::Name:variable"
        unless $module && $varname;

    # Convert Module::Name to Module/Name.pm for require
    (my $path = $module) =~ s{::}{/}g;
    $path .= '.pm';

    eval { require $path } or croak "Cannot load module '$module': $@";

    # Access the variable from the module's namespace
    my $app;
    {
        no strict 'refs';
        $app = ${$module . '::' . $varname};
    }

    croak "Variable '\$$varname' not found or undefined in $module"
        unless defined $app;
    croak "Variable '\$$varname' in $module is not an AIO::Celery::App instance"
        unless blessed($app) && $app->isa('AIO::Celery::App');

    return $app;
}

sub _print_banner {
    my (%args) = @_;
    my $app         = $args{app};
    my $queues      = $args{queues};
    my $concurrency = $args{concurrency};

    my $task_list = join("\n", map { "  . $_" } $app->list_registered_task_names);
    $task_list ||= '  (none)';

    my $queue_list = join(', ', @$queues);

    print <<"BANNER";

 -------------- aio-celery-perl v$AIO::Celery::VERSION
--- ***** -----
-- ******* ---- Perl $^V
- *** --- * ---
- ** ---------- [config]
- ** ---------- .> app:         @{[$app->name // 'default']}
- ** ---------- .> broker:      @{[$app->conf->broker_url]}
- ** ---------- .> result:      @{[$app->conf->result_backend // 'disabled']}
- *** --- * --- .> concurrency: $concurrency
-- ******* ---- .> queues:      $queue_list
--- ***** -----
 -------------- [tasks]
$task_list

BANNER
}

async sub _handle_task_success {
    my (%args) = @_;
    my $task   = $args{task};
    my $result = $args{result};
    my $app    = $args{app};

    $log->info(sprintf("Task %s[%s] succeeded", $task->name, $task->request->id));

    # Store SUCCESS state
    await $task->update_state(
        state     => 'SUCCESS',
        meta      => $result,
        _finalize => 1,
    );

    # Handle task chaining — publish next task in chain
    my ($next_message, $routing_key) = $task->build_next_task_message($result);
    if ($next_message) {
        $log->debug("Publishing chained task to $routing_key");
        await $app->broker->publish_message($next_message, routing_key => $routing_key);
    }
}

async sub _handle_task_failure {
    my (%args) = @_;
    my $task  = $args{task};
    my $error = $args{error};

    my $err_str = "$error";
    $log->error(sprintf("Task %s[%s] failed: %s", $task->name, $task->request->id, $err_str));

    await $task->update_state(
        state      => 'FAILURE',
        meta       => { exc_type => ref($error) || 'Error', exc_message => $err_str },
        _finalize  => 1,
        _traceback => $err_str,
    );
}

async sub _handle_task_retry {
    my (%args) = @_;
    my $task      = $args{task};
    my $retry_exc = $args{retry_exc};
    my $app       = $args{app};
    my $annotated = $args{annotated_task};

    my $max_retries = $annotated->max_retries;
    my $retries     = $task->request->retries;

    if (defined $max_retries && $retries >= $max_retries) {
        $log->error(sprintf("Task %s[%s] max retries (%d) exceeded",
            $task->name, $task->request->id, $max_retries));
        await $task->update_state(
            state     => 'FAILURE',
            meta      => { exc_type => 'MaxRetriesExceededError', exc_message => 'Max retries exceeded' },
            _finalize => 1,
        );
        return;
    }

    $log->info(sprintf("Task %s[%s] retrying in %ss (attempt %d)",
        $task->name, $task->request->id, $retry_exc->delay, $retries + 1));

    await $task->update_state(state => 'RETRY', meta => {});

    # Republish the retry message
    my $retry_msg = $retry_exc->amqp_message;
    my $routing_key = $retry_exc->routing_key || $app->conf->task_default_queue;
    await $app->broker->publish_message($retry_msg, routing_key => $routing_key);
}

async sub _execute_task_with_timeout {
    my (%args) = @_;
    my $annotated      = $args{annotated_task};
    my $task_args      = $args{args};
    my $task_kwargs    = $args{kwargs};
    my $soft_time_limit = $args{soft_time_limit};
    my $task_obj       = $args{task};
    my $loop           = $args{loop};

    # Build the argument list
    my @call_args;
    if ($annotated->bind) {
        push @call_args, $task_obj;
    }
    push @call_args, @$task_args;
    if (%$task_kwargs) {
        push @call_args, %$task_kwargs;
    }

    my $result_future;
    my $fn_result = $annotated->fn->(@call_args);

    # If the function returns a Future, await it; otherwise wrap in a done Future
    if (blessed($fn_result) && $fn_result->isa('Future')) {
        $result_future = $fn_result;
    } else {
        $result_future = Future->done($fn_result);
    }

    # Apply soft time limit
    if (defined $soft_time_limit && $soft_time_limit > 0) {
        my $timeout_f = $loop->delay_future(after => $soft_time_limit)->then(sub {
            Future->fail(
                AIO::Celery::Exception::Timeout->new(
                    message => "Soft time limit ($soft_time_limit s) exceeded",
                ),
            );
        });
        $result_future = Future->wait_any($result_future, $timeout_f);
    }

    return await $result_future;
}

async sub on_message_received {
    my (%args) = @_;
    my $message    = $args{message};
    my $app        = $args{app};
    my $semaphore  = $args{semaphore};
    my $loop       = $args{loop};

    my $headers = $message->{headers} // {};
    my $task_id   = $headers->{id};
    my $task_name = $headers->{task};

    unless ($task_id && $task_name) {
        $log->error("Received message without id or task header, rejecting");
        return;
    }

    # Look up the registered task
    my $annotated;
    eval { $annotated = $app->get_annotated_task($task_name) };
    if ($@) {
        $log->error("Received unknown task '$task_name', rejecting: $@");
        return;
    }

    # Parse the request
    my $request = AIO::Celery::Request->from_message($message);

    # Create the Task execution object
    my $task = AIO::Celery::Task->new(
        app                 => $app,
        request             => $request,
        default_retry_delay => $annotated->default_retry_delay,
    );

    # Resolve soft time limit
    my $soft_time_limit = $annotated->soft_time_limit
        // $request->timelimit->[1]
        // $app->conf->task_soft_time_limit;

    # Register running task
    my $running = AIO::Celery::State::RunningTask->new(
        task_id         => $task_id,
        task_name       => $task_name,
        args            => $request->args,
        kwargs          => $request->kwargs,
        retries         => $request->retries,
        eta             => $request->eta ? "$request->{eta}" : undef,
        soft_time_limit => $soft_time_limit,
    );
    $RUNNING_TASKS{$task_id} = $running;

    # Set context variables
    local $CURRENT_TASK_ID  = $task_id;
    local $CURRENT_ROOT_ID  = $request->root_id;

    $log->info(sprintf("Received task: %s[%s]", $task_name, $task_id));

    # Wait for ETA if set
    if ($request->eta) {
        my $now = DateTime->now(time_zone => 'UTC');
        my $wait_seconds = $request->eta->epoch - $now->epoch;
        if ($wait_seconds > 0) {
            $running->state('SLEEPING');
            $log->debug("Sleeping ${wait_seconds}s until ETA for $task_id");
            await $loop->delay_future(after => $wait_seconds);
        }
    }

    # Acquire semaphore
    $running->state('SEMAPHORE');
    await $semaphore->acquire;
    $running->state('RUNNING');

    # Update state to STARTED
    eval { await $task->update_state(state => 'STARTED', meta => {}) };
    warn "Failed to update task state to STARTED: $@" if $@;

    my $exec_future = _execute_task_with_timeout(
        annotated_task  => $annotated,
        args            => $request->args,
        kwargs          => $request->kwargs,
        soft_time_limit => $soft_time_limit,
        task            => $task,
        loop            => $loop,
    );

    my ($success, $result, $error);
    eval {
        $result = await $exec_future;
        $success = 1;
    };
    $error = $@ unless $success;

    if ($success) {
        eval { await _handle_task_success(task => $task, result => $result, app => $app) };
        $log->error("Failed to handle success: $@") if $@;
    } elsif (blessed($error) && $error->isa('AIO::Celery::Exception::Retry')) {
        eval {
            await _handle_task_retry(
                task           => $task,
                retry_exc      => $error,
                app            => $app,
                annotated_task => $annotated,
            );
        };
        $log->error("Failed to handle retry: $@") if $@;
    } else {
        eval { await _handle_task_failure(task => $task, error => $error) };
        $log->error("Failed to handle failure: $@") if $@;
    }

    # Cleanup
    $semaphore->release;
    delete $RUNNING_TASKS{$task_id};
    $TASKS_SINCE_LAST_GC++;

    $log->debug("Task $task_id cleanup complete");
}

async sub run {
    my (%args) = @_;
    my $loop        = $args{loop}        // IO::Async::Loop->new;
    my $locator     = $args{locator}     // croak "locator is required";
    my $concurrency = $args{concurrency} // 10_000;
    my $queues      = $args{queues};
    my $loglevel    = $args{loglevel}    // 'INFO';

    # Load the application
    my $app = _find_app_instance($locator);
    my $conf = $app->conf;

    # Resolve queues
    $queues //= [$conf->task_default_queue];

    # Setup connections
    await $app->setup(loop => $loop);

    # Print banner
    _print_banner(app => $app, queues => $queues, concurrency => $concurrency);

    # Create semaphore
    my $semaphore = AIO::Celery::Worker::Semaphore->new(max_concurrent => $concurrency);

    # Setup consuming AMQP connection (separate from publishing)
    require Net::Async::AMQP;
    my $consumer_amqp = Net::Async::AMQP->new;
    $loop->add($consumer_amqp);

    my $uri = URI->new($conf->broker_url);
    my $vhost = $uri->path;
    $vhost =~ s{^/}{};
    $vhost = '/' if $vhost eq '' || $vhost eq '/';

    await $consumer_amqp->connect(
        host  => $uri->host  // 'localhost',
        port  => $uri->port  // 5672,
        user  => $uri->userinfo ? (split(/:/, $uri->userinfo))[0] : 'guest',
        pass  => $uri->userinfo ? (split(/:/, $uri->userinfo))[1] : 'guest',
        vhost => $vhost,
    );

    my $channel = await $consumer_amqp->open_channel;

    # Set QoS (prefetch)
    my $prefetch = $conf->worker_prefetch_multiplier * $concurrency;
    $prefetch = 65535 if $prefetch > 65535;
    await $channel->qos(prefetch_count => $prefetch);

    # Start inspection HTTP server if enabled
    if ($conf->inspection_http_server_enabled) {
        require AIO::Celery::InspectionHTTPServer;
        AIO::Celery::InspectionHTTPServer::start(
            loop => $loop,
            host => $conf->inspection_http_server_host,
            port => $conf->inspection_http_server_port,
        );
        $log->info("Inspection HTTP server started on "
            . $conf->inspection_http_server_host . ":" . $conf->inspection_http_server_port);
    }

    # Consume from each queue
    for my $queue_name (@$queues) {
        # Declare queue
        my %queue_args;
        if (defined $conf->task_queue_max_priority) {
            $queue_args{'x-max-priority'} = $conf->task_queue_max_priority;
        }
        await $channel->queue_declare(
            queue     => $queue_name,
            durable   => 1,
            arguments => \%queue_args,
        );

        my $consumer_tag = generate_consumer_tag(
            prefix         => $conf->worker_consumer_tag_prefix,
            channel_number => 1,
        );

        await $channel->consume(
            queue        => $queue_name,
            consumer_tag => $consumer_tag,
            no_ack       => 0,
            on_message   => sub {
                my ($msg) = @_;
                # Parse raw AMQP message into our format
                my $parsed = _parse_amqp_message($msg);

                # Fire-and-forget: handle message asynchronously
                my $f = on_message_received(
                    message   => $parsed,
                    app       => $app,
                    semaphore => $semaphore,
                    loop      => $loop,
                )->on_fail(sub {
                    my ($err) = @_;
                    $log->error("Unhandled error in message handler: $err");
                })->retain;

                # ACK the message after processing
                $f->on_done(sub {
                    eval { $msg->ack };
                });
                $f->on_fail(sub {
                    eval { $msg->nack };
                });
            },
        );

        $log->info("Consuming from queue: $queue_name (tag: $consumer_tag)");
    }

    $log->info("Worker ready. Waiting for tasks...");

    # Setup graceful shutdown
    my $shutdown = Future->new;
    my $shutting_down = 0;

    for my $sig (qw(INT TERM)) {
        $loop->attach_signal($sig => sub {
            if ($shutting_down) {
                $log->warn("Forced shutdown");
                exit(1);
            }
            $shutting_down = 1;
            $log->info("Graceful shutdown initiated (SIG$sig)");
            # Wait for in-flight tasks then resolve
            if (keys %RUNNING_TASKS) {
                $log->info("Waiting for " . scalar(keys %RUNNING_TASKS) . " tasks to complete...");
                # Poll until all tasks done
                my $check;
                $check = $loop->watch_time(
                    after    => 1,
                    interval => 1,
                    code     => sub {
                        unless (keys %RUNNING_TASKS) {
                            $loop->unwatch_time($check) if $check;
                            $shutdown->done unless $shutdown->is_ready;
                        }
                    },
                );
            } else {
                $shutdown->done unless $shutdown->is_ready;
            }
        });
    }

    await $shutdown;

    # Cleanup
    await $app->teardown;
    $log->info("Worker shutdown complete");
}

sub _parse_amqp_message {
    my ($raw_msg) = @_;

    # Net::Async::AMQP message format adaptation
    # This handles the translation between the AMQP library's message format
    # and our internal message hashref format
    my $headers = {};
    my $body    = '';

    if (blessed($raw_msg)) {
        # Net::Async::AMQP message object
        $body    = $raw_msg->payload // $raw_msg->body // '';
        $headers = $raw_msg->headers // {};

        # Also extract from properties if headers are in properties
        if ($raw_msg->can('properties')) {
            my $props = $raw_msg->properties // {};
            $headers = { %$headers, %{$props->{headers} // {}} };
        }
    } elsif (ref $raw_msg eq 'HASH') {
        $body    = $raw_msg->{body}    // '';
        $headers = $raw_msg->{headers} // {};
    }

    return {
        body    => $body,
        headers => $headers,
    };
}

1;
