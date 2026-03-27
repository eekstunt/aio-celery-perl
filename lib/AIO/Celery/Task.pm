package AIO::Celery::Task;
use Moo;
use Types::Standard qw(Str Num Maybe InstanceOf);
use Future::AsyncAwait;
use JSON::MaybeXS qw(encode_json);
use DateTime;
use Storable qw(dclone);
use Log::Any qw($log);
use AIO::Celery::AMQP qw(create_task_message);
use AIO::Celery::Exceptions;
use namespace::clean;

has app                 => (is => 'ro', required => 1, weak_ref => 1);
has request             => (is => 'ro', required => 1);
has _default_retry_delay => (is => 'ro', isa => Num, default => 180, init_arg => 'default_retry_delay');

sub name {
    my ($self) = @_;
    return $self->request->task;
}

async sub update_state {
    my ($self, %args) = @_;
    my $state     = $args{state}     // die "state is required";
    my $meta      = $args{meta}      // {};
    my $finalize  = $args{_finalize} // 0;
    my $traceback = $args{_traceback};

    my $redis = $self->app->result_backend;
    unless ($redis) {
        $log->debug("No result backend configured, skipping state update");
        return;
    }

    my $date_done;
    if ($finalize) {
        $date_done = DateTime->now(time_zone => 'UTC')->strftime('%Y-%m-%dT%H:%M:%S.000000');
    }

    my $payload = {
        status    => $state,
        result    => $meta,
        traceback => $traceback,
        children  => [],
        date_done => $date_done,
        task_id   => $self->request->id,
    };

    if (defined $self->request->group) {
        $payload->{group_id} = $self->request->group;
    }
    if (defined $self->request->parent_id) {
        $payload->{parent_id} = $self->request->parent_id;
    }

    my $key = 'celery-task-meta-' . $self->request->id;
    my $json = encode_json($payload);
    my $ttl = $self->app->conf->result_expires;

    await $redis->set($key, $json);
    if ($ttl && $ttl > 0) {
        await $redis->expire($key, $ttl);
    }

    $log->debug("Updated state for task ${\$self->request->id}: $state");
}

sub build_next_task_message {
    my ($self, $result) = @_;

    my $chain = $self->request->chain;
    return (undef, '') unless $chain && @$chain;

    # Deep copy chain to avoid mutating the original
    my @chain_copy = map { dclone($_) } @$chain;

    # Celery chains are stored reversed — pop the last element (next task)
    my $next_task_dict = pop @chain_copy;

    my $task_name = $next_task_dict->{task};
    my $args      = $next_task_dict->{args}    // [];
    my $kwargs    = $next_task_dict->{kwargs}   // {};
    my $options   = $next_task_dict->{options}  // {};

    # Prepend the result to args (chain passes result forward)
    unshift @$args, $result;

    my $routing_key = $options->{queue} // $self->app->conf->task_default_queue;

    my $message = create_task_message(
        task_name     => $task_name,
        args          => $args,
        kwargs        => $kwargs,
        priority      => $options->{priority},
        parent_id     => $self->request->id,
        root_id       => $self->request->root_id,
        chain         => \@chain_copy,
        ignore_result => $options->{ignore_result} // 0,
    );

    return ($message, $routing_key);
}

sub retry {
    my ($self, %args) = @_;
    my $countdown = $args{countdown} // $self->_default_retry_delay;

    my $retry_message = $self->request->build_retry_message(countdown => $countdown);
    my $routing_key = $args{queue}
        // $self->request->message->{routing_key}
        // $self->app->conf->task_default_queue;

    die AIO::Celery::Exception::Retry->new(
        message      => "Retry in ${countdown}s",
        delay        => $countdown,
        amqp_message => $retry_message,
        routing_key  => $routing_key,
    );
}

1;
