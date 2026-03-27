package AIO::Celery::App;
use Moo;
use Types::Standard qw(Str Maybe HashRef CodeRef InstanceOf);
use Future::AsyncAwait;
use Data::UUID;
use Log::Any qw($log);
use Carp qw(croak);
use AIO::Celery::Config;
use AIO::Celery::AMQP qw(create_task_message);
use AIO::Celery::AnnotatedTask;
use AIO::Celery::Result;
use AIO::Celery::State qw(set_current_app);
use namespace::clean;

my $_uuid_gen = Data::UUID->new;

has name             => (is => 'ro', isa => Maybe[Str], default => undef);
has conf             => (is => 'ro', default => sub { AIO::Celery::Config->new });
has _tasks_registry  => (is => 'ro', default => sub { {} });
has _broker          => (is => 'rw', default => undef);
has _redis           => (is => 'rw', default => undef);
has _loop            => (is => 'rw', default => undef);

sub broker {
    my ($self) = @_;
    croak "Broker not connected. Call setup() first." unless $self->_broker;
    return $self->_broker;
}

sub result_backend {
    my ($self) = @_;
    return $self->_redis;
}

async sub setup {
    my ($self, %args) = @_;
    my $loop = $args{loop} // do {
        require IO::Async::Loop;
        IO::Async::Loop->new;
    };
    $self->_loop($loop);

    # Connect broker
    require AIO::Celery::Broker;
    my $broker = AIO::Celery::Broker->new(
        broker_url              => $self->conf->broker_url,
        task_queue_max_priority => $self->conf->task_queue_max_priority,
        broker_publish_timeout  => $self->conf->broker_publish_timeout,
    );
    await $broker->connect(loop => $loop);
    $self->_broker($broker);

    # Connect Redis result backend if configured
    if (defined $self->conf->result_backend) {
        require AIO::Celery::Backend;
        my $redis = AIO::Celery::Backend::create_redis_connection(
            loop => $loop,
            url  => $self->conf->result_backend,
        );
        $self->_redis($redis);
    }

    set_current_app($self);

    $log->info("AIO::Celery app setup complete");
    return $self;
}

async sub teardown {
    my ($self) = @_;
    if ($self->_broker) {
        await $self->_broker->disconnect;
        $self->_broker(undef);
    }
    if ($self->_redis) {
        # Net::Async::Redis doesn't have an explicit disconnect
        $self->_redis(undef);
    }
    $log->info("AIO::Celery app teardown complete");
}

sub task {
    my ($self, $fn, %opts) = @_;
    croak "First argument must be a coderef" unless ref $fn eq 'CODE';
    croak "name is required for task registration" unless $opts{name};

    my $annotated = AIO::Celery::AnnotatedTask->new(
        fn                  => $fn,
        name                => $opts{name},
        bind                => $opts{bind}                // 0,
        ignore_result       => $opts{ignore_result},
        max_retries         => $opts{max_retries},
        default_retry_delay => $opts{default_retry_delay} // 180,
        autoretry_for       => $opts{autoretry_for}       // [],
        queue               => $opts{queue},
        priority            => $opts{priority},
        soft_time_limit     => $opts{soft_time_limit},
        app                 => $self,
    );

    $self->_tasks_registry->{$opts{name}} = $annotated;
    $log->debug("Registered task: $opts{name}");

    return $annotated;
}

sub get_annotated_task {
    my ($self, $task_name) = @_;
    my $t = $self->_tasks_registry->{$task_name};
    croak "Task '$task_name' is not registered" unless $t;
    return $t;
}

sub list_registered_task_names {
    my ($self) = @_;
    return sort keys %{$self->_tasks_registry};
}

sub async_result {
    my ($self, $task_id) = @_;
    return AIO::Celery::Result->new(
        id     => $task_id,
        _redis => $self->_redis,
    );
}

sub _generate_uuid {
    my $hex = lc($_uuid_gen->create_hex);
    $hex =~ s/^0x//;
    return join('-',
        substr($hex, 0, 8),
        substr($hex, 8, 4),
        substr($hex, 12, 4),
        substr($hex, 16, 4),
        substr($hex, 20, 12),
    );
}

async sub send_task {
    my ($self, $task_name, %opts) = @_;
    croak "Broker not connected. Call setup() first." unless $self->_broker;

    my $task_id = $opts{task_id} // _generate_uuid();
    my $queue   = $opts{queue}   // $self->conf->task_default_queue;

    my $message = create_task_message(
        task_id       => $task_id,
        task_name     => $task_name,
        args          => $opts{args}          // [],
        kwargs        => $opts{kwargs}        // {},
        priority      => $opts{priority}      // $self->conf->task_default_priority,
        countdown     => $opts{countdown},
        chain         => $opts{chain}         // [],
        ignore_result => $opts{ignore_result} // $self->conf->task_ignore_result,
        reply_to      => $opts{reply_to}      // '',
    );

    await $self->_broker->publish_message($message, routing_key => $queue);

    $log->info("Sent task $task_name [$task_id] to queue $queue");

    return $self->async_result($task_id);
}

1;
