package AIO::Celery::Broker;
use Moo;
use Types::Standard qw(Str Int Num Maybe HashRef InstanceOf);
use Future::AsyncAwait;
use JSON::MaybeXS qw(encode_json);
use Log::Any qw($log);
use Carp qw(croak);
use namespace::clean;

has broker_url              => (is => 'ro', isa => Str,        required => 1);
has task_queue_max_priority => (is => 'ro', isa => Maybe[Int], default  => undef);
has broker_publish_timeout  => (is => 'ro', isa => Maybe[Num], default  => undef);
has _channel                => (is => 'rw');
has _connection             => (is => 'rw');
has _declared_queues        => (is => 'ro', default => sub { {} });

async sub connect {
    my ($self, %args) = @_;
    my $loop = $args{loop} // croak "loop is required";

    require Net::Async::AMQP;
    my $amqp = Net::Async::AMQP->new;
    $loop->add($amqp);

    my $uri = URI->new($self->broker_url);
    my $vhost = $uri->path;
    $vhost =~ s{^/}{};
    $vhost = '/' if $vhost eq '' || $vhost eq '/';

    await $amqp->connect(
        host  => $uri->host  // 'localhost',
        port  => $uri->port  // 5672,
        user  => $uri->userinfo ? (split(/:/, $uri->userinfo))[0] : 'guest',
        pass  => $uri->userinfo ? (split(/:/, $uri->userinfo))[1] : 'guest',
        vhost => $vhost,
    );

    my $channel = await $amqp->open_channel;
    $self->_connection($amqp);
    $self->_channel($channel);

    $log->info("Broker connected to " . $self->broker_url);
    return $self;
}

async sub declare_queue {
    my ($self, %args) = @_;
    my $queue_name = $args{queue_name} // croak "queue_name is required";
    my $channel    = $args{channel}    // $self->_channel;

    return if $self->_declared_queues->{$queue_name};

    my %queue_args;
    if (defined $self->task_queue_max_priority) {
        $queue_args{'x-max-priority'} = $self->task_queue_max_priority;
    }

    await $channel->queue_declare(
        queue     => $queue_name,
        durable   => 1,
        arguments => \%queue_args,
    );

    $self->_declared_queues->{$queue_name} = 1;
    $log->debug("Declared queue: $queue_name");
}

async sub publish_message {
    my ($self, $message, %args) = @_;
    my $routing_key = $args{routing_key} // croak "routing_key is required";
    my $channel     = $self->_channel // croak "Broker not connected";

    await $self->declare_queue(queue_name => $routing_key);

    my $properties = {
        content_type     => $message->{content_type}     // 'application/json',
        content_encoding => $message->{content_encoding} // 'utf-8',
        delivery_mode    => $message->{delivery_mode}    // 2,
        correlation_id   => $message->{correlation_id},
        reply_to         => $message->{reply_to},
        headers          => $message->{headers},
    };
    $properties->{priority}   = $message->{priority}   if defined $message->{priority};
    $properties->{expiration} = $message->{expiration}  if defined $message->{expiration};

    my $body = $message->{body};
    $body = encode_json($body) if ref $body;

    await $channel->publish(
        exchange    => '',
        routing_key => $routing_key,
        body        => $body,
        headers     => $message->{headers},
        %$properties,
    );

    $log->debug("Published message to $routing_key");
}

async sub disconnect {
    my ($self) = @_;
    if ($self->_connection) {
        eval { await $self->_connection->close };
        $self->_connection(undef);
        $self->_channel(undef);
    }
}

1;
