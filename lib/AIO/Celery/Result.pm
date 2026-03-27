package AIO::Celery::Result;
use Moo;
use Types::Standard qw(Str Maybe Num);
use Future::AsyncAwait;
use JSON::MaybeXS qw(decode_json);
use Log::Any qw($log);
use AIO::Celery::Exceptions;
use namespace::clean;

has id     => (is => 'ro', isa => Str, required => 1);
has _redis => (is => 'ro',            default  => undef);
has _cache => (is => 'rw',            default  => undef);

sub _redis_key {
    my ($self) = @_;
    return 'celery-task-meta-' . $self->id;
}

async sub _get_task_meta {
    my ($self) = @_;

    return $self->_cache if defined $self->_cache;

    unless ($self->_redis) {
        return { result => undef, status => 'PENDING' };
    }

    my $raw = await $self->_redis->get($self->_redis_key);

    unless (defined $raw) {
        return { result => undef, status => 'PENDING' };
    }

    my $meta = decode_json($raw);
    $self->_cache($meta);
    return $meta;
}

async sub result {
    my ($self) = @_;
    my $meta = await $self->_get_task_meta;
    return $meta->{result};
}

async sub state {
    my ($self) = @_;
    my $meta = await $self->_get_task_meta;
    return $meta->{status};
}

async sub get {
    my ($self, %args) = @_;
    my $timeout  = $args{timeout};
    my $interval = $args{interval} // 0.5;

    my $start = time();

    while (1) {
        # Clear cache to force a fresh read
        $self->_cache(undef);

        my $meta = await $self->_get_task_meta;
        my $status = $meta->{status} // 'PENDING';

        if ($status eq 'SUCCESS' || $status eq 'FAILURE') {
            if ($status eq 'FAILURE') {
                die AIO::Celery::Exception::CeleryError->new(
                    message => "Task ${\$self->id} failed: " . ($meta->{result} // 'unknown error'),
                );
            }
            return $meta->{result};
        }

        if (defined $timeout && (time() - $start) >= $timeout) {
            die AIO::Celery::Exception::Timeout->new(
                message => "Timeout waiting for task " . $self->id,
            );
        }

        # Async sleep
        my $loop = IO::Async::Loop->new;
        await $loop->delay_future(after => $interval);
    }
}

1;
