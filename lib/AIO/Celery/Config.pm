package AIO::Celery::Config;
use Moo;
use Types::Standard qw(Str Int Num Bool Maybe);
use Carp qw(croak);
use namespace::clean;

has task_ignore_result              => (is => 'rw', isa => Bool,       default => 0);
has task_soft_time_limit            => (is => 'rw', isa => Maybe[Num], default => undef);
has task_time_limit                 => (is => 'rw', isa => Maybe[Num], default => undef);
has task_acks_late                  => (is => 'rw', isa => Bool,       default => 0);
has result_backend                  => (is => 'rw', isa => Maybe[Str], default => undef);
has result_backend_connection_pool_size => (is => 'rw', isa => Int,    default => 50);
has result_expires                  => (is => 'rw', isa => Int,        default => 86400);
has task_default_priority           => (is => 'rw', isa => Maybe[Int], default => undef);
has task_default_queue              => (is => 'rw', isa => Str,        default => 'celery');
has task_queue_max_priority         => (is => 'rw', isa => Maybe[Int], default => undef);
has broker_publish_timeout          => (is => 'rw', isa => Maybe[Num], default => undef);
has broker_url                      => (is => 'rw', isa => Str,        default => 'amqp://guest:guest@localhost:5672//');
has worker_consumer_tag_prefix      => (is => 'rw', isa => Str,        default => '');
has worker_prefetch_multiplier      => (is => 'rw', isa => Int,        default => 4);
has inspection_http_server_enabled  => (is => 'rw', isa => Bool,       default => 0);
has inspection_http_server_host     => (is => 'rw', isa => Str,        default => 'localhost');
has inspection_http_server_port     => (is => 'rw', isa => Int,        default => 1430);
has intermittent_gc_enabled         => (is => 'rw', isa => Bool,       default => 0);
has intermittent_gc_max_tasks       => (is => 'rw', isa => Int,        default => 100);
has intermittent_gc_max_interval    => (is => 'rw', isa => Int,        default => 3600);

my %_known_keys = map { $_ => 1 } qw(
    task_ignore_result task_soft_time_limit task_time_limit task_acks_late
    result_backend result_backend_connection_pool_size result_expires
    task_default_priority task_default_queue task_queue_max_priority
    broker_publish_timeout broker_url
    worker_consumer_tag_prefix worker_prefetch_multiplier
    inspection_http_server_enabled inspection_http_server_host inspection_http_server_port
    intermittent_gc_enabled intermittent_gc_max_tasks intermittent_gc_max_interval
);

sub update {
    my ($self, %opts) = @_;
    for my $key (keys %opts) {
        croak "Unknown configuration key: $key" unless $_known_keys{$key};
        $self->$key($opts{$key});
    }
    return $self;
}

1;
