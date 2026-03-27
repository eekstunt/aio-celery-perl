package AIO::Celery::Canvas::Signature;
use Moo;
use Types::Standard qw(Str ArrayRef HashRef Maybe);
use Storable qw(dclone);
use AIO::Celery::Utils qw(first_not_null);
use Future::AsyncAwait;
use namespace::clean;

has task    => (is => 'ro', isa => Str,      required => 1);
has args    => (is => 'ro', isa => ArrayRef, default  => sub { [] });
has kwargs  => (is => 'ro', isa => HashRef,  default  => sub { {} });
has options => (is => 'rw', isa => HashRef,  default  => sub { {} });
has _app    => (is => 'ro',                  default  => undef, init_arg => 'app');

sub from_dict {
    my ($class, $dict, %extra) = @_;
    return $class->new(
        task    => $dict->{task},
        args    => $dict->{args}    // [],
        kwargs  => $dict->{kwargs}  // {},
        options => $dict->{options} // {},
        (exists $extra{app} ? (app => $extra{app}) : ()),
    );
}

sub set {
    my ($self, %opts) = @_;
    my $current = $self->options;
    $self->options({ %$current, %opts });
    return $self;
}

async sub apply_async {
    my ($self, %overrides) = @_;

    require AIO::Celery::State;
    my $app = $self->_app // AIO::Celery::State::get_current_app();

    my $task_name = $self->task;
    my $args      = first_not_null($overrides{args},    $self->args);
    my $kwargs    = first_not_null($overrides{kwargs},   $self->kwargs);
    my $queue     = first_not_null($overrides{queue},    $self->options->{queue});
    my $priority  = first_not_null($overrides{priority}, $self->options->{priority});
    my $countdown = first_not_null($overrides{countdown}, $self->options->{countdown});
    my $task_id   = first_not_null($overrides{task_id},  $self->options->{task_id});
    my $chain     = first_not_null($overrides{chain},    $self->options->{chain});
    my $ignore_result = first_not_null($overrides{ignore_result}, $self->options->{ignore_result});

    return await $app->send_task(
        $task_name,
        args          => $args,
        kwargs        => $kwargs,
        queue         => $queue,
        priority      => $priority,
        countdown     => $countdown,
        task_id       => $task_id,
        chain         => $chain,
        ignore_result => $ignore_result,
    );
}

async sub delay {
    my ($self, @call_args) = @_;
    # Separate positional args from keyword args is tricky in Perl;
    # for simplicity, all args are positional when using delay()
    return await $self->apply_async(args => \@call_args);
}

sub TO_JSON {
    my ($self) = @_;
    return {
        task    => $self->task,
        args    => $self->args,
        kwargs  => $self->kwargs,
        options => $self->options,
    };
}

package AIO::Celery::Canvas;
use strict;
use warnings;
use Exporter 'import';

our @EXPORT_OK = qw(chain signature);

sub chain {
    my @tasks = @_;
    # Celery chains are stored in reverse order
    my @reversed = reverse @tasks;
    my @chain_sigs;
    for my $t (@reversed) {
        if (ref $t eq 'AIO::Celery::Canvas::Signature') {
            push @chain_sigs, $t->TO_JSON;
        } elsif (ref $t eq 'HASH') {
            push @chain_sigs, $t;
        } else {
            die "chain() expects Signature objects or hashrefs, got: " . ref($t);
        }
    }
    return AIO::Celery::Canvas::Signature->new(
        task    => 'celery.chain',
        args    => [],
        kwargs  => {},
        options => { chain => \@chain_sigs },
    );
}

sub signature {
    my ($spec, %opts) = @_;
    if (ref $spec eq 'AIO::Celery::Canvas::Signature') {
        return $spec;
    }
    if (ref $spec eq 'HASH') {
        return AIO::Celery::Canvas::Signature->from_dict($spec, %opts);
    }
    if (!ref $spec) {
        # String: treat as task name
        return AIO::Celery::Canvas::Signature->new(
            task => $spec,
            %opts,
        );
    }
    die "signature() expects a string, hashref, or Signature; got: " . ref($spec);
}

1;
