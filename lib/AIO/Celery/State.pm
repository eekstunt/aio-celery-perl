package AIO::Celery::State::RunningTask;
use Moo;
use Types::Standard qw(Str Int Num Maybe ArrayRef);
use POSIX qw(strftime);
use namespace::clean;

has task_id         => (is => 'ro', isa => Str,        required => 1);
has task_name       => (is => 'ro', isa => Str,        required => 1);
has state           => (is => 'rw', isa => Str,        default  => 'RUNNING');
has started         => (is => 'ro', isa => Num,        default  => sub { time() });
has args            => (is => 'ro', isa => ArrayRef,   default  => sub { [] });
has kwargs          => (is => 'ro',                    default  => sub { {} });
has retries         => (is => 'ro', isa => Int,        default  => 0);
has eta             => (is => 'ro', isa => Maybe[Str], default  => undef);
has soft_time_limit => (is => 'ro', isa => Maybe[Num], default  => undef);

sub elapsed {
    my ($self) = @_;
    return time() - $self->started;
}

sub serialize {
    my ($self) = @_;
    return {
        id              => $self->task_id,
        name            => $self->task_name,
        state           => $self->state,
        started         => strftime('%Y-%m-%dT%H:%M:%S', gmtime(int($self->started))),
        elapsed         => sprintf('%.2f', $self->elapsed),
        args            => $self->args,
        kwargs          => $self->kwargs,
        retries         => $self->retries,
        eta             => $self->eta,
        soft_time_limit => $self->soft_time_limit,
    };
}

package AIO::Celery::State;
use strict;
use warnings;
use Exporter 'import';
use Carp qw(croak);

our @EXPORT_OK = qw(
    get_current_app set_current_app
    $CURRENT_APP %RUNNING_TASKS $TASKS_SINCE_LAST_GC
);

our $CURRENT_APP;
our %RUNNING_TASKS;
our $TASKS_SINCE_LAST_GC = 0;

sub get_current_app {
    croak 'No current AIO::Celery app is set' unless defined $CURRENT_APP;
    return $CURRENT_APP;
}

sub set_current_app {
    my ($app) = @_;
    $CURRENT_APP = $app;
}

1;
