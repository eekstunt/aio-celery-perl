package AIO::Celery::AnnotatedTask;
use Moo;
use Types::Standard qw(Str CodeRef Bool Int Num Maybe ArrayRef);
use Future::AsyncAwait;
use AIO::Celery::Utils qw(first_not_null);
use namespace::clean;

has fn                  => (is => 'ro', isa => CodeRef,      required => 1);
has name                => (is => 'ro', isa => Str,          required => 1);
has bind                => (is => 'ro', isa => Bool,         default  => 0);
has ignore_result       => (is => 'ro', isa => Maybe[Bool],  default  => undef);
has max_retries         => (is => 'ro', isa => Maybe[Int],   default  => undef);
has default_retry_delay => (is => 'ro', isa => Num,          default  => 180);
has autoretry_for       => (is => 'ro', isa => ArrayRef[Str], default => sub { [] });
has app                 => (is => 'ro',                      weak_ref => 1);
has queue               => (is => 'ro', isa => Maybe[Str],   default  => undef);
has priority            => (is => 'ro', isa => Maybe[Int],   default  => undef);
has soft_time_limit     => (is => 'ro', isa => Maybe[Num],   default  => undef);

sub call {
    my ($self, @args) = @_;
    if ($self->bind) {
        return $self->fn->($self, @args);
    }
    return $self->fn->(@args);
}

sub signature {
    my ($self, %opts) = @_;
    require AIO::Celery::Canvas;
    return AIO::Celery::Canvas::Signature->new(
        task    => $self->name,
        args    => $opts{args}    // [],
        kwargs  => $opts{kwargs}  // {},
        options => {
            queue         => first_not_null($opts{queue},         $self->queue),
            priority      => first_not_null($opts{priority},      $self->priority),
            ignore_result => first_not_null($opts{ignore_result}, $self->ignore_result),
        },
        app => $self->app,
    );
}

sub s {
    my ($self, @args) = @_;
    # s(arg1, arg2, key => val) — positional become args, pairs become kwargs
    # For simplicity: all positional args
    return $self->signature(args => \@args);
}

async sub apply_async {
    my ($self, %opts) = @_;
    my $sig = $self->signature(%opts);
    return await $sig->apply_async;
}

async sub delay {
    my ($self, @args) = @_;
    return await $self->apply_async(args => \@args);
}

1;
