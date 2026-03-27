package AIO::Celery;
use strict;
use warnings;

our $VERSION = '0.01';

use AIO::Celery::App;
use AIO::Celery::Task;
use AIO::Celery::Canvas qw(chain signature);
use AIO::Celery::State qw(get_current_app set_current_app);
use AIO::Celery::Exceptions;
use AIO::Celery::Result;
use AIO::Celery::AnnotatedTask;

use Exporter 'import';
our @EXPORT_OK = qw(chain signature get_current_app);

1;

__END__

=head1 NAME

AIO::Celery - Async Celery task queue implementation for Perl

=head1 SYNOPSIS

    use AIO::Celery::App;

    my $app = AIO::Celery::App->new(name => 'myapp');
    $app->conf->update(
        broker_url     => 'amqp://guest:guest@localhost:5672//',
        result_backend => 'redis://localhost:6379/0',
    );

    # Register a task
    my $add = $app->task(
        sub { return $_[0] + $_[1] },
        name => 'tasks.add',
    );

    # Send a task (requires event loop and setup)
    # await $app->setup(loop => $loop);
    # my $result = await $add->delay(2, 3);
    # my $value = await $result->get(timeout => 10);

=head1 DESCRIPTION

AIO::Celery is a Perl port of the Python aio-celery library. It implements
the Celery Message Protocol for interoperability with Python Celery workers
and clients.

Features:

=over 4

=item * RabbitMQ message broker via Net::Async::AMQP

=item * Redis result backend via Net::Async::Redis

=item * Celery Message Protocol v2 compatibility

=item * Async worker with configurable concurrency

=item * Task chaining via Canvas signatures

=item * Task retry with configurable backoff

=item * HTTP inspection server for monitoring

=back

=head1 SEE ALSO

L<AIO::Celery::App>, L<AIO::Celery::Worker>

=cut
