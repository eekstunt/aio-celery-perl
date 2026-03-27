use strict;
use warnings;
use Test2::V0;
use AIO::Celery::Exceptions;

# CeleryError
my $err = AIO::Celery::Exception::CeleryError->new(message => 'something failed');
is("$err", 'something failed', 'CeleryError stringifies');
ok($err->isa('AIO::Celery::Exception::CeleryError'), 'CeleryError isa');
ok($err->does('Throwable'), 'CeleryError does Throwable');

# MaxRetriesExceeded
my $max_err = AIO::Celery::Exception::MaxRetriesExceeded->new(message => 'too many');
ok($max_err->isa('AIO::Celery::Exception::CeleryError'),       'MaxRetries inherits CeleryError');
ok($max_err->isa('AIO::Celery::Exception::MaxRetriesExceeded'), 'MaxRetries isa');
is("$max_err", 'too many', 'MaxRetries stringifies');

# Timeout
my $timeout = AIO::Celery::Exception::Timeout->new(message => 'timed out');
ok($timeout->isa('AIO::Celery::Exception::CeleryError'), 'Timeout inherits CeleryError');
is("$timeout", 'timed out', 'Timeout stringifies');

# Retry
my $retry = AIO::Celery::Exception::Retry->new(
    delay        => 30,
    amqp_message => { body => 'test' },
    routing_key  => 'celery',
);
ok($retry->isa('AIO::Celery::Exception::CeleryError'), 'Retry inherits CeleryError');
is("$retry", 'Retry in 30s', 'Retry stringifies with delay');
is($retry->delay, 30, 'Retry delay');
is($retry->routing_key, 'celery', 'Retry routing_key');
ref_ok($retry->amqp_message, 'HASH', 'Retry amqp_message is hashref');

# Throwable: can be thrown and caught
eval { $err->throw };
ok($@, 'CeleryError can be thrown');
ok(ref $@ && $@->isa('AIO::Celery::Exception::CeleryError'), 'thrown error is CeleryError');

done_testing;
