package AIO::Celery::Exception::CeleryError;
use Moo;
with 'Throwable';
use namespace::clean;

has message => (is => 'ro', default => '');

use overload '""' => sub { $_[0]->message }, fallback => 1;

package AIO::Celery::Exception::MaxRetriesExceeded;
use Moo;
extends 'AIO::Celery::Exception::CeleryError';
use namespace::clean;

use overload '""' => sub { $_[0]->message }, fallback => 1;

package AIO::Celery::Exception::Timeout;
use Moo;
extends 'AIO::Celery::Exception::CeleryError';
use namespace::clean;

use overload '""' => sub { $_[0]->message }, fallback => 1;

package AIO::Celery::Exception::Retry;
use Moo;
extends 'AIO::Celery::Exception::CeleryError';
use namespace::clean;

has delay        => (is => 'ro', default => 0);
has amqp_message => (is => 'ro');
has routing_key  => (is => 'ro', default => '');

use overload '""' => sub { sprintf('Retry in %ss', $_[0]->delay) }, fallback => 1;

package AIO::Celery::Exceptions;
use strict;
use warnings;

1;
