package AIO::Celery::Context;
use strict;
use warnings;
use Exporter 'import';

our @EXPORT_OK = qw($CURRENT_TASK_ID $CURRENT_ROOT_ID);

our $CURRENT_TASK_ID;
our $CURRENT_ROOT_ID;

1;
