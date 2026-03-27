use strict;
use warnings;
use Test2::V0;
use AIO::Celery::Context qw($CURRENT_TASK_ID $CURRENT_ROOT_ID);

# Default values
is($CURRENT_TASK_ID, undef, 'CURRENT_TASK_ID default undef');
is($CURRENT_ROOT_ID, undef, 'CURRENT_ROOT_ID default undef');

# Set and read
$CURRENT_TASK_ID = 'task-123';
$CURRENT_ROOT_ID = 'root-456';
is($CURRENT_TASK_ID, 'task-123', 'set task id');
is($CURRENT_ROOT_ID, 'root-456', 'set root id');

# local scoping
{
    local $CURRENT_TASK_ID = 'inner-task';
    local $CURRENT_ROOT_ID = 'inner-root';
    is($CURRENT_TASK_ID, 'inner-task', 'local task id');
    is($CURRENT_ROOT_ID, 'inner-root', 'local root id');
}
is($CURRENT_TASK_ID, 'task-123', 'restored task id after scope');
is($CURRENT_ROOT_ID, 'root-456', 'restored root id after scope');

# Cleanup
$CURRENT_TASK_ID = undef;
$CURRENT_ROOT_ID = undef;

done_testing;
