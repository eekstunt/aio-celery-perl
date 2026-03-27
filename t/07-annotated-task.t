use strict;
use warnings;
use Test2::V0;
use AIO::Celery::AnnotatedTask;

# Basic task
my $add_fn = sub { return $_[0] + $_[1] };
my $task = AIO::Celery::AnnotatedTask->new(
    fn   => $add_fn,
    name => 'tasks.add',
);

is($task->name, 'tasks.add', 'task name');
is($task->bind, 0,            'bind default false');
is($task->default_retry_delay, 180, 'default_retry_delay');

# call() without bind
is($task->call(2, 3), 5, 'call without bind');

# call() with bind
my $bound_fn = sub {
    my ($self, $x) = @_;
    return ref($self) . ":$x";
};
my $bound_task = AIO::Celery::AnnotatedTask->new(
    fn   => $bound_fn,
    name => 'tasks.bound',
    bind => 1,
);
like($bound_task->call(42), qr/AnnotatedTask:42/, 'call with bind injects self');

# signature() generation
my $sig = $task->signature(args => [1, 2], queue => 'fast');
is($sig->task, 'tasks.add',            'signature task');
is($sig->args, [1, 2],                 'signature args');
is($sig->options->{queue}, 'fast',     'signature queue');

# s() shortcut
my $sig2 = $task->s(10, 20);
is($sig2->task, 'tasks.add',  'shortcut task');
is($sig2->args, [10, 20],     'shortcut args');

# Task with options
my $task2 = AIO::Celery::AnnotatedTask->new(
    fn            => sub { 1 },
    name          => 'tasks.opts',
    max_retries   => 5,
    queue         => 'priority',
    priority      => 3,
    ignore_result => 1,
    soft_time_limit => 30,
);
is($task2->max_retries,     5,          'max_retries');
is($task2->queue,           'priority', 'queue');
is($task2->priority,        3,          'priority');
is($task2->ignore_result,   1,          'ignore_result');
is($task2->soft_time_limit, 30,         'soft_time_limit');

done_testing;
