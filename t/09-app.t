use strict;
use warnings;
use Test2::V0;
use AIO::Celery::App;

# App creation
my $app = AIO::Celery::App->new(name => 'testapp');
is($app->name, 'testapp', 'app name');
ok($app->conf->isa('AIO::Celery::Config'), 'app has config');
is($app->conf->task_default_queue, 'celery', 'default queue via conf');

# Config update
$app->conf->update(task_default_queue => 'myqueue');
is($app->conf->task_default_queue, 'myqueue', 'updated queue');

# Task registration
my $add = $app->task(sub { $_[0] + $_[1] }, name => 'tasks.add');
ok($add->isa('AIO::Celery::AnnotatedTask'), 'task returns AnnotatedTask');
is($add->name, 'tasks.add', 'task name');

my $mul = $app->task(
    sub { $_[0] * $_[1] },
    name        => 'tasks.mul',
    max_retries => 3,
    queue       => 'fast',
);
is($mul->max_retries, 3,      'task max_retries');
is($mul->queue,       'fast', 'task queue');

# Task registry
my @names = $app->list_registered_task_names;
is(\@names, ['tasks.add', 'tasks.mul'], 'registered task names sorted');

# get_annotated_task
ok($app->get_annotated_task('tasks.add') == $add, 'get_annotated_task');
like(
    dies { $app->get_annotated_task('tasks.nonexistent') },
    qr/not registered/,
    'get_annotated_task dies for unknown',
);

# Task registration requires name
like(
    dies { $app->task(sub { 1 }) },
    qr/name is required/,
    'task requires name',
);

# Task registration requires coderef
like(
    dies { $app->task('not a code', name => 'x') },
    qr/coderef/,
    'task requires coderef',
);

# Broker not connected
like(
    dies { $app->broker },
    qr/not connected/,
    'broker dies before setup',
);

# Result backend is undef before setup
is($app->result_backend, undef, 'result_backend undef before setup');

done_testing;
