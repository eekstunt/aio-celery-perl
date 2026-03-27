use strict;
use warnings;
use Test2::V0;
use AIO::Celery::Canvas qw(chain signature);

# Signature construction
my $sig = AIO::Celery::Canvas::Signature->new(
    task => 'tasks.add',
    args => [1, 2],
);
is($sig->task, 'tasks.add', 'sig task');
is($sig->args, [1, 2],      'sig args');
is($sig->kwargs, {},         'sig kwargs default');
is($sig->options, {},        'sig options default');

# from_dict
my $sig2 = AIO::Celery::Canvas::Signature->from_dict({
    task    => 'tasks.mul',
    args    => [3, 4],
    kwargs  => { round => 1 },
    options => { queue => 'fast' },
});
is($sig2->task, 'tasks.mul',         'from_dict task');
is($sig2->args, [3, 4],              'from_dict args');
is($sig2->kwargs, { round => 1 },    'from_dict kwargs');
is($sig2->options, { queue => 'fast' }, 'from_dict options');

# set()
$sig2->set(priority => 5, countdown => 10);
is($sig2->options->{priority},  5,  'set priority');
is($sig2->options->{countdown}, 10, 'set countdown');
is($sig2->options->{queue}, 'fast', 'set preserves existing');

# TO_JSON
my $json = $sig->TO_JSON;
is($json->{task}, 'tasks.add', 'TO_JSON task');
is($json->{args}, [1, 2],      'TO_JSON args');

# signature() factory — string
my $sig3 = signature('tasks.test');
is($sig3->task, 'tasks.test', 'signature from string');

# signature() factory — hashref
my $sig4 = signature({ task => 'tasks.x', args => [9] });
is($sig4->task, 'tasks.x',  'signature from hashref');
is($sig4->args, [9],        'signature from hashref args');

# signature() factory — Signature passthrough
my $sig5 = signature($sig);
is($sig5, $sig, 'signature passthrough');

# chain()
my $c = chain(
    AIO::Celery::Canvas::Signature->new(task => 'tasks.a'),
    AIO::Celery::Canvas::Signature->new(task => 'tasks.b'),
    AIO::Celery::Canvas::Signature->new(task => 'tasks.c'),
);
is($c->task, 'celery.chain', 'chain task name');
ref_ok($c->options->{chain}, 'ARRAY', 'chain has chain in options');
# Chain is reversed: [c, b, a]
is(scalar @{$c->options->{chain}}, 3, 'chain has 3 items');
is($c->options->{chain}[0]{task}, 'tasks.c', 'chain reversed: first is last');
is($c->options->{chain}[2]{task}, 'tasks.a', 'chain reversed: last is first');

done_testing;
