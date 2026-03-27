#!/usr/bin/env perl
#
# Benchmark Perl AIO::Celery: message serialization, deserialization,
# and publish to RabbitMQ.
#
use strict;
use warnings;
use lib 'lib';
use Time::HiRes qw(time);
use JSON::MaybeXS qw(encode_json decode_json);

use Future;
use IO::Async::Loop;
use AIO::Celery::App;
use AIO::Celery::AMQP qw(create_task_message);
use AIO::Celery::Request;

my $NUM_TASKS  = $ARGV[0] // 10_000;
my $NUM_RUNS   = $ARGV[1] // 5;
my $BROKER_URL = $ENV{BROKER_URL} // 'amqp://guest:guest@127.0.0.1:5673//';

print "=" x 60, "\n";
print "Perl AIO::Celery benchmark\n";
print "=" x 60, "\n";
printf "Tasks per run:   %d\n", $NUM_TASKS;
printf "Runs:            %d\n", $NUM_RUNS;
printf "Broker:          %s\n", $BROKER_URL;
printf "Perl:            %s\n", $^V;
printf "AIO::Celery:     0.01\n";
print "=" x 60, "\n";


# --- Benchmark 1: Message Serialization ---
sub bench_serialize {
    my ($n) = @_;
    my $start = time();
    for my $i (0 .. $n - 1) {
        my $msg = create_task_message(
            task_id   => sprintf("task-%06d", $i),
            task_name => 'tasks.add',
            args      => [$i, $i + 1],
            kwargs    => { extra => 'value' },
        );
    }
    return time() - $start;
}


# --- Benchmark 2: Message Deserialization ---
sub bench_deserialize {
    my ($n) = @_;
    # Create a sample message
    my $sample = create_task_message(
        task_id   => 'sample-task',
        task_name => 'tasks.add',
        args      => [1, 2],
        kwargs    => { key => 'val' },
    );
    my $raw_body    = $sample->{body};
    my $raw_headers = $sample->{headers};

    my $start = time();
    for my $i (0 .. $n - 1) {
        my $req = AIO::Celery::Request->from_message($sample);
        my $task_id   = $req->id;
        my $task_name = $req->task;
        my $retries   = $req->retries;
    }
    return time() - $start;
}


# --- Benchmark 3: Publish to RabbitMQ ---
sub bench_publish {
    my ($n) = @_;

    my $loop = IO::Async::Loop->new;

    my $app = AIO::Celery::App->new(name => 'bench');
    $app->conf->update(broker_url => $BROKER_URL);

    # Setup connection first
    $loop->await($app->setup(loop => $loop));

    # Now benchmark the actual publishing
    my $start = time();

    my @futures;
    for my $i (0 .. $n - 1) {
        push @futures, $app->send_task('tasks.add', args => [$i, $i + 1]);
    }
    $loop->await(Future->wait_all(@futures));

    my $elapsed = time() - $start;

    $loop->await($app->teardown);
    return $elapsed;
}


# --- Run ---
my %results;
$results{$_} = [] for qw(serialize deserialize publish);

for my $run (1 .. $NUM_RUNS) {
    print "\n--- Run $run/$NUM_RUNS ---\n";

    # Serialize
    my $t = bench_serialize($NUM_TASKS);
    push @{$results{serialize}}, $t;
    printf "  Serialize %d messages:   %.3fs  (%.0f msg/s)\n", $NUM_TASKS, $t, $NUM_TASKS / $t;

    # Deserialize
    $t = bench_deserialize($NUM_TASKS);
    push @{$results{deserialize}}, $t;
    printf "  Deserialize %d messages: %.3fs  (%.0f msg/s)\n", $NUM_TASKS, $t, $NUM_TASKS / $t;

    # Publish
    $t = bench_publish($NUM_TASKS);
    push @{$results{publish}}, $t;
    printf "  Publish %d to RabbitMQ:  %.3fs  (%.0f msg/s)\n", $NUM_TASKS, $t, $NUM_TASKS / $t;
}

# Summary
print "\n", "=" x 60, "\n";
print "PERL RESULTS (averages)\n";
print "=" x 60, "\n";
for my $key (qw(serialize deserialize publish)) {
    my @times = @{$results{$key}};
    my $avg   = 0; $avg += $_ for @times; $avg /= @times;
    my $best  = (sort { $a <=> $b } @times)[0];
    my $worst = (sort { $b <=> $a } @times)[0];
    my $rate  = $NUM_TASKS / $avg;
    printf "  %-15s: avg=%.3fs  best=%.3fs  worst=%.3fs  (%.0f msg/s)\n",
        $key, $avg, $best, $worst, $rate;
}

# Machine-readable
for my $key (qw(serialize deserialize publish)) {
    my @times = @{$results{$key}};
    my $avg = 0; $avg += $_ for @times; $avg /= @times;
    printf "PERL_%s_AVG=%.6f\n", uc($key), $avg;
}
print "\n";
