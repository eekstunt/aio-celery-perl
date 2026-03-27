package AIO::Celery::InspectionHTTPServer;
use strict;
use warnings;
use JSON::MaybeXS qw(encode_json);
use Log::Any qw($log);
use POSIX qw(strftime);
use AIO::Celery::State qw(%RUNNING_TASKS);

sub start {
    my (%args) = @_;
    my $loop = $args{loop} // die "loop is required";
    my $host = $args{host} // 'localhost';
    my $port = $args{port} // 1430;

    require Net::Async::HTTP::Server;

    my $server = Net::Async::HTTP::Server->new(
        on_request => sub {
            my ($self, $req) = @_;
            my $path = $req->path;
            my $response;

            if ($path eq '/health') {
                $response = _respond($req, 200, '{}');
            } else {
                my $stats = _collect_stats();
                $response = _respond($req, 200, encode_json($stats));
            }
        },
    );

    $loop->add($server);

    $server->listen(
        addr => {
            family   => 'inet',
            socktype => 'stream',
            ip       => $host,
            port     => $port,
        },
    )->on_done(sub {
        $log->info("Inspection HTTP server listening on $host:$port");
    })->on_fail(sub {
        my ($err) = @_;
        $log->error("Failed to start inspection HTTP server: $err");
    });

    return $server;
}

sub _respond {
    my ($req, $status, $body) = @_;
    my $resp = HTTP::Response->new($status);
    $resp->content_type('application/json');
    $resp->content($body . "\n");
    $req->respond($resp);
}

sub _collect_stats {
    my @running_tasks;
    for my $id (sort keys %RUNNING_TASKS) {
        push @running_tasks, $RUNNING_TASKS{$id}->serialize;
    }

    # Count states
    my %state_counts;
    for my $t (values %RUNNING_TASKS) {
        $state_counts{lc($t->state)}++;
    }

    return {
        running_tasks      => \@running_tasks,
        running_task_count => scalar(@running_tasks),
        state_counts       => \%state_counts,
        timestamp          => strftime('%Y-%m-%dT%H:%M:%SZ', gmtime()),
        pid                => $$,
    };
}

1;
