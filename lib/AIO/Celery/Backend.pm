package AIO::Celery::Backend;
use strict;
use warnings;
use Exporter 'import';
use URI;
use Log::Any qw($log);
use Carp qw(croak);

our @EXPORT_OK = qw(create_redis_connection);

sub create_redis_connection {
    my (%args) = @_;
    my $loop = $args{loop} // croak "loop is required";
    my $url  = $args{url}  // croak "url is required";

    require Net::Async::Redis;

    my $uri  = URI->new($url);
    my $host = $uri->host // 'localhost';
    my $port = $uri->port // 6379;

    # Extract database number from path (e.g., /0, /1)
    my $path = $uri->path // '';
    my $db   = 0;
    if ($path =~ m{^/(\d+)$}) {
        $db = $1;
    }

    # Extract password from userinfo
    my $auth;
    if (my $userinfo = $uri->userinfo) {
        my @parts = split(/:/, $userinfo, 2);
        $auth = $parts[1] // $parts[0];
    }

    my $redis = Net::Async::Redis->new(
        host => $host,
        port => $port,
        (defined $auth ? (auth => $auth) : ()),
        ($db ? (db => $db) : ()),
    );

    $loop->add($redis);

    $log->info("Redis backend connecting to $host:$port/$db");

    return $redis;
}

1;
