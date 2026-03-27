package AIO::Celery::AMQP;
use strict;
use warnings;
use Exporter 'import';
use JSON::MaybeXS qw(encode_json JSON);
use Data::UUID;
use DateTime;
use DateTime::Duration;
use Sys::Hostname qw(hostname);
use POSIX qw(getpid);

our @EXPORT_OK = qw(create_task_message);

my $_uuid_gen = Data::UUID->new;

sub _generate_uuid {
    my $hex = lc($_uuid_gen->create_hex);
    $hex =~ s/^0x//;
    # Format as standard UUID: 8-4-4-4-12
    return join('-',
        substr($hex, 0, 8),
        substr($hex, 8, 4),
        substr($hex, 12, 4),
        substr($hex, 16, 4),
        substr($hex, 20, 12),
    );
}

sub _perl_repr {
    my ($val) = @_;
    if (!defined $val) {
        return 'None';
    }
    if (ref $val eq 'ARRAY') {
        my @items = map { _perl_repr($_) } @$val;
        if (@items == 1) {
            return '(' . $items[0] . ',)';
        }
        return '(' . join(', ', @items) . ')';
    }
    if (ref $val eq 'HASH') {
        my @pairs;
        for my $k (sort keys %$val) {
            push @pairs, "'" . $k . "': " . _perl_repr($val->{$k});
        }
        return '{' . join(', ', @pairs) . '}';
    }
    if (JSON::MaybeXS::is_bool($val)) {
        return $val ? 'True' : 'False';
    }
    if ($val =~ /\A-?\d+(\.\d+)?\z/) {
        return "$val";
    }
    return "'" . $val . "'";
}

sub _compute_eta {
    my ($countdown) = @_;
    return undef unless defined $countdown;
    my $now = DateTime->now(time_zone => 'local');
    $now->add(seconds => $countdown);
    # Format matching Python: 2024-01-15T12:30:45.123456+00:00
    my $str = $now->strftime('%Y-%m-%dT%H:%M:%S.000000%z');
    # Perl's %z gives +0400, but ISO 8601 needs +04:00
    $str =~ s/(\d{2})(\d{2})$/$1:$2/;
    return $str;
}

sub create_task_message {
    my (%args) = @_;

    my $task_id       = $args{task_id}       // _generate_uuid();
    my $task_name     = $args{task_name}     // die "task_name is required";
    my $args_list     = $args{args}          // [];
    my $kwargs_hash   = $args{kwargs}        // {};
    my $priority      = $args{priority};
    my $parent_id     = $args{parent_id};
    my $root_id       = $args{root_id}       // $task_id;
    my $chain         = $args{chain}         // [];
    my $ignore_result = $args{ignore_result} // 0;
    my $countdown     = $args{countdown};
    my $reply_to      = $args{reply_to}      // '';
    my $expiration    = $args{expiration};
    my $retries       = $args{retries}       // 0;

    my $eta = _compute_eta($countdown);

    my $origin = sprintf('%s@%s', getpid(), hostname());

    my $body = encode_json([
        $args_list,
        $kwargs_hash,
        {
            callbacks => undef,
            errbacks  => undef,
            chain     => $chain,
            chord     => undef,
        },
    ]);

    my $headers = {
        argsrepr        => _perl_repr($args_list),
        eta             => $eta,
        expires         => undef,
        group           => undef,
        group_index     => undef,
        id              => $task_id,
        ignore_result   => $ignore_result ? JSON::MaybeXS::true : JSON::MaybeXS::false,
        kwargsrepr      => _perl_repr($kwargs_hash),
        lang            => 'py',
        origin          => $origin,
        parent_id       => $parent_id,
        retries         => $retries,
        root_id         => $root_id,
        shadow          => undef,
        stamped_headers => undef,
        stamps          => {},
        task            => $task_name,
        timelimit       => [undef, undef],
    };

    return {
        body             => $body,
        headers          => $headers,
        content_type     => 'application/json',
        content_encoding => 'utf-8',
        delivery_mode    => 2,
        priority         => $priority,
        correlation_id   => $task_id,
        reply_to         => $reply_to,
        expiration       => $expiration,
    };
}

1;
