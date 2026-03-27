package AIO::Celery::Request;
use Moo;
use Types::Standard qw(Str Int ArrayRef HashRef Maybe Num);
use JSON::MaybeXS qw(decode_json);
use DateTime::Format::ISO8601;
use Carp qw(croak);
use namespace::clean;

has message       => (is => 'ro', required => 1);
has id            => (is => 'ro', isa => Str,             required => 1);
has task          => (is => 'ro', isa => Str,             required => 1);
has args          => (is => 'ro', isa => ArrayRef,        default  => sub { [] });
has kwargs        => (is => 'ro', isa => HashRef,         default  => sub { {} });
has retries       => (is => 'ro', isa => Int,             default  => 0);
has eta           => (is => 'ro',                         default  => undef);
has parent_id     => (is => 'ro', isa => Maybe[Str],      default  => undef);
has group         => (is => 'ro', isa => Maybe[Str],      default  => undef);
has root_id       => (is => 'ro', isa => Maybe[Str],      default  => undef);
has ignore_result => (is => 'ro',                         default  => 0);
has timelimit     => (is => 'ro', isa => ArrayRef,        default  => sub { [undef, undef] });
has chain         => (is => 'ro', isa => ArrayRef,        default  => sub { [] });

sub from_message {
    my ($class, $message) = @_;

    my $headers = $message->{headers} // croak "Message has no headers";
    my $body_raw = $message->{body} // '[[],{},{}]';

    my $body;
    if (ref $body_raw) {
        $body = $body_raw;
    } else {
        $body = decode_json($body_raw);
    }

    my ($args, $kwargs, $options) = @$body;
    $args    //= [];
    $kwargs  //= {};
    $options //= {};

    my $eta;
    if (defined $headers->{eta}) {
        eval {
            $eta = DateTime::Format::ISO8601->parse_datetime($headers->{eta});
        };
        if ($@) {
            warn "Failed to parse ETA '$headers->{eta}': $@";
            $eta = undef;
        }
    }

    my $ignore_result = $headers->{ignore_result};
    if (ref $ignore_result && JSON::MaybeXS::is_bool($ignore_result)) {
        $ignore_result = $ignore_result ? 1 : 0;
    }

    my $chain = $options->{chain} // [];

    return $class->new(
        message       => $message,
        id            => $headers->{id} // croak("Message has no id header"),
        task          => $headers->{task} // croak("Message has no task header"),
        args          => $args,
        kwargs        => $kwargs,
        retries       => $headers->{retries} // 0,
        eta           => $eta,
        parent_id     => $headers->{parent_id},
        group         => $headers->{group},
        root_id       => $headers->{root_id},
        ignore_result => $ignore_result // 0,
        timelimit     => $headers->{timelimit} // [undef, undef],
        chain         => $chain,
    );
}

sub build_retry_message {
    my ($self, %args) = @_;
    my $countdown = $args{countdown} // croak "countdown is required";

    # Deep copy the original message
    my $msg = { %{$self->message} };
    my $headers = { %{$msg->{headers}} };
    $msg->{headers} = $headers;

    # Increment retries
    $headers->{retries} = ($headers->{retries} // 0) + 1;

    # Compute new ETA
    my $now = DateTime->now(time_zone => 'local');
    $now->add(seconds => $countdown);
    my $eta_str = $now->strftime('%Y-%m-%dT%H:%M:%S.000000%z');
    $eta_str =~ s/(\d{2})(\d{2})$/$1:$2/;
    $headers->{eta} = $eta_str;

    # Set parent_id to current task
    $headers->{parent_id} = $self->id;

    return $msg;
}

1;
