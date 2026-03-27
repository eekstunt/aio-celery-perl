package AIO::Celery::Utils;
use strict;
use warnings;
use Exporter 'import';
use Data::UUID;

our @EXPORT_OK = qw(first_not_null generate_consumer_tag);

my $_uuid_gen = Data::UUID->new;

sub first_not_null {
    for my $item (@_) {
        return $item if defined $item;
    }
    return undef;
}

sub generate_consumer_tag {
    my (%args) = @_;
    my $prefix         = $args{prefix}         // '';
    my $channel_number = $args{channel_number} // 1;
    my $hex = lc($_uuid_gen->create_hex);
    $hex =~ s/^0x//;
    return "${prefix}ctag${channel_number}.${hex}";
}

1;
