# $Id$

package POE::Component::MessageQueue::Statistics::Publish::YAML;
use strict;
use warnings;
use base qw(POE::Component::MessageQueue::Statistics::Publish);
use Best
    [ qw(YAML::Syck YAML) ], qw(Dump)
;
use File::Temp;

sub publish_file
{
    my ($self, $filename) = @_;

    # Be friendly to people who might be reading the file
    my $fh = File::Temp->new(UNLINK => 0);
    eval {
        $fh->print( Dump( $self->{statistics}->{statistics} ) );
        # I want to use system's rename(). Don't know how portable that is
        rename($fh->filename, $filename) or die "Failed to rename $fh to $filename";
    };
    if (my $e = $@) {
        $fh->unlink_on_destroy( 1 ) if $fh;
        die $e;
    }
}

sub publish_handle
{
    my ($self, $handle) = @_;
    $handle->print( Dump( $self->{statistics}->{statistics} ) );
}

1;

__END__

=head1 NAME

POE::Component::MessageQueue::Statistics::Publish::YAML - Publish Statistics In YAML Format

=head1 SYNOPSIS

  use POE::Component::MessageQueue::Statistics;
  use POE::Component::MessageQueue::Statistics::Publish::YAML;

  # This is initialized elsewhere
  my $stats   = POE::Component::MessageQueue::Statistics->new();

  my $publish = POE::Component::MessageQueue::Statistics::Publish::YAML->new(
    output => \*STDOUT, 
  );
  $publish->publish($stats);

=cut