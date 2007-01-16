
package POE::Component::MessageQueue::Subscription;

use strict;

sub new
{
	my $class = shift;
	my $args  = shift;

	my $client;
	my $ack_type;

	if ( ref($args) eq 'HASH' )
	{
		$client   = $args->{client};
		$ack_type = $args->{ack_type};
	}
	else
	{
		$client   = $args;
		$ack_type = shift;
	}

	my $self =
	{
		client    => $client,
		ack_type  => $ack_type,
		ready     => 1
	};

	bless  $self, $class;
	return $self;
}

sub get_client            { return shift->{client}; }
sub get_ack_type          { return shift->{ack_type}; }
sub is_ready              { return shift->{ready}; }
sub set_handling_message  { shift->{ready} = 0; }
sub set_done_with_message { shift->{ready} = 1; }

1;

