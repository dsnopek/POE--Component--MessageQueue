
package POE::Component::MessageQueue::Storage::Memory;
use base qw(POE::Component::MessageQueue::Storage);

use strict;

use Data::Dumper;

sub new
{
	my $class = shift;
	my $self  = $class->SUPER::new( @_ );

	$self->{message_id} = 0;
	$self->{messages}   = [ ];

	return $self;
}

sub get_next_message_id
{
	my $self = shift;
	return ++$self->{message_id};
}

sub has_message
{
	my ($self, $message_id) = @_;

	# find the message and remove it
	foreach my $message ( @{$self->{messages}} )
	{
		if ( $message->{message_id} == $message_id )
		{
			return 1;
		}
	}

	return 0;
}

sub store
{
	my ($self, $message) = @_;

	# push onto our array
	push @{$self->{messages}}, $message;

	# call the message_stored handler
	if ( defined $self->{message_stored} )
	{
		$self->{message_stored}->( $message->{destination} );
	}
}

sub remove
{
	my ($self, $message_id) = @_;

	my $max = scalar @{$self->{messages}};

	# find the message and remove it
	for( my $i = 0; $i < $max; $i++ )
	{
		if ( $self->{messages}->[$i]->{message_id} == $message_id )
		{
			splice @{$self->{messages}}, $i, 1;

			# return 1 to denote that a message was actually removed
			return 1;
		}
	}

	return 0;
}

sub remove_multiple
{
	my ($self, $message_ids) = @_;

	my $max = scalar @{$self->{messages}};
	my @removed;

	# find the message and remove it
	for( my $i = 0; $i < $max; $i++ )
	{
		my $message = $self->{messages}->[$i];

		# check if its on the list of message ids
		foreach my $other_id ( @$message_ids )
		{
			if ( $message->{message_id} == $other_id )
			{
				# put on our list
				push @removed, $message;

				# remove
				splice @{$self->{messages}}, $i--, 1;

				# move onto next message
				last;
			}
		}
	}

	return \@removed;
}

sub claim_and_retrieve
{
	my $self = shift;
	my $args = shift;

	my $destination;
	my $client_id;

	if ( ref($args) eq 'HASH' )
	{
		$destination = $args->{destination};
		$client_id   = $args->{client_id};
	}
	else
	{
		$destination = $args;
		$client_id   = shift;
	}

	my $max = scalar @{$self->{messages}};

	# look for an unclaimed message and take it
	for ( my $i = 0; $i < $max; $i++ )
	{
		my $message = $self->{messages}->[$i];

		if ( $message->{destination} eq $destination and not defined $message->{in_use_by} )
		{
			if ( not defined $self->{dispatch_message} )
			{
				die "Pulled message from backstore, but there is no dispatch_message handler";
			}

			# claim it, yo!
			$message->{in_use_by} = $client_id;

			# dispatch message
			$self->{dispatch_message}->( $message, $destination, $client_id );

			# let it know that the destination is ready
			$self->{destination_ready}->( $destination );

			# we are always capable to attempt to claim
			return 1;
		}
	}
	
	return 0;
}

# unmark all messages owned by this client
sub disown
{
	my ($self, $destination, $client_id) = @_;

	foreach my $message ( @{$self->{messages}} )
	{
		if ( $message->{destination} eq $destination and $message->{in_use_by} == $client_id )
		{
			$message->{in_use_by} = undef;
		}
	}
}

1;

__END__

=pod

=head1 NAME

POE::Component::MessageQueue::Storage::Memory -- In memory storage backend.

=head1 SYNOPSIS

  use POE;
  use POE::Component::MessageQueue;
  use POE::Component::MessageQueue::Storage::Memory;
  use strict;

  POE::Component::MessageQueue->new({
    storage => POE::Component::MessageQueue::Storage::Memory->new()
  });

  POE::Kernel->run();
  exit;

=head1 DESCRIPTION

A storage backend that keeps all the messages in memory.  Provides no persistence
what-so-ever.

I wouldn't suggest using this as your main storage backend because if messages aren't
removed by consumers, it will continue to consume more memory until it explodes.  Check-out
L<POE::Component::MessageQueue::Complex> which uses this module internally to keep messages
in memory for a period of time before moving them into persistent storage.

=head1 CONSTRUCTOR PARAMETERS

None to speak of!

=head1 SEE ALSO

L<POE::Component::MessageQueue>,
L<POE::Component::MessageQueue::Storage>,
L<POE::Component::MessageQueue::Storage::DBI>,
L<POE::Component::MessageQueue::Storage::FileSystem>,
L<POE::Component::MessageQueue::Storage::Complex>

=cut
