#
# Copyright 2007 David Snopek <dsnopek@gmail.com>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

package POE::Component::MessageQueue::Storage::Memory;
use base qw(POE::Component::MessageQueue::Storage);

use strict;

use Data::Dumper;

sub new
{
	my $class = shift;
	my $self  = $class->SUPER::new( @_ );

	$self->{message_id} = 0;
	$self->{messages}   = { }; # destination => @messages

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

	foreach my $dest ( keys %{$self->{messages}} )
	{
		my $messages = $self->{messages}->{$dest};
		foreach my $message ( @{$messages} )
		{
			if ( $message->{message_id} == $message_id )
			{
				return 1;
			}
		}
	}

	return 0;
}

# this function will clear out the engine and return an array reference
# with all the messages on it.
sub empty_all
{
	my $self = shift;

	my @ret;

	foreach my $messages ( values %{$self->{messages}} )
	{
		@ret = ( @ret, @$messages );
	}
	$self->{messages} = {};

	return \@ret;
}

sub store
{
	my ($self, $message) = @_;

	# push onto our array
	$self->{messages}{ $message->{destination} } ||= [];
	push @{$self->{messages}{$message->{destination}}}, $message;
	$self->_log( 
		"STORE: MEMORY: Added $message->{message_id} to in-memory store" 
	);

	# call the message_stored handler
	if ( defined $self->{message_stored} )
	{
		$self->{message_stored}->( $message );
	}
}

sub remove
{
	my ($self, $message_id) = @_;

	foreach my $dest ( keys %{$self->{messages}} )
	{
		my $messages = $self->{messages}->{$dest};
		my $max = scalar @{$messages};

		# find the message and remove it
		for ( my $i = 0; $i < $max; $i++ )
		{
			if ( $messages->[$i]->{message_id} == $message_id )
			{
				splice @{$messages}, $i, 1;

				# return 1 to denote that a message was actually removed
				return 1;
			}
		}
	}

	return 0;
}

sub remove_multiple
{
	my ($self, $message_ids) = @_;

	my @removed;
	while (my($dest, $messages) = each %{ $self->{messages} }) {
		my $max = scalar @{$messages};

		# find the message and remove it
		for ( my $i = 0; $i < $max; $i++ )
		{
			my $message = $messages->[$i];

			# check if its on the list of message ids
			foreach my $other_id ( @$message_ids )
			{
				if ( $message->{message_id} == $other_id )
				{
					# put on our list
					push @removed, $message;

					# remove
					splice @{$messages}, $i--, 1;

					# move onto next message
					last;
				}
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

	my @messages = @{ $self->{messages}{$destination} || [] };

	# look for an unclaimed message and take it
	foreach my $message (@messages)
	{
		if ( not defined $message->{in_use_by} )
		{
			if ( not defined $self->{dispatch_message} )
			{
				die "Pulled message from backstore, but there is no dispatch_message handler";
			}

			# claim it, yo!
			$message->{in_use_by} = $client_id;
			$self->_log('info',
				"STORE: MEMORY: Message $message->{message_id} ".
				"claimed by client $client_id."
			);

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

	my $messages = $self->{messages}{$destination} || [];
	foreach my $message ( @{$messages} )
	{
		if ( $message->{in_use_by} == $client_id )
		{
			$message->{in_use_by} = undef;
		}
	}
}

sub shutdown
{
	my $self = shift;

	# this storage engine is so simple, it has nothing to do to
	# shutdown!  Since its purely in memory, it can't even persist
	# any messages.
	if ( defined $self->{shutdown_complete} )
	{
		$self->{shutdown_complete}->();
	}
}

1;

__END__

=pod

=head1 NAME

POE::Component::MessageQueue::Storage::Memory -- In memory storage engine.

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

A storage engine that keeps all the messages in memory.  Provides no persistence
what-so-ever.

I wouldn't suggest using this as your main storage engine because if messages aren't
removed by consumers, it will continue to consume more memory until it explodes.  Check-out
L<POE::Component::MessageQueue::Storage::Complex> which uses this module internally to keep
messages in memory for a period of time before moving them into persistent storage.

=head1 CONSTRUCTOR PARAMETERS

None to speak of!

=head1 SEE ALSO

L<POE::Component::MessageQueue>,
L<POE::Component::MessageQueue::Storage>,
L<POE::Component::MessageQueue::Storage::FileSystem>,
L<POE::Component::MessageQueue::Storage::DBI>,
L<POE::Component::MessageQueue::Storage::Generic>,
L<POE::Component::MessageQueue::Storage::Generic::DBI>,
L<POE::Component::MessageQueue::Storage::Throttled>,
L<POE::Component::MessageQueue::Storage::Complex>

=cut
