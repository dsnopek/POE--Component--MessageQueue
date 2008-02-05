#
# Copyright 2007, 2008 Paul Driver <frodwith@gmail.com>
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

use strict;
package POE::Component::MessageQueue::Storage::BigMemory;
use base qw(POE::Component::MessageQueue::Storage);

use POE::Component::MessageQueue::Storage::Structure::DLList;

sub new
{
	my $class = shift;
	my $self  = $class->SUPER::new(@_);

	# claimed messages (removed from named queues when claimed).
	# Key: Client ID.
	# Value: A doubly linked queue of messages.
	$self->{claimed} = {};

	# Named queues.
	# Key: Queue Name
	# Value: A doubly linked queue of messages
	$self->{unclaimed} = {};   

	# All messages.
	# Key: A message id
	# Value: A cell in a doubly linked queue
	$self->{messages} = {};

	return bless $self, $class;
}

# O(1)
sub has_message
{
	my ($self, $id) = @_;

	return ( exists($self->{messages}->{$id}) );
}

# this function will clear out the engine and return an array reference
# with all the messages on it.
sub empty_all
{
	my $self = shift;
	my $old = $self->{messages};
	$self->{messages} = {};
	$self->{claimed} = {};
	$self->{unclaimed} = {};

	my @messages = map { $_->data() } (values %$old);
	return \@messages;
}

sub _force_store {
	my ($self, $hashname, $key, $message) = @_;
	my $id = $message->{message_id}; 
	if ( !exists $self->{$hashname}->{$key} )
	{
		$self->{$hashname}->{$key} = 
			POE::Component::MessageQueue::Storage::Structure::DLList->new();
	}
	$self->{messages}->{$id} = $self->{$hashname}->{$key}->enqueue($message); 
}

sub store
{
	my ($self, $message) = @_;
	my $claimant = $message->{in_use_by};

	if ( defined $claimant )
	{
		$self->_force_store('claimed', $claimant, $message);
	}
	else
	{
		$self->_force_store('unclaimed', $message->{destination}, $message);
	}

	$self->_log('info', "STORE: BIGMEMORY: Added $message->{message_id}.");
	my $handler = $self->{message_stored};
	$handler->($message) if $handler;  
}

# O(1)
sub remove
{
	my ($self, $id) = @_;
	return 0 unless ( exists $self->{messages}->{$id} );

	my $message = $self->{messages}->{$id}->delete();
	my $claimant = $message->{in_use_by};

	if ( defined $claimant )
	{
		delete $self->{claimed}->{$claimant};
	}
	else
	{
		delete $self->{unclaimed}->{$message->{destination}};
	}

	delete $self->{messages}->{$id};
	$self->_log('info', "STORE: BIGMEMORY: Removed $id from in-memory store");
	return $message;
}

sub remove_multiple
{
	my ($self, $message_ids) = @_;

	my @removed = grep {$_} map {$self->remove($_)}(@$message_ids);
	return \@removed;
}

# O(1)
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

	# Find an unclaimed message
	my $q = $self->{unclaimed}->{$destination} || return 0;
	my $message = $q->dequeue() || return 0;

	# Claim it
	$message->{in_use_by} = $client_id;
	$self->_force_store('claimed', $client_id, $message);
	$self->_log('info',
		"STORE: BIGMEMORY: Message $message->{message_id} ".
		"claimed by client $client_id."
	);

	# Dispatch it
	my $dispatcher = $self->{dispatch_message} ||
		die "No dispatch_message handler"; 
	$dispatcher->($message, $destination, $client_id);
	$self->{destination_ready}->( $destination );
}

# unmark all messages owned by this client
sub disown
{
	my ($self, $destination, $client_id) = @_;
	my $q = $self->{claimed}->{$client_id} || return;
	my $iterator = $q->first();

	while ($iterator = $iterator->next())
	{
		my $message = $iterator->data();
		if ($message->{destination} eq $destination)
		{
			$iterator->delete(); # ->next() still valid though.
			$self->_force_store('unclaimed', $destination, $message);
			delete $message->{in_use_by};
		}
	}
}

# We don't persist anything, so just call our complete handler.
sub shutdown
{
	my $self = shift;
	my $handler = $self->{shutdown_complete};
	$handler->() if $handler;
}

1;

__END__

=pod

=head1 NAME

POE::Component::MessageQueue::Storage::BigMemory -- In-memory storage engine
optimized for a large number of messages.

=head1 SYNOPSIS

  use POE;
  use POE::Component::MessageQueue;
  use POE::Component::MessageQueue::Storage::BigMemory;
  use strict;

  POE::Component::MessageQueue->new({
    storage => POE::Component::MessageQueue::Storage::BigMemory->new()
  });

  POE::Kernel->run();
  exit;

=head1 DESCRIPTION

An in-memory storage engine that is optimised for a large number of messages.
Its an alternative to L<POE::Componenent::MessageQueue::Storage::Memory>, which
stores everything in a Perl ARARY, which can slow the MQ to a CRAWL when the
number of messsages in this store gets big.  

store() is a little bit slower per message in this module and it uses
more memory per message. Everything else should be considerably more efficient,
though, especially when the number of messages starts to climb.  Many operations
in Storage::Memory are O(n*n).  Most operations in this module are O(1)!

I wouldn't suggest using this as your main storage engine because if messages aren't
removed by consumers, it will continue to consume more memory until it explodes.  Check-out
L<POE::Component::MessageQueue::Storage::Complex> which uses this module internally to keep
messages in memory for a period of time before moving them into persistent storage.

=head1 SEE ALSO

L<POE::Component::MessageQueue>,
L<POE::Component::MessageQueue::Storage>

I<Other storage engines:>

L<POE::Component::MessageQueue::Storage::Memory>,
L<POE::Component::MessageQueue::Storage::FileSystem>,
L<POE::Component::MessageQueue::Storage::DBI>,
L<POE::Component::MessageQueue::Storage::Generic>,
L<POE::Component::MessageQueue::Storage::Generic::DBI>,
L<POE::Component::MessageQueue::Storage::Throttled>,
L<POE::Component::MessageQueue::Storage::Complex>,
L<POE::Component::MessageQueue::Storage::Default>

=cut
