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

package POE::Component::MessageQueue::Storage::BigMemory;
use Moose;
with qw(POE::Component::MessageQueue::Storage);

use POE::Component::MessageQueue::Storage::Structure::DLList;

use constant empty_hashref => (is => 'ro', default => sub { {} });
# claimer_id => DLList[Message]
has 'claimed'   => empty_hashref;
# queue_name => DLList[Message]
has 'unclaimed' => empty_hashref;
# message_id => DLList[Message] ... messages = claimed UNION unclaimed
has 'messages'  => empty_hashref;

make_immutable;

sub _force_store {
	my ($self, $hashname, $key, $message) = @_;

	unless ( exists $self->{$hashname}->{$key} )
	{
		$self->{$hashname}->{$key} = 
			POE::Component::MessageQueue::Storage::Structure::DLList->new();
	}

	$self->{messages}->{$message->id} = 
		$self->{$hashname}->{$key}->enqueue($message); 
	return;
}

sub store
{
	my ($self, $message, $callback) = @_;

	if ( $message->claimed )
	{
		$self->_force_store('claimed', $message->claimant, $message);
	}
	else
	{
		$self->_force_store('unclaimed', $message->destination, $message);
	}

	$self->log('info', sprintf('Added %s.', $message->id));
	$callback->($message) if $callback;
	return;
}

sub peek
{
	my ($self, $ids, $callback) = @_;
	my @messages; # can't just map cause they may not all be there...
	foreach my $id (@$ids)
	{
		my $msg = $self->messages->{$id};
		push(@messages, $msg) if $msg;
	}
	$callback->(\@messages);
	return;
}

sub remove
{
	my ($self, $ids, $callback) = @_;
	my @removed;

	foreach my $id (@$ids)
	{
		my $cell = delete $self->messages->{$id};
		next unless $cell;
		my $message = $cell->delete();
		push(@removed, $message);
		
		if ( $message->claimed )
		{
			delete $self->claimed->{$message->claimant};
		}
		else
		{
			delete $self->unclaimed->{$message->destination};
		}
	}
	$callback->(\@removed) if $callback;
	return;
}

sub empty 
{
	my ($self, $callback) = @_;
	if ($callback)
	{
		my @messages = map { $_->data() } (values %{$self->{messages}});
		$callback->(\@messages);	
	}
	%{$self->{$_}} = () foreach qw(messages claimed unclaimed);
	return;
}

sub claim_and_retrieve
{
	my ($self, $destination, $client_id, $dispatch) = @_;
	my ($q, $message);

	# Find an unclaimed message
	if (($q = $self->unclaimed->{$destination}) &&
	    ($message = $q->dequeue()))
	{
		# Claim it
		$message->claim($client_id);
		$self->_force_store('claimed', $client_id, $message);
		$self->log('info', sprintf('Message %s claimed by client %s',
			$message->id, $client_id));
	}

	# Dispatch it (even if undef)
	$dispatch->($message, $destination, $client_id);
}

# unmark all messages owned by this client
sub disown
{
	my ($self, $destination, $client_id) = @_;
	my $q = $self->claimed->{$client_id} || return;

	for(my $i = $q->first(); $i; $i = $i->next())
	{
		my $message = $i->data();
		if ($message->destination eq $destination)
		{
			$i->delete(); # ->next() still valid though.
			$self->_force_store('unclaimed', $destination, $message);
			$message->disown();
		}
	}
	return;
}

# We don't persist anything, so just call our complete handler.
sub storage_shutdown
{
	my ($self, $complete) = @_;
	$complete->();	
	return;
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
