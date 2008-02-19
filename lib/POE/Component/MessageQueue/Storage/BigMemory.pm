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

package POE::Component::MessageQueue::Storage::BigMemory::MessageElement;
use Heap::Elem;
use base qw(Heap::Elem);

sub new
{
	my ($class, $message) = @_;
	my $self = $class->SUPER::new;

	$self->val($message);	
	bless($self, $class);
}

sub cmp
{
	my ($self, $other) = @_;
	return $self->val->timestamp - $other->val->timestamp;
}

1;

package POE::Component::MessageQueue::Storage::BigMemory;
use Moose;
with qw(POE::Component::MessageQueue::Storage);

use Heap::Fibonacci;

use constant empty_hashref => (is => 'ro', default => sub { {} });

# claimer_id => heap element
has 'claimed'   => empty_hashref;
# queue_name => heap of messages
has 'unclaimed' => empty_hashref;
# message_id => heap element
has 'messages'  => empty_hashref;

has 'message_heap' => (
	is      => 'rw',
	default => sub { Heap::Fibonacci->new },
);

make_immutable;

# Where messages are stored:
#   -- A heap of all unclaimed messages sorted by timestamp
#   -- Per destination heaps for unclaimed messages
#   -- A hash of claimant => messages.
#
# There is also a hash of ids to info about heap elements and such.

sub _make_heap_elem
{
	POE::Component::MessageQueue::Storage::BigMemory::MessageElement->new(@_);
}

sub store
{
	my ($self, $message, $callback) = @_;

	my $elem = _make_heap_elem($message);
	my $main = _make_heap_elem($message);
	$self->message_heap->add($main);

	my $info = $self->messages->{$message->id} = {
		message => $message,
		main    => $main,
	};

	if ($message->claimed) 
	{
		$self->claimed->{$message->claimant}->{$message->destination} = $elem;
	}
	else
	{
		my $heap = 
			($self->unclaimed->{$message->destination} ||= Heap::Fibonacci->new);
		$heap->add($elem);
		$info->{unclaimed} = $elem;
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
		my $info = $self->messages->{$id};
		push(@messages, $info->{message}) if $info;
	}
	$callback->(\@messages);
	return;
}

sub peek_oldest
{
	my ($self, $callback) = @_;
	my $top = $self->message_heap->top;
	$callback->($top && $top->val);
}

sub remove
{
	my ($self, $ids, $callback) = @_;
	my @removed;

	foreach my $id (@$ids)
	{
		my $info = delete $self->messages->{$id};
		next unless $info && $info->{message};
		my $msg = $info->{message};

		$self->message_heap->delete($info->{main});
		if ($msg->claimed)
		{
			delete $self->claimed->{$msg->claimant}->{$msg->destination};
		}
		else
		{
			$self->unclaimed->{$msg->destination}->delete($info->{unclaimed});
		}
		push(@removed, $msg);
	}

	$callback->(\@removed) if $callback;
	return;
}

sub empty 
{
	my ($self, $callback) = @_;
	if ($callback)
	{
		my @messages = map {$_->{message}} (values %{$self->messages});
		$callback->(\@messages);	
	}
	%{$self->{$_}} = () foreach qw(messages claimed unclaimed);
	$self->message_heap(Heap::Fibonacci->new);
	return;
}

sub claim_and_retrieve
{
	my ($self, $destination, $client_id, $dispatch) = @_;
	my $heap = $self->unclaimed->{$destination};
	my $message;

	if ($heap && (my $elem = $heap->extract_top()))
	{
		$message = $elem->val;

		$message->claim($client_id);
		$self->claimed->{$client_id}->{$destination} = $elem;
		delete $self->messages->{$message->id}->{unclaimed};

		$self->log('info', sprintf('Message %s claimed by client %s',
			$message->id, $client_id));
	}

	# Dispatch it (even if undef)
	$dispatch->($message, $destination, $client_id);
}

sub disown
{
	my ($self, $destination, $client_id) = @_;
	my $elem = delete $self->claimed->{$client_id}->{$destination};
	return unless $elem;

	my $message = $elem->val;
	$message->disown();
	$self->unclaimed->{$destination}->add($elem);
	$self->messages->{$message->id}->{unclaimed} = $elem;
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
