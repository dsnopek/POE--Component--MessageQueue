#
# Copyright 2007, 2008 David Snopek <dsnopek@gmail.com>
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

package POE::Component::MessageQueue::Queue;

use POE;
use POE::Session;
use Moose;

with qw(POE::Component::MessageQueue::Destination);

use constant flag => (is => 'rw', default => 0);
has pumping => flag;
has pump_pending => flag;
has shutting_down => flag;

has ordered_subscriptions => (
	metaclass => 'Collection::Array',
	is => 'ro',
	isa => 'ArrayRef[POE::Component::MessageQueue::Subscription]',
	default => sub { [] },
	provides => {
		'push'   => 'add_ordered_subscription',
		'delete' => 'delete_ordered_subscription'
	},
);

has next_robin_index => (
	is  => 'rw',
	isa => 'Int',
	default => 0,
);

before set_subscription => sub
{
	my ($self, $id, $sub) = @_;

	if (not $self->has_subscription($id))
	{
		$self->add_ordered_subscription($sub);
	}
};

after delete_subscription => sub
{
	my ($self, $id) = @_;

	my $index = 0;
	foreach my $sub ( @{$self->ordered_subscriptions} )
	{
		last if ( $sub->client->id == $id )
	}

	if ($index < scalar @{$self->ordered_subscriptions})
	{
		$self->delete_ordered_subscription($index);
		if ($index < $self->next_robin_index)
		{
			$self->next_robin_index($self->next_robin_index - 1);
		}
	}
};

__PACKAGE__->meta->make_immutable();

sub BUILD
{
	my ($self, $args) = @_;
	POE::Session->create(
		object_states => [ $self => [qw(_start _shutdown _subloop)]],
	);
}

sub _start
{
	my ($self, $kernel) = @_[OBJECT, KERNEL];
	$kernel->alias_set($self->name);
}

sub shutdown { 
	my $self = $_[0];
	$self->shutting_down(1);
	$poe_kernel->post($self->name, '_shutdown') 
}

sub _shutdown
{
	my ($self, $kernel) = @_[OBJECT, KERNEL];
	$kernel->alias_remove($self->name);
}

# This is the pumping philosophy:  When we receive a pump request, we will
# give everyone a chance to claim a message.  If any pumps are asked for while
# this is happening, we will remember and do another pump when this one is
# finished (just one).

sub is_persistent { return 1 }

sub round_robin_subscriptions
{
	my ($self) = @_;

	my $next_index = $self->next_robin_index;
	my @subs = $self->all_subscriptions;

	if ($next_index == 0)
	{
		return @subs;
	}
	
	return ( @subs[$next_index .. $#subs], 
	         @subs[0           .. ($next_index - 1)] );
}

sub set_last_robin
{
	my ($self, $robin) = @_;

	my $index = 0;
	foreach my $sub ( @{$self->ordered_subscriptions} )
	{
		last if ($sub == $robin);
		$index++;
	}

	# increment one so that we are pointing at the next robin
	$index++;

	if ($index > scalar @{$self->ordered_subscriptions})
	{
		$index = 0;
	}

	$self->next_robin_index($index);
}

sub _subloop
{
	my ($self, $subs) = @_[OBJECT, ARG0];
	return if $self->shutting_down;
	my $s; while ($s = shift(@$subs)) { last if $s->ready };
	if($s && $s->client)
	{
		$s->ready(0);
		$self->storage->claim_and_retrieve($self->name, $s->client->id, sub {
			if (my $msg = $_[0])
			{
				$self->set_last_robin($s);
				$self->dispatch_message($msg, $s);
				$poe_kernel->post($self->name, _subloop => $subs);
			}
			else
			{
				$s->ready(1);
				$self->_done_pumping();
			}
		});
	}
	else
	{
		$self->_done_pumping();
	}
}

sub _done_pumping
{
	my $self = $_[0];
	$self->pumping(0);
	$self->pump() if $self->pump_pending;
}

sub pump
{
	my $self = $_[0];
	if($self->pumping)
	{
		$self->pump_pending(1);
	}
	else
	{
		$self->log(debug => ' -- PUMP QUEUE: '.$self->name.' -- ');
		$self->notify('pump');
		my @subs = $self->round_robin_subscriptions;
		$self->pump_pending(0);
		$self->pumping(1);
		$poe_kernel->call($self->name, '_subloop', \@subs);
	}
}

sub send
{
	my ($self, $message) = @_;
	return if $self->shutting_down;

	# If we already have a ready subscriber, we'll claim and dispatch before we
	# store to give the subscriber a headstart on processing.
	foreach my $s ($self->round_robin_subscriptions)
	{
		if ($s->ready)
		{
			$self->set_last_robin($s);
			my $cid = $s->client->id;
			$message->claim($cid);
			$self->log(info => 
				'QUEUE: Message '.$message->id." claimed by $cid during send");
			$self->dispatch_message($message, $s);
			last;
		}
	}

	$self->storage->store($message, sub {
		$self->notify(store => $message);
		$self->pump();
	});
}

1;

