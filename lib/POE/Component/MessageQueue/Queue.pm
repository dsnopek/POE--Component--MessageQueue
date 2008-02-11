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

package POE::Component::MessageQueue::Queue;

use POE::Component::MessageQueue::Subscription;
use POE;
use POE::Session;
use Moose;

sub destination { return '/queue/'.$_[0]->name };
sub is_persistent { return 1 }

with qw(POE::Component::MessageQueue::Place);

has 'alias' => (
	is      => 'ro',
	lazy    => 1,
	default => sub {
		my $name = $_[0]->name;
		return "MQ-Queue-$name";
	},
);

has 'session' => (is => 'rw');

use constant flag => (
	is      => 'rw',
	default => 0,
);
has 'shutting_down' => flag;
has 'pumping' => flag;

make_immutable;

sub BUILD
{
	my $self = $_[0];
	$self->session(POE::Session->create(
		object_states => [$self => [qw(_start _pump _shutdown)] ],
	));
}

sub _start
{
	my ($self, $kernel) = @_[OBJECT, KERNEL];
	$kernel->alias_set($self->{alias});
}

sub shutdown { $poe_kernel->post($_[0]->session, '_shutdown') }
sub _shutdown
{
	my ($self, $kernel) = @_[OBJECT, KERNEL];
	$kernel->alias_remove($self->{alias});
	$self->shutting_down(1);
}

sub pump
{
	my $self = shift;

	# Ignore repeated calls until we have pumped once.
	return if $self->pumping;
	$self->pumping(1);

	$poe_kernel->post($self->{session}, '_pump');
}

sub _pump
{
	my ($self, $kernel) = @_[OBJECT, KERNEL];

	return if $self->shutting_down;
	$self->pumping(0);

	$self->log('debug', sprintf(" -- PUMP QUEUE: %s -- ", $self->name));
	$self->notify('pump');

	foreach my $subscriber (grep {$_->ready} (values %{$self->subscriptions}))
	{
		$subscriber->ready(0);
		$self->storage->claim_and_retrieve(
			$self->destination, 
			$subscriber->client->id, 
			sub {
				if(my $message = $_[0])
				{
					$self->dispatch_message($message, $subscriber);	
				}
				else
				{
					$subscriber->ready(1);
				}
			},
		);
	}

	# This overwrites old delays, so we only ever pump from this timer if we've
	# been idle longer than the delay.
	$kernel->delay('_pump', 1);
}

sub send
{
	my ($self, $message) = @_;

	# If we already have a ready subscriber, we'll claim and dispatch before we
	# store to give the subscriber a headstart on processing.
	foreach my $subscriber (values %{$self->subscriptions})
	{
		if ($subscriber->ready)
		{
			my $cid = $subscriber->client->id;
			my $mid = $message->id;
			$message->claim($cid);
			$self->log('info', 
				"QUEUE: Message $mid claimed by client $cid during enqueue");

			$self->dispatch_message($message, $subscriber);
			last;
		}
	}

	$self->storage->store($message, sub {$self->pump()});
	$self->notify('store', { place => $self, message => $message });
}

1;

