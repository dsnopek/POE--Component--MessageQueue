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
use strict;

use Data::Dumper;

sub new
{
	my $class = shift;
	my $args  = shift;

	my $parent;
	my $queue_name;

	if ( ref($args) eq 'HASH' )
	{
		$parent     = $args->{parent};
		$queue_name = $args->{queue_name};
	}
	else
	{
		$parent     = $args;
		$queue_name = shift;
	}

	# TODO: deal with the circular reference

	my $self = {
		parent               => $parent,
		queue_name           => $queue_name,
		subscriptions        => [ ],
		sub_map              => { },
	};

	bless  $self, $class;
	return $self;
}

sub get_parent { return shift->{parent}; }

sub log
{
	my $self = shift;
	$self->get_parent()->log(@_);
}

sub add_subscription
{
	my $self   = shift;
	my $client = shift;
	my $ack_type = shift;
	my $sub    = POE::Component::MessageQueue::Subscription->new( $client, $ack_type );
	push @{$self->{subscriptions}}, $sub;

	# add the subscription to the sub_map.
	$self->{sub_map}->{$sub->{client}->{client_id}} = $sub;
	
	# add to the client's list of subscriptions
	$sub->{client}->_add_queue_name( $self->{queue_name} );

	$self->get_parent->{notify}->notify('subscribe', { queue => $self, client => $client });
	# pump the queue now that we have a new subscriber
	$self->pump();
}

sub remove_subscription
{
	my $self   = shift;
	my $client = shift;

	my $i;
	my $max = scalar @{$self->{subscriptions}};

	for( $i = 0; $i < $max; $i++ )
	{
		if ( $self->{subscriptions}->[$i]->{client} == $client )
		{
			# remove from the map
			delete $self->{sub_map}->{$client->{client_id}};

			# remove from the queue list
			splice @{$self->{subscriptions}}, $i, 1;
			$client->_remove_queue_name( $self->{queue_name} );

			# disown all messages on the storage layer for this client on
			# this queue.
			$self->get_parent()->get_storage()->disown(
				$self->destination(), $client->{client_id});

			$self->get_parent->{notify}->notify('unsubscribe', { queue => $self, client => $client });
			return;
		}
	}
}

sub destination
{
	my $self = shift;
	return "/queue/$self->{queue_name}";
}

sub get_subscription
{
	my ($self, $client) = @_;

	return $self->{sub_map}->{$client->{client_id}};
}

sub get_available_subscriber
{
	my $self = shift;

	my $count = @{$self->{subscriptions}};

	for(my $i = 0; $i < $count; $i++)
	{
		if ( $self->{subscriptions}->[$i]->is_ready() )
		{
			my $sub = $self->{subscriptions}->[$i];
			# Here we remove the chosen one from the subscription list and 
			# push it on the end, so that we favor other subscribers.
			splice @{$self->{subscriptions}}, $i, 1;
			push   @{$self->{subscriptions}}, $sub;
			return $sub;
		}
	}
	return;
}

sub has_available_subscribers
{
	my $self = shift;

	foreach my $sub ( @{$self->{subscriptions}} )
	{
		if ( $sub->is_ready() )
		{
			return 1;
		}
	}

	return 0;
}

sub pump
{
	my $self = shift;

	$self->log( 'debug', " -- PUMP QUEUE: $self->{queue_name} -- " );
	$self->get_parent->{notify}->notify('pump');

	foreach my $sub (@{$self->{subscriptions}})
	{
		next unless $sub->is_ready();
		$sub->set_handling_message();

		my $done_claiming = sub {
			if (my $message = shift)
			{
				$self->dispatch_message_to($message, $sub);
			}
			else
			{
				$sub->set_done_with_message();
			}
		};

		$self->get_parent()->get_storage()->claim_and_retrieve(
			$self->destination(), $sub->{client}->{client_id}, $done_claiming);
	}
}

sub dispatch_message_to
{
	my ($self, $message, $subscriber) = @_;
	my $client = $subscriber->get_client();
	my $client_id = $client->{client_id};

	$self->log('info', sprintf('QUEUE: Sending message %s to client %s', 
		$message->id, $client_id));

	# send actual message
	if($client->send_frame($message->create_stomp_frame()))
	{
		if ( $subscriber->{ack_type} eq 'client' )
		{
			$subscriber->set_handling_message();
			$self->get_parent()->push_unacked_message($message);
		}
		else
		{
			$self->get_parent()->get_storage()->remove($message->id);
		}

		# We've dispatched the message for sure: THIS IS WHERE THIS GOES.
		$self->get_parent()->{notify}->notify('dispatch', {
			queue   => $self, 
			message => $message, 
			client  => $client,
		});
	}
	else # We failed to send the frame: client is no longer available.
	{
		my $dest = $self->destination();

		$self->log('warning', sprintf(
			"QUEUE: Message %s intended for %s on %s could not be delivered", 
			$message->id, $client_id, $dest,
		));

		# The message *NEEDS* to be disowned in the storage layer, otherwise
		# it will live forever as being claimed by a client that doesn't exist.
		$self->get_parent()->get_storage()->disown($dest, $client_id);
	}

	# This is a good time to pump the queue, since we either succeeded in
	# sending out a message or just disowned one.
	$self->pump();
}

sub enqueue
{
	my ($self, $message) = @_;

	# If we already have a ready subscriber, we'll claim and dispatch before we
	# store to give the subscriber a headstart on processing.
	if ( my $sub = $self->get_available_subscriber() )
	{
		my $client_id = $sub->{client}->{client_id};
		$message->claim($client_id);
		$self->log('info', sprintf(
			'QUEUE: Message %s claimed by client %s during enqueue',
			$message->id, $client_id,
		));

		$self->dispatch_message_to( $message, $sub );
	}

	# Store the message, pump when it it's done being stored.
	$self->get_parent()->get_storage()->store($message, sub {
		$self->pump();
	});
}

1;

