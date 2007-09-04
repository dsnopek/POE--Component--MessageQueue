
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
		has_pending_messages => 1,
		pumping              => 0
	};

	bless  $self, $class;
	return $self;
}

sub get_parent { return shift->{parent}; }

sub _log
{
	my $self = shift;
	$self->get_parent()->_log(@_);
}

sub add_subscription
{
	my $self = shift;
	my $sub  = POE::Component::MessageQueue::Subscription->new( @_ );
	push @{$self->{subscriptions}}, $sub;

	# add the subscription to the sub_map.
	$self->{sub_map}->{$sub->{client}->{client_id}} = $sub;
	
	# add to the client's list of subscriptions
	$sub->{client}->_add_queue_name( $self->{queue_name} );

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
				"/queue/$self->{queue_name}", $client->{client_id} );

			return;
		}
	}
}

sub get_subscription
{
	my ($self, $client) = @_;

	return $self->{sub_map}->{$client->{client_id}};
}

sub get_available_subscriber
{
	my $self = shift;
	my $args = shift;

	my $total = scalar @{$self->{subscriptions}};
	my $sub;
	my $i;

	for( $i = 0; $i < $total; $i++ )
	{
		if ( $self->{subscriptions}->[$i]->is_ready() )
		{
			$sub = $self->{subscriptions}->[$i];
			last;
		}
	}

	if ( $sub and $total != 1 )
	{
		# Here we remove the chosen one from the subscription list and 
		# push it on the end, so that we favor other subscribers.
		splice @{$self->{subscriptions}}, $i, 1;
		push   @{$self->{subscriptions}}, $sub;
	}

	return $sub;
}

sub has_pending_messages
{
	my ($self, $value) = @_;

	# TODO: doesn't work for some reason.
#	if ( $value == 0 )
#	{
#		$self->{has_pending_messages} = 0;
#	}

	return $self->{has_pending_messages};
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

	# Make sure we don't end up in some kind of recursive pump loop!
	if ( $self->{pumping} )
	{
		return;
	}
	$self->{pumping} = 1;
	
	$self->_log( 'debug', " -- PUMP QUEUE: $self->{queue_name} -- " );

	# attempt to get a pending message and pass it the the 'send_queue' action.
	if ( $self->{has_pending_messages} )
	{
		my $sub = $self->get_available_subscriber();
		if ( $sub )
		{
			# get a message out of the backing store
			my $ret = $self->get_parent()->get_storage()->claim_and_retrieve({
				destination => "/queue/$self->{queue_name}",
				client_id   => $sub->{client}->{client_id}
			});

			# if a message was actually claimed!
			if ( $ret and $sub->{ack_type} eq 'client' )
			{
				# makes sure that this subscription isn't double picked
				$sub->set_handling_message();
			}
		}
	}

	# end pumping lock!
	$self->{pumping} = 0;
}

sub send_message
{
	my ($self, $message) = @_;

	my $sub = $self->get_available_subscriber();
	if ( defined $sub )
	{
		$self->dispatch_message_to( $message, $sub );
	}
	else
	{
		# we have messages waiting in the store
		$self->{has_pending_messages} = 1;
	}
}

sub dispatch_message_to
{
	my ($self, $message, $receiver) = @_;

	my $sub;

	if ( ref($receiver) eq 'POE::Component::MessageQueue::Client' )
	{
		# automatically convert clients to subscribers!
		$sub = $self->get_subscription( $receiver );
	}
	else
	{
		$sub = $receiver;
	}

	my $result = 0;

	if ( defined $sub )
	{
		$self->_log( "QUEUE: Sending message $message->{message_id} to client $sub->{client}->{client_id}" );

		# send actual message
		$result = $sub->get_client()->send_frame( $message->create_stomp_frame() );
	}

	if ( not $result )
	{
		# This can happen when a client disconnects before the server
		# can give them the message intended for them.
		$self->_log( 'warning', "QUEUE: Message $message->{message_id} intended for $receiver->{client_id} on /queue/$self->{queue_name} could not be delivered" );

		# The message *NEEDS* to be disowned in the storage layer, otherwise
		# it will live forever as being claimed by a client that doesn't exist.
		$self->get_parent()->get_storage()->disown(
			"/queue/$self->{queue_name}", $receiver->{client_id} );

		# pump the queue to get the message to another suscriber.
		$self->pump();

		return;
	}

	# mark as needing ack, or remove message.
	if ( $sub->{ack_type} eq 'client' )
	{
		# Put into waiting for ACK mode.
		$sub->set_handling_message();

		# add to the general list of messages requiring ACK
		$self->get_parent()->push_unacked_message( $message, $sub->get_client() );
	}
	else
	{
		$self->get_parent()->get_storage()->remove( $message->get_message_id() );

		# we aren't waiting for anything, so pump the queue
		# NOTE: For some reason, using the deferred pump is so much more performant!
		$self->get_parent->pump_deferred("/queue/$self->{queue_name}");
	}
}

sub enqueue
{
	my ($self, $message) = @_;

	my $sub = $self->get_available_subscriber();

	# if we already have a subscriber in mind, be sure to set that right away.
	if ( defined $sub )
	{
		$message->set_in_use_by( $sub->{client}->{client_id} );
	}
	# store it as soon as possible!
	$self->get_parent()->get_storage()->store( $message );
	# actually send it to the subscriber that we had in mind
	if ( defined $sub )
	{
		$self->dispatch_message_to( $message, $sub );
	}
}

1;

