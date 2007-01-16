
package POE::Component::MessageQueue::Queue;

use POE::Component::MessageQueue::Subscription;
use strict;

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
		has_pending_messages => 1
	};

	bless  $self, $class;
	return $self;
}

sub get_parent { return shift->{parent}; }

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

	# attempt to get a pending message and pass it the the 'send_queue' action.

	if ( $self->{has_pending_messages} )
	{
		my $sub = $self->get_available_subscriber();
		if ( $sub )
		{
			# makes sure that this subscription isn't double picked
			$sub->set_handling_message();

			# get a message out of the backing store
			$self->get_parent()->get_storage()->claim_and_retrieve({
				destination => "/queue/$self->{queue_name}",
				client_id   => $sub->{client}->{client_id}
			});
		}
	}
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

	print "QUEUE: Sending message $message->{message_id} to client $sub->{client}->{client_id}\n";

	# mark as needing ack, or remove message.
	if ( $sub->{ack_type} eq 'client' )
	{
		# Put into waiting for ACK mode.
		$sub->set_handling_message( $message );

		# add to the general list of messages requiring ACK
		$self->get_parent()->push_unacked_message( $message, $sub->get_client() );
	}
	else
	{
		$self->get_parent()->get_storage()->remove( $message->get_message_id() );
	}

	# send actual message
	$sub->get_client()->send_frame( $message->create_stomp_frame() );
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

