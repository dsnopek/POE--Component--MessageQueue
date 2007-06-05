
package POE::Component::MessageQueue::Storage::Throttled;
use base qw(POE::Component::MessageQueue::Storage);

use POE;
use strict;

use Data::Dumper;

sub new
{
	my $class = shift;
	my $args  = shift;

	my $storage;
	my $throttle_max = 2;

	if ( ref($args) eq 'HASH' )
	{
		$storage      = $args->{storage};
		$throttle_max = $args->{throttle_max} if exists $args->{throttle_max};
	}
	else
	{
		$storage = $args;
	}

	my $self = $class->SUPER::new( $args );

	$self->{storage} = $storage;

	# for throttling data to the engine
	$self->{throttle_buffer} = { };
	$self->{throttle_order}  = [ ];
	$self->{throttle_max}    = $throttle_max;
	$self->{throttle_count}  = 0;

	# we have to intercept the message_stored handler.
	$self->{storage}->set_message_stored_handler(sub { return $self->_message_stored(@_); });

	bless  $self, $class;
	return $self;
}

# set_message_stored_handler() -- We maintain the parents version.

sub set_dispatch_message_handler
{
	my ($self, $handler) = @_;
	# We never need to call this directly, storage will!
	#$self->SUPER::set_dispatch_message_handler( $handler );
	$self->{storage}->set_dispatch_message_handler( $handler );
}

sub set_destination_ready_handler
{
	my ($self, $handler) = @_;
	# We never need to call this directly, storage will!
	#$self->SUPER::set_destination_ready_handler( $handler );
	$self->{storage}->set_destination_ready_handler( $handler );
}

sub set_logger
{
	my ($self, $logger) = @_;
	$self->SUPER::set_logger( $logger );
	$self->{storage}->set_logger( $logger );
}

sub _throttle_push
{
	my ($self, $message) = @_;

	# stash in an ordered-lookup kind of way
	$self->{throttle_buffer}->{$message->{message_id}} = $message;
	push @{$self->{throttle_order}}, $message->{message_id};
}

sub _throttle_pop
{
	my ($self) = @_;

	while ( scalar @{$self->{throttle_order}} > 0 )
	{
		my $message_id = shift @{$self->{throttle_order}};

		# if there is still a message with that id in the buffer
		# then return it.
		if ( exists $self->{throttle_buffer}->{$message_id} )
		{
			return delete $self->{throttle_buffer}->{$message_id};
		}
	}

	undef;
}

sub _throttle_remove
{
	my ($self, $message_id) = @_;

	if ( exists $self->{throttle_buffer}->{$message_id} )
	{
		delete $self->{throttle_buffer}->{$message_id};
		return 1;
	}

	return 0;
}

sub _message_stored
{
	my ($self, $destination) = @_;

	if ( defined $self->{message_stored} )
	{
		$self->{message_stored}->( $destination );
	}

	if ( $self->{throttle_max} )
	{
		my $message = $self->_throttle_pop();
		if ( $message )
		{
			# if we have a throttled message then send it!
			$self->_log("STORE: Sending throttled message from the buffer to the storage engine");
			$self->{storage}->store($message);
		}
		else
		{
			# else, simple decrease the throttle count
			$self->{throttle_count} --;
		}
	}
}

sub get_next_message_id
{
	return shift->{storage}->get_next_message_id();
}

sub store
{
	my ($self, $message) = @_;

	if ( $self->{throttle_max} )
	{
		if ( $self->{throttle_count} >= $self->{throttle_max} )
		{
			$self->_log("STORE: Already have sent $self->{throttle_max} messages to store engine.  Throttling.  Message will be buffered until engine has stored some messages.");

			# push into buffer
			$self->_throttle_push($message);
			
			# don't send, yet!
			return;
		}
		else
		{
			# increment so we know that another message was sent to the 
			# underlying engine.
			$self->{throttle_count} ++;
		}
	}

	$self->{storage}->store($message);
}

sub remove
{
	my ($self, $message_id) = @_;

	if ( $self->{throttle_max} )
	{
		# if we put this message in the throttle buffer, then remove
		# it before it can even get to the storage engine
		if ( $self->_throttle_remove( $message_id ) )
		{
			return;
		}
	}

	$self->{storage}->remove($message_id);
}

sub claim_and_retrieve
{
	return shift->{storage}->claim_and_retrieve(@_);
}

sub disown
{
	return shift->{storage}->disown(@_);
}

1;

