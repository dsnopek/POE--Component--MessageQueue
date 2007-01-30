
package POE::Component::MessageQueue;

use POE;
use POE::Component::Server::Stomp;
use POE::Component::MessageQueue::Client;
use POE::Component::MessageQueue::Queue;
use POE::Component::MessageQueue::Message;
use Net::Stomp;
use strict;

use Carp qw(croak);
use Data::Dumper;

sub new
{
	my $class = shift;
	my $args  = shift;

	my $address;
	my $hostname;
	my $port;
	my $domain;

	my $storage;
	my $logger_alias;

	if ( ref($args) eq 'HASH' )
	{
		$address  = $args->{address};
		$hostname = $args->{hostname};
		$port     = $args->{port};
		$domain   = $args->{domain};
		
		$storage      = $args->{storage};
		$logger_alias = $args->{logger_alias};
	}

	if ( not defined $storage )
	{
		# TODO: We could do some kind of default, like using SQLite or memory 
		# or something.  But for now, require that the storage engine be specified.
		croak "$class->new(): Must pass a storage object for the message queue to operate on."
	}

	# create our logger object
	my $logger = POE::Component::MessageQueue::Logger->new({ logger_alias => $logger_alias });

	my $self = {
		storage   => $storage,
		logger    => $logger,
		clients   => { },
		queues    => { },
		needs_ack => { },
	};
	bless $self, $class;

	# setup the storage callbacks
	$self->{storage}->set_message_stored_handler(  $self->__closure('_message_stored') );
	$self->{storage}->set_dispatch_message_handler(  $self->__closure('_dispatch_from_store') );
	$self->{storage}->set_destination_ready_handler( $self->__closure('_destination_store_ready') );

	# get the storage object using our logger
	$self->{storage}->set_logger( $self->{logger} );

	# setup our stomp server
	POE::Component::Server::Stomp->new(
		Address  => $address,
		Hostname => $hostname,
		Port     => $port,
		Domain   => $domain,

		HandleFrame        => $self->__closure('_handle_frame'),
		ClientDisconnected => $self->__closure('_client_disconnected'),
		ClientError        => $self->__closure('_client_error')
	);

	# TODO: We will probably need to setup a custom session for 'master' tasks.

	return $self;
}

sub get_storage { return shift->{storage}; }

sub __closure
{
	my ($self, $method_name) = @_;
	my $func = sub {
		return $self->$method_name(@_);
	};
	return $func;
}

sub _log
{
	my $self = shift;
	$self->{logger}->log(@_);
}

sub get_client
{
	my $self = shift;
	my $client_id = shift;

	if ( not defined $self->{clients}->{$client_id} )
	{
		$self->{clients}->{$client_id} = POE::Component::MessageQueue::Client->new( $client_id );
	}

	return $self->{clients}->{$client_id};
}

sub get_queue
{
	my $self = shift;
	my $queue_name = shift;

	if ( not defined $self->{queues}->{$queue_name} )
	{
		$self->{queues}->{$queue_name} = POE::Component::MessageQueue::Queue->new( $self, $queue_name );
	}

	return $self->{queues}->{$queue_name};
}

sub remove_client
{
	my $self      = shift;
	my $client_id = shift;

	$self->_log( "MASTER: Removing client $client_id" );
	
	my $client = $self->get_client( $client_id );

	foreach my $queue_name ( @{$client->{queue_names}} )
	{
		my $queue = $self->get_queue( $queue_name );
		$queue->remove_subscription( $client );
	}

	# remove from the client list
	delete $self->{clients}->{$client_id};

	# remove all references from needs_ack.
	while ( my ($key, $value) = each %{$self->{needs_ack}} )
	{
		if ( $value->{client} == $client )
		{
			delete $self->{needs_ack}->{$key};
		}
	}

	# mark all messages owned by this client to be owned by nobody.
	$self->{storage}->disown( $client_id );
}

sub _handle_frame
{
	my $self = shift;
	my ($kernel, $heap, $frame) = @_[ KERNEL, HEAP, ARG0 ];

	my $id = $kernel->get_active_session()->ID();
	my $client = $self->get_client( $id );

	$self->route_frame( $client, $frame );
}

sub _client_disconnected
{
	my $self = shift;
	my ($kernel, $heap) = @_[ KERNEL, HEAP ];

	my $id = $kernel->get_active_session()->ID();
	$self->remove_client( $id );
}

sub _client_error
{
	my $self = shift;
	my ($kernel, $name, $number, $message) = @_[ KERNEL, ARG0, ARG1, ARG2 ];

	if ( $name eq 'read' and $number == 0 )
	{
		# This is EOF, which is perfectly fine!
	}
	else
	{
		$self->_log( 'error', "$name $number $message" );
	}
}

sub _message_stored
{
	my ($self, $destination) = @_;
	
	my $queue;
	if ( $destination =~ /\/queue\/(.*)/ )
	{
		my $queue_name = $1;

		$queue = $self->get_queue( $queue_name );
	}

	# pump the queue for good luck!
	$queue->pump();
}

sub _dispatch_from_store
{
	my ($self, $message, $destination, $client_id) = @_;
	
	my $queue;
	if ( $destination =~ /\/queue\/(.*)/ )
	{
		my $queue_name = $1;

		$queue = $self->get_queue( $queue_name );
	}

	my $client = $self->get_client( $client_id );

	if ( defined $message )
	{
		#print "MESSAGE FROM STORE\n";
		#print Dumper $message;

		$queue->dispatch_message_to( $message, $client );
	}
	else
	{
		$self->_log( "No message in backstore on $destination for $client_id" );

		# We need to free up the subscription.
		my $sub = $queue->get_subscription($client);
		if ( defined $sub )
		{
			# We have to test if it exists, because the client could have
			# disconnected already.
			$sub->set_done_with_message();
		}
	}
}

sub _destination_store_ready
{
	my ($self, $destination) = @_;

	#print "Queue is ready: $destination\n";

	if ( $destination =~ /\/queue\/(.*)/ )
	{
		my $queue_name = $1;
		my $queue = $self->get_queue( $queue_name );

		$queue->pump();
	}
}

sub route_frame
{
	my $self = shift;
	my $args = shift;

	my $client;
	my $frame;

	if ( ref($args) eq 'HASH' )
	{
		$client = $args->{client};
		$frame  = $args->{frame};
	}
	else
	{
		$client = $args;
		$frame  = shift;
	}

	if ( $frame->command eq 'CONNECT' )
	{
		$self->_log(
			sprintf ("RECV (%i): CONNECT %s:%s",
				$client->{client_id},
				$frame->headers->{login},
				$frame->headers->{passcode})
		);
		
		# connect!
		$client->connect({
			login    => $frame->headers->{login},
			passcode => $frame->headers->{passcode}
		});
	}
	elsif ( $frame->command eq 'DISCONNECT' )
	{
		$self->_log( sprintf("RECV (%i): DISCONNECT", $client->{client_id}) );

		# disconnect, yo!
		$self->remove_client( $client->{client_id} );
	}
	elsif ( $frame->command eq 'SEND' )
	{
		my $destination = $frame->headers->{destination};
		my $persistent  = $frame->headers->{persistent} eq 'true';

		$self->_log( 
			sprintf ("RECV (%i): SEND message (%i bytes) to %s (persistent: %i)",
				$client->{client_id},
				length $frame->body,
				$destination,
				$persistent)
		);

		if ( $destination =~ /^\/queue\/(.*)$/ )
		{
			my $queue_name = $1;
			my $queue = $self->get_queue( $queue_name );

			my $message = $self->create_message({
				destination => $destination,
				persistent  => $persistent,
				body        => $frame->body,
				stored      => 0
			});

			$queue->enqueue( $message );
		}
		else
		{
			$self->_log( 'error', "Don't know how to handle destination: $destination" );
		}
	}
	elsif ( $frame->command eq 'SUBSCRIBE' )
	{
		my $destination = $frame->headers->{destination};
		my $ack_type    = $frame->headers->{ack} || 'auto';

		$self->_log(
			sprintf ("RECV (%i): SUBSCRIBE %s (ack: %s)",
				$client->{client_id},
				$destination,
				$ack_type)
		);

		if ( $destination =~ /^\/queue\/(.*)$/ )
		{
			my $queue_name = $1;
			my $queue = $self->get_queue( $queue_name );

			$self->_log( "MASTER: Subscribing client $client->{client_id} to $queue_name" );

			$queue->add_subscription( $client, $ack_type );
		}
	}
	elsif ( $frame->command eq 'UNSUBSCRIBE' )
	{
		my $destination = $frame->headers->{destination};

		$self->_log(
			sprintf ("RECV (%i): UNSUBSCRIBE %s\n",
				$client->{client_id},
				$destination)
		);

		if ( $destination =~ /^\/queue\/(.*)$/ )
		{
			my $queue_name = $1;
			my $queue = $self->get_queue( $queue_name );

			$self->_log( "MASTER: UN-subscribing client $client->{client_id} from $queue_name" );

			$queue->remove_subscription( $client );
		}
	}
	elsif ( $frame->command eq 'ACK' )
	{
		my $message_id = $frame->headers->{'message-id'};

		$self->_log(
			sprintf ("RECV (%i): ACK - message %i",
				$client->{client_id},
				$message_id)
		);

		$self->ack_message( $client, $message_id );
	}
	else
	{
		$self->_log( "ERROR: Don't know how to handle frame: " . $frame->as_string );
	}
}

sub create_message
{
	my $self = shift;
	my $message = POE::Component::MessageQueue::Message->new(@_);

	if ( not defined $message->{message_id} )
	{
		$message->{message_id} = $self->{storage}->get_next_message_id();
	}

	return $message;
}

sub push_unacked_message
{
	my ($self, $message, $client) = @_;

	my $unacked = {
		client     => $client,
		message_id => $message->{message_id},
		queue_name => $message->get_queue_name()
	};
	
	$self->{needs_ack}->{$message->{message_id}} = $unacked;

	$self->_log( "MASTER: message $message->{message_id} needs ACK from client $client->{client}" );
}

sub pop_unacked_message
{
	my ($self, $message_id, $client) = @_;

	my $unacked = $self->{needs_ack}->{$message_id};

	if ( $client != $unacked->{client} )
	{
		$self->_log( 'alert', "DANGER! Someone is trying to ACK a message that isn't theirs" );
		$self->_log( 'alert', "message id: $message_id" );;
		$self->_log( 'alert', "needs_ack says $unacked->{client}->{client_id}" );
		$self->_log( 'alert', "but we got a message from $client->{client_id}" );
		exit 1;
		return undef;
	}
	else
	{
		# remove from our needs ack list
		delete $self->{needs_ack}->{$message_id};
	}

	return $unacked;
}

sub ack_message
{
	my ($self, $client, $message_id) = @_;

	my $unacked = $self->pop_unacked_message( $message_id, $client );

	if ( not defined $unacked )
	{
		$self->_log( 'alert', "Attempting to ACK a message not in our needs_ack list" );
		return;
	}
	
	# remove from the backing store
	$self->get_storage()->remove( $message_id );

	my $queue = $self->get_queue( $unacked->{queue_name} );

	# ACK the subscriber back into ready mode.
	$queue->get_subscription( $client )->set_done_with_message();

	# pump the queue, so that this subscriber will get another message
	$queue->pump();
}

1;

__END__

=pod

=head1 NAME

POE::Component::MessageQueue - A POE message queue that uses STOMP for the communication protocol

=head1 SYNOPSIS

=head1 DESCRIPTION

=cut

