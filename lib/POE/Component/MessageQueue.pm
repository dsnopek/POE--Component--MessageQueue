
package POE::Component::MessageQueue;

use POE 0.38;
use POE::Component::Server::Stomp;
use POE::Component::MessageQueue::Client;
use POE::Component::MessageQueue::Queue;
use POE::Component::MessageQueue::Message;
use Net::Stomp;
use vars qw($VERSION);
use strict;

$VERSION = '0.1.3';

use Carp qw(croak);
use Data::Dumper;

sub new
{
	my $class = shift;
	my $args  = shift;

	my $alias;
	my $address;
	my $hostname;
	my $port;
	my $domain;

	my $storage;
	my $logger_alias;

	if ( ref($args) eq 'HASH' )
	{
		$alias    = $args->{alias};
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
		Alias    => $alias,
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

	$self->_log( 'notice', "MASTER: Removing client $client_id" );
	
	my $client = $self->get_client( $client_id );

	# remove subscriptions to all queues
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

	# shutdown TCP connection
	$client->shutdown();
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
		$self->_log( 'notice', "No message in backstore on $destination for $client_id" );

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
		$self->_log( 'notice',
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
		$self->_log( 'notice', sprintf("RECV (%i): DISCONNECT", $client->{client_id}) );

		# disconnect, yo!
		$self->remove_client( $client->{client_id} );
	}
	elsif ( $frame->command eq 'SEND' )
	{
		my $destination = $frame->headers->{destination};
		my $persistent  = $frame->headers->{persistent} eq 'true';

		$self->_log( 'notice',
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

		$self->_log( 'notice',
			sprintf ("RECV (%i): SUBSCRIBE %s (ack: %s)",
				$client->{client_id},
				$destination,
				$ack_type)
		);

		if ( $destination =~ /^\/queue\/(.*)$/ )
		{
			my $queue_name = $1;
			my $queue = $self->get_queue( $queue_name );

			$self->_log( 'notice', "MASTER: Subscribing client $client->{client_id} to $queue_name" );

			$queue->add_subscription( $client, $ack_type );
		}
	}
	elsif ( $frame->command eq 'UNSUBSCRIBE' )
	{
		my $destination = $frame->headers->{destination};

		$self->_log( 'notice',
			sprintf ("RECV (%i): UNSUBSCRIBE %s\n",
				$client->{client_id},
				$destination)
		);

		if ( $destination =~ /^\/queue\/(.*)$/ )
		{
			my $queue_name = $1;
			my $queue = $self->get_queue( $queue_name );

			$self->_log( 'notice', "MASTER: UN-subscribing client $client->{client_id} from $queue_name" );

			$queue->remove_subscription( $client );
		}
	}
	elsif ( $frame->command eq 'ACK' )
	{
		my $message_id = $frame->headers->{'message-id'};

		$self->_log( 'notice',
			sprintf ("RECV (%i): ACK - message %i",
				$client->{client_id},
				$message_id)
		);

		$self->ack_message( $client, $message_id );
	}
	else
	{
		$self->_log( 'error', "ERROR: Don't know how to handle frame: " . $frame->as_string );
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

	$self->_log( 'notice', "MASTER: message $message->{message_id} needs ACK from client $client->{client_id}" );
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
	my $sub = $queue->get_subscription( $client );
	if ( defined $sub )
	{
		# Must check if subscriber is still connected before setting!
		$sub->set_done_with_message();
	}

	# pump the queue, so that this subscriber will get another message
	$queue->pump();
}

1;

__END__

=pod

=head1 NAME

POE::Component::MessageQueue - A POE message queue that uses STOMP for the communication protocol

=head1 SYNOPSIS

  use POE;
  use POE::Component::Logger;
  use POE::Component::MessageQueue;
  use POE::Component::MessageQueue::Storage::Complex;
  use strict;

  my $DATA_DIR = '/tmp/perl_mq';

  # we create a logger, because a production message queue would
  # really need one.
  POE::Component::Logger->spawn(
    ConfigFile => 'log.conf',
    Alias      => 'mq_logger'
  );

  POE::Component::MessageQueue->new({
    port     => 61613,            # Optional.
    address  => '127.0.0.1',      # Optional.
    hostname => 'localhost',      # Optional.
    domain   => AF_INET,          # Optional.
    
    logger_alias => 'mq_logger',  # Optional.

    # Required!!
    storage => POE::Component::MessageQueue::Storage::Complex->new({
      data_dir     => $DATA_DIR,
      timeout      => 2,
	  throttle_max => 2
    })
  });

  POE::Kernel->run();
  exit;

=head1 DESCRIPTION

This module implements a message queue [1] on top of L<POE> that communicates
via the STOMP protocol [2].

There exist a few good Open Source message queues, most notably ActiveMQ [3] which
is written in Java.  It provides more features and flexibility than this one (while
still implementing the STOMP protocol), however, it was (at the time I last used it)
very unstable.  With every version there was a different mix of memory leaks, persistence
problems, STOMP bugs, and file descriptor leaks.  Due to its complexity I was
unable to be very helpful in fixing any of these problems, so I wrote this module!

This component distinguishes itself in a number of ways:

=over 4

=item *

No OS threads, its asynchronous.  (Thanks to L<POE>!)

=item *

Persistence was a high priority.

=item *

A strong effort is put to low memory and high performance.

=item *

Message storage can be provided by a number of different backends.

=back

=head1 STORAGE

When creating an instance of this component you must pass in a storage object
so that the message queue knows how to store its messages.  There are some storage
backends provided with this distribution.  See their individual documentation for 
usage information.  Here is a quick break down:

=over 4

=item *

L<POE::Component::MessageQueue::Storage::Memory> -- The simplest storage engine.  It keeps messages in memory and provides absolutely no presistence.

=item *

L<POE::Component::MessageQueue::Storage::DBI> -- Uses Perl L<DBI> to store messages.  Not recommended to use directly because message body doesn't belong in the database.  All messages are stored persistently.  (Underneath this is really just L<POE::Component::MessageQueue::Storage::Generic> and L<POE::Component::MessageQueue::Storage::Generic::DBI>)

=item *

L<POE::Component::MessageQueue::Storage::EasyDBI> -- An alternative L<DBI> storage engine based on L<POE::Component::EasyDBI>.  Its use is B<not recommend> because of inexplicable memory problems.  This will remain in the distribution for the time being but will probably be removed in the future.  All messages are stored persistently.

=item *

L<POE::Component::MessageQueue::Storage::FileSystem> -- Wraps around another storage engine to store the message bodies on the filesystem.  This can be used in conjunction with the DBI storage engine so that message properties are stored in DBI, but the message bodies are stored on disk.  All messages are stored persistently regardless of whether a message has the persistent flag set or not.

=item *

L<POE::Component::MessageQueue::Storage::Generic> -- Uses L<POE::Component::Generic> to wrap storage modules that aren't asynchronous.  Using this module is the easiest way to write custom storage engines.

=item *

L<POE::Component::MessageQueue::Storage::Generic::DBI> -- A synchronise L<DBI>-based storage engine that can be used in side of Generic.  This provides the basis for the L<POE::Component::MessageQueue::Storage::DBI> module.

=item *

L<POE::Component::MessageQueue::Storage::Throttled> -- Wraps around another engine to limit the number of messages sent to be stored at once.  Use of this module is B<highly> recommend!  If the storage engine is unable to store the messages fast enough (ie. with slow disk IO) it can get really backed up and stall messages coming out of the queue, allowing execessive producers to basically monopolise the server, preventing any messages from getting distributed to subscribers.

=item *

L<POE::Component::MessageQueue::Storage::Complex> -- A combination of the Memory, FileSystem, DBI and Throttled modules above.  It will keep messages in Memory and move them into FileSystem after a given number of seconds, throttling messages passed into DBI.  The DBI backend is configured to use SQLite.  It is capable of correctly handling a messages persistent flag.  This is the recommended storage engine and should provide the best performance in the most common case (ie. when both providers and consumers are connected to the queue at the same time).

=back

=head1 CONSTRUCTOR PARAMETERS

=over 2

=item storage => SCALAR

The only required parameter.  Sets the object that the message queue should use for
message storage.  This must be an object that follows the interface of
L<POE::Component::MessageQueue::Storage> but doesn't necessarily need to be a child
of that class.

=item alias => SCALAR

The session alias to use.

=item port => SCALAR

The optional port to listen on.  If none is given, we use 61613 by default.

=item address => SCALAR

The option interface address to bind to.  It defaults to INADDR_ANY or INADDR6_ANY
when using IPv4 or IPv6, respectively.

=item hostname => SCALAR

The optional name of the interface to bind to.  This will be converted to the IP and
used as if you set I<address> instead.  If you set both I<hostname> and I<address>,
I<address> will override this value.

=item domain => SCALAR

Optionally specifies the domain within which communication will take place.  Defaults
to AF_INET.

=item logger_alias => SCALAR

Opitionally set the alias of the POE::Component::Logger object that you want the message
queue to log to.  If no value is given, log information is simply printed to STDERR.

=back

=head1 REFERENCES

=over 4

=item [1]

L<http://en.wikipedia.org/Message_Queue> -- General information about message queues

=item [2]

L<http://stomp.codehaus.org/Protocol> -- The informal "spec" for the STOMP protocol

=item [3]

L<http://www.activemq.org/> -- ActiveMQ is a popular Java-based message queue

=back

=head1 FUTURE

The goal of this module is not to support every possible feature but rather to be
small, simple, efficient and robust.  So, for the most part expect only incremental
changes to address those areas.  Other than that, here are some things I would like
to implement someday in the future:

=over 4

=item *

Full support for the STOMP protocol.

=item *

Topics a la "topic://" in ActiveMQ.

=item *

Some kind of security based on username/password.

=item *

Optional add on module via L<POE::Component::IKC::Server> that allows to introspect the state of the message queue.

=back

=head1 SEE ALSO

I<External modules:>

L<POE>, L<POE::Component::Server::Stomp>, L<Net::Stomp>, L<POE::Component::Logger>, L<DBD::SQLite>,
L<POE::Component::Generic>

I<Internal modules:>

L<POE::Component::MessageQueue::Storage>,
L<POE::Component::MessageQueue::Storage::Memory>,
L<POE::Component::MessageQueue::Storage::DBI>,
L<POE::Component::MessageQueue::Storage::FileSystem>,
L<POE::Component::MessageQueue::Storage::Generic>,
L<POE::Component::MessageQueue::Storage::Generic::DBI>,
L<POE::Component::MessageQueue::Storage::Throttled>,
L<POE::Component::MessageQueue::Storage::Complex>

=head1 BUGS

A ton of debugging work went into the 0.1.3 release.

I recieved a script from a user that would cause a memory leak in 0.1.2.  By switching to
L<POE::Component::Generic> for the L<POE::Component::MessageQueue::Storage::DBI> module,
I was able to eliminate this memory leak.

However!  Our message queue in production still appears to steadily increase its memory 
usage.  This could be another memory leak but it is also possible that its just memory
fragmentation or the load being so high that the number of throttled messages is getting
out of control.

I am unable to recreate this in testing, making it difficult to debug.  It only turns up
under our production load.  If anyone else experiences this problem and can recreate in an
reliable way (preferably with something automated like a script), I<let me know>!

That said, we are using this in production in a commercial application for
thousands of large messages daily and it takes quite awhile to get unreasonably bloated.
Despite its problems, in the true spirit of Open Source and Free Software, I've decided
to "release early -- release often."

=head1 AUTHOR

Copyright 2007 David Snopek <dsnopek@gmail.com>

=cut

