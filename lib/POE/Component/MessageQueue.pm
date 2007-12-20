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

package POE::Component::MessageQueue;

use POE 0.38;
use POE::Component::Server::Stomp;
use POE::Component::MessageQueue::Client;
use POE::Component::MessageQueue::Queue;
use POE::Component::MessageQueue::Message;
use Net::Stomp;
use Event::Notify;
use vars qw($VERSION);
use strict;

$VERSION = '0.1.7';

use Carp qw(croak);
use Data::Dumper;

use constant SHUTDOWN_SIGNALS => ('TERM', 'HUP', 'INT');

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
	my $observers;

	if ( ref($args) eq 'HASH' )
	{
		$alias    = $args->{alias};
		$address  = $args->{address};
		$hostname = $args->{hostname};
		$port     = $args->{port};
		$domain   = $args->{domain};
		
		$storage      = $args->{storage};
		$logger_alias = $args->{logger_alias};
		$observers    = $args->{observers};
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
		notify    => Event::Notify->new(),
		observers => $observers,
	};
	bless $self, $class;

	if ($observers) {
		# Register the observers
		$_->register($self) for (@$observers);
	}

	# setup the storage callbacks
	$self->{storage}->set_message_stored_handler(  $self->__closure('_message_stored') );
	$self->{storage}->set_dispatch_message_handler(  $self->__closure('_dispatch_from_store') );
	$self->{storage}->set_destination_ready_handler( $self->__closure('_destination_store_ready') );
	$self->{storage}->set_shutdown_complete_handler( $self->__closure('_shutdown_complete') );

	# get the storage object using our logger
	$self->{storage}->set_logger( $self->{logger} );

	# to name the session for master tasks
	if ( not defined $alias )
	{
		$alias = "MQ";
	}

	# setup our stomp server
	POE::Component::Server::Stomp->new(
		Alias    => $alias,
		Address  => $address,
		Hostname => $hostname,
		Port     => $port,
		Domain   => $domain,

		HandleFrame        => $self->__closure('_handle_frame'),
		ClientDisconnected => $self->__closure('_client_disconnected'),
		ClientError        => $self->__closure('_client_error'),

		ObjectStates => [
			$self => [ '_pump' ]
		],
	);

	# a custom session for non-STOMP responsive tasks
	$self->{session} = POE::Session->create(
		inline_states => {
			_start => sub { 
				my $kernel = $_[ KERNEL ];
				$kernel->alias_set("$alias-master");
				# install signal handlers to initiate graceful shutdown.
				# We only respond to user-type signals - crash signals like 
				# SEGV and BUS should behave normally
				foreach my $signal ( SHUTDOWN_SIGNALS )
				{
					$kernel->sig($signal => '_shutdown'); 
				}
			},
		},
		object_states => [
			$self => [ '_pump', '_shutdown' ]
		],
	);

	# stash our session aliases for later
	$self->{server_alias} = $alias;
	$self->{master_alias} = "$alias-master";

	return $self;
}

sub register_event { shift->{notify}->register_event(@_) }
sub unregister_event { shift->{notify}->unregister_event(@_) }

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
	my @queue_names = $client->get_subscribed_queue_names();
	foreach my $queue_name ( @queue_names )
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
	my ($self, $message) = @_;
	
	my $destination = $message->{destination};
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

		$self->{notify}->notify( 'dispatch', {
			queue => $queue,
			message => $message,
			client => $client
		});
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

sub _shutdown_complete
{
	my ($self) = @_;

	$self->_log('alert', 'Storage engine has finished shutting down');

	# Really, really take us down!
	$self->_log('alert', 'Sending TERM signal to master sessions');
	$poe_kernel->signal( $self->{server_alias}, 'TERM' );
	$poe_kernel->signal( $self->{master_alias}, 'TERM' );

	# shutdown the logger
	$self->_log('alert', 'Shutting down the logger');
	$self->{logger}->shutdown();

	# Shutdown anyone watching us
	my $oref = $self->{observers};
	if ($oref)
	{
		$_->shutdown() for (@$oref);
	}
}

sub _pump
{
	my ($self, $kernel, $destination) = @_[ OBJECT, KERNEL, ARG0 ];

	if ( $destination =~ /\/queue\/(.*)/ )
	{
		my $queue_name = $1;
		my $queue = $self->get_queue( $queue_name );
		$queue->pump();
	}
}

sub pump_deferred
{
	my $self = shift;
	my $args = shift;

	my $destination;

	if ( ref($args) eq 'HASH' )
	{
		$destination = $args->{destination};
	}
	else
	{
		$destination = $args;
	}

	$poe_kernel->post( $self->{session}, '_pump', $destination );
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

			$self->{notify}->notify( 'recv', {
				message => $message,
				queue   => $queue,
				client  => $client,
			});

			$queue->enqueue( $message );

			$self->{notify}->notify( 'store', { queue => $queue, message => $message } );
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

	if ($frame->command ne 'CONNECT' && $frame->headers && (my $receipt = $frame->headers->{receipt}))
	{
		$client->send_frame( Net::Stomp::Frame->new( {
			command => 'RECEIPT',
			headers => {
				receipt => $receipt
			}
		} ) );
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
		queue_name => $message->get_queue_name(),
		timestamp  => $message->{timestamp},
		size       => $message->{size}
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
		$self->_log( 'alert', "Error ACK'ing message: $message_id" );
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

	$self->{notify}->notify('ack', {
		queue => $queue,
		client => $client,
		message_info => {
			message_id => $unacked->{message_id},
			timestamp  => $unacked->{timestamp},
			size       => $unacked->{size},
		}
	});

	# pump the queue, so that this subscriber will get another message
	$queue->pump();
}

sub _shutdown 
{
	my ($self, $kernel, $signal) = @_[ OBJECT, KERNEL, ARG0 ];
	$self->_log('alert', "Got SIG$signal. Shutting down.");
	$kernel->sig_handled();
	$self->shutdown(); 
}

sub shutdown
{
	my $self = shift;

	if ( $self->{shutdown} )
	{
		$self->{shutdown}++;
		if ( $self->{shutdown} >= 3 )
		{
			# TODO: Probably this isn't the right thing to do, but right now, during
			# development, this is necessary because the graceful shutdown doesn't work
			# at all.
			my $msg = "Shutdown called $self->{shutdown} times!  Forcing ungraceful quit.";
			$self->_log('emergency', $msg);
			print STDERR "$msg\n";
			$poe_kernel->stop();
		}
		return;
	}
	$self->{shutdown} = 1;

	$self->_log('alert', 'Initiating message queue shutdown...');

	# stop listening for connections
	$poe_kernel->post( $self->{server_alias} => 'shutdown' );

	# shutdown all client connections
	my @client_ids = keys %{$self->{clients}};
	foreach my $client_id ( @client_ids )
	{
		$poe_kernel->post( $client_id => 'shutdown' );
	}

	# shutdown the storage
	$self->{storage}->shutdown();
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

=head1 COMMAND LINE

If you are only interested in running with the recommended storage backend and
some predetermined defaults, you can use the included command line script.

  user$ mq.pl --help
  POE::Component::MessageQueue version 0.1.7
  Copyright 2007 David Snopek

  mq.pl [--port|-p <num>] [--hostname|-h <host>]
        [--timeout|-i <seconds>]   [--throttle|-T <count>]
        [--data-dir <path_to_dir>] [--log-conf <path_to_file>]
        [--stats] [--stats-interval|-i <seconds>]
        [--background|-b] [--pidfile|-p <path_to_file>]
        [--debug-shell] [--version|-v] [--help|-h]

  SERVER OPTIONS:
    --port     -p <num>     The port number to listen on (Default: 61613)
    --hostname -h <host>    The hostname of the interface to listen on 
                            (Default: localhost)

  STORAGE OPTIONS:
    --timeout  -i <secs>    The number of seconds to keep messages in the 
                            front-store (Default: 4)
    --throttle -T <count>   The number of messages that can be stored at once 
                            before throttling (Default: 2)
    --data-dir <path>       The path to the directory to store data 
                            (Default: /var/lib/perl_mq)
    --log-conf <path>       The path to the log configuration file 
                            (Default: /etc/perl_mq/log.conf

  STATISTICS OPTIONS:
    --stats                 If specified the, statistics information will be 
                            written to $DATA_DIR/stats.yml
    --stats-interval <secs> Specifies the number of seconds to wait before 
                            dumping statistics (Default: 10)

  DAEMON OPTIONS:
    --background -b         If specified the script will daemonize and run in the
                            background
    --pidfile    -p <path>  The path to a file to store the PID of the process

  OTHER OPTIONS:
    --debug-shell           Run with POE::Component::DebugShell
    --version    -v         Show the current version.
    --help       -h         Show this usage message

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

L<POE::Component::MessageQueue::Storage::FileSystem> -- Wraps around another storage engine to store the message bodies on the filesystem.  This can be used in conjunction with the DBI storage engine so that message properties are stored in DBI, but the message bodies are stored on disk.  All messages are stored persistently regardless of whether a message has the persistent flag set or not.

=item *

L<POE::Component::MessageQueue::Storage::Generic> -- Uses L<POE::Component::Generic> to wrap storage modules that aren't asynchronous.  Using this module is the easiest way to write custom storage engines.

=item *

L<POE::Component::MessageQueue::Storage::Generic::DBI> -- A synchronous L<DBI>-based storage engine that can be used in side of Generic.  This provides the basis for the L<POE::Component::MessageQueue::Storage::DBI> module.

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

=item observers => ARRAYREF

Optionally pass in a number of objects that will receive information about events inside
of the message queue.

Currently, only one observer is provided with the PoCo::MQ distribution:
L<POE::Component::MessageQueue::Statistics>.  Please see its documentation for more information.

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

=head1 UPGRADING FROM 0.1.6 OR OLDER

If you used any of the following storage engines with PoCo::MQ 0.1.6 or older:

=over 4

=item *

L<POE::Component::MessageQueue::Storage::DBI>

=back

The database format has changed.

B<Note:> When using L<POE::Component::MessageQueue::Storage::Complex> (meaning mq.pl)
the database will be automatically updated in place, so you don't need to worry
about this.

You will need to execute the following ALTER statements on your database to allow
PoCo::MQ to keep working:

  ALTER TABLE messages ADD COLUMN timestamp INT;
  ALTER TABLE messages ADD COLUMN size      INT;

=head1 CONTACT

Please check out the Google Group at:

L<http://groups.google.com/group/pocomq>

Or just send an e-mail to: pocomq@googlegroups.com

=head1 DEVELOPMENT

If you find any bugs, have feature requests, or wish to contribute, please
contact us at our Google Group mentioned above.  We'll do our best to help you
out!

Development is coordinated via Bazaar (See L<http://bazaar-vcs.org>).  The main
Bazaar branch can be found here:

L<http://code.hackyourlife.org/bzr/dsnopek/perl_mq>

We prefer that contributions come in the form of a published Bazaar branch with the
changes.  This helps facilitate the back-and-forth in the review process to get
any new code merged into the main branch.

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

I<Storage modules:>

L<POE::Component::MessageQueue::Storage>,
L<POE::Component::MessageQueue::Storage::Memory>,
L<POE::Component::MessageQueue::Storage::DBI>,
L<POE::Component::MessageQueue::Storage::FileSystem>,
L<POE::Component::MessageQueue::Storage::Generic>,
L<POE::Component::MessageQueue::Storage::Generic::DBI>,
L<POE::Component::MessageQueue::Storage::Throttled>,
L<POE::Component::MessageQueue::Storage::Complex>

I<Statistics modules:>

L<POE::Component::MessageQueue::Statistics>,
L<POE::Component::MessageQueue::Statistics::Publish>,
L<POE::Component::MessageQueue::Statistics::Publish::YAML>

=head1 BUGS

We are serious about squashing bugs!  Currently, there are no known bugs, but
some probably do exist.  If you find any, please let us know at the Google group.

That said, we are using this in production in a commercial application for
thousands of large messages daily and we experience very few issues.

=head1 AUTHOR

Copyright 2007 David Snopek (L<http://www.hackyourlife.org/>)

=head1 LICENSE

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.

=cut

