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

package POE::Component::MessageQueue;
use vars qw($VERSION);
$VERSION = '0.1.8';

use POE 0.38;
use POE::Component::Server::Stomp;
use POE::Component::MessageQueue::Client;
use POE::Component::MessageQueue::Queue;
use POE::Component::MessageQueue::Topic;
use POE::Component::MessageQueue::Message;
use POE::Component::MessageQueue::IDGenerator::UUID;
use Net::Stomp;
use Event::Notify;
use Moose;

use constant SHUTDOWN_SIGNALS => ('TERM', 'HUP', 'INT');


has 'alias' => (
	is      => 'ro',
	default => 'MQ',
);

sub master_alias { $_[0]->alias.'-master' }

has 'logger'       => (
	is      => 'ro',
	lazy    => 1,
	default => sub {
		my $self = shift;
		POE::Component::MessageQueue::Logger->new(
			logger_alias => $self->logger_alias
		);
	},
	handles => [qw(log)],
);

has 'notifier' => (
	is => 'ro',
	default => sub { Event::Notify->new() },
	handles => [qw(notify register_event unregister_event)],
);

has 'idgen' => (
	is => 'ro',
	default => sub { POE::Component::MessageQueue::IDGenerator::UUID->new() },
	handles => { generate_id => 'generate' },
);

use constant optional => (is => 'ro');
has 'observers'    => optional;
has 'logger_alias' => optional;
has 'address'      => optional;
has 'domain'       => optional;

use constant required => (is => 'ro', required => 1);
has 'hostname' => required;
has 'port'     => required;
has 'storage'  => required;

use constant empty_hashref => (is => 'ro', default => sub { {} });
has 'clients'   => empty_hashref;
has 'queues'    => empty_hashref;
has 'topics'    => empty_hashref;
has 'needs_ack' => empty_hashref;


sub BUILD
{
	my ($self, $args) = @_;

	my $observers = $self->observers;
	if ($observers) 
	{
		$_->register($self) for (@$observers);
	}

	$self->storage->set_logger($self->logger);

	POE::Component::Server::Stomp->new(
		Alias    => $self->alias,
		Address  => $self->address,
		Hostname => $self->hostname,
		Port     => $self->port,
		Domain   => $self->domain,

		HandleFrame        => $self->__closure('_handle_frame'),
		ClientDisconnected => $self->__closure('_client_disconnected'),
		ClientError        => $self->__closure('_client_error'),
	);

	# a custom session for non-STOMP responsive tasks
	$self->{session} = POE::Session->create(
		object_states => [ $self => [qw(_start _shutdown)] ],
	);
}

sub _start
{
	my ($self, $kernel) = @_[OBJECT, KERNEL ];
	$kernel->alias_set($self->master_alias);

	# install signal handlers to initiate graceful shutdown.
	# We only respond to user-type signals - crash signals like 
	# SEGV and BUS should behave normally
	foreach my $signal ( SHUTDOWN_SIGNALS )
	{
		$kernel->sig($signal => '_shutdown'); 
	}
}

sub __closure
{
	my ($self, $method_name) = @_;
	my $func = sub {
		return $self->$method_name(@_);
	};
	return $func;
}

sub _auto_vivify
{
	my ($self, $hashname, $key, $constructor) = @_;
	
	unless (exists $self->{$hashname}->{$key})
	{
		$self->{$hashname}->{$key} = $constructor->();
	}
	return $self->{$hashname}->{$key};
}

sub get_client
{
	my ($self, $client_id) = @_;

	return $self->_auto_vivify(clients => "$client_id" => sub {
		POE::Component::MessageQueue::Client->new(id => $client_id);
	});
}

sub get_topic
{
	my ($self, $name) = @_;

	return $self->_auto_vivify(topics => $name => sub {
		POE::Component::MessageQueue::Topic->new(
			parent => $self,
			name   => $name,
		);
	});
}

sub get_queue
{
	my ($self, $name) = @_;

	return $self->_auto_vivify(queues => $name => sub {
		POE::Component::MessageQueue::Queue->new(
			parent => $self,
			name   => $name,
		);
	});
}

sub get_place
{
	my ($self, $name) = @_;

	my $queue_name = _destination_to_queue($name);
	return $self->get_queue($queue_name) if $queue_name;

	my $topic_name = _destination_to_topic($name);
	return $self->get_topic($topic_name) if $topic_name;

	return;	
}

sub remove_client
{
	my ($self, $client_id) = @_; 

	$self->log( 'notice', "MASTER: Removing client $client_id" );
	
	my $client = $self->get_client($client_id);

	$client->unsubscribe_all();
	delete $self->clients->{$client_id};

	# remove all references from needs_ack.
	while ( my ($id, $message) = each %{$self->needs_ack} )
	{
		if ( $message->claimant == $client )
		{
			delete $self->needs_ack->{$id};
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
		$self->log( 'error', "$name $number $message" );
	}
}

sub _destination_to_queue 
{
	$_ = shift;
	return unless m{/queue/(.*)};
	return $1;
}

sub _destination_to_topic
{
	$_ = shift;
	return unless m{/topic/(.*)};
	return $1;
}

sub _shutdown_complete
{
	my ($self) = @_;

	$self->log('alert', 'Storage engine has finished shutting down');

	# Really, really take us down!
	$self->log('alert', 'Sending TERM signal to master sessions');
	$poe_kernel->signal( $self->alias, 'TERM' );
	$poe_kernel->signal( $self->master_alias, 'TERM' );

	# shutdown the logger
	$self->log('alert', 'Shutting down the logger');
	$self->logger->shutdown();

	# Shutdown anyone watching us
	my $oref = $self->observers;
	if ($oref)
	{
		$_->shutdown() for (@$oref);
	}
}

sub route_frame
{
	my ($self, $client, $frame) = @_;
	my $cid = $client->id;
	my $destination = $frame->headers->{destination};

	my %handlers = (
		CONNECT => sub {
			my $login = $frame->headers->{login} || q{};
			my $passcode = $frame->headers->{passcode} || q{};

			$self->log('notice', "RECV ($cid): CONNECT $login:$passcode");
			$client->connect($login, $passcode);
		},

		DISCONNECT => sub {
			$self->log( 'notice', "RECV ($cid): DISCONNECT");
			$self->remove_client($cid);
		},

		SEND => sub {
			my $persistent  = $frame->headers->{persistent} eq 'true' ? 1 : 0;

			$self->log('notice',
				sprintf ("RECV (%s): SEND message (%i bytes) to %s (persistent: %i)",
					$cid, length $frame->body, $destination, $persistent));

			my $message = POE::Component::MessageQueue::Message->new(
				id          => $self->generate_id(),
				destination => $destination,
				persistent  => $persistent,
				body        => $frame->body,
			);

			my $place = $self->get_place($destination);

			$self->notify( 'recv', {
				place   => $place,
				message => $message,
				client  => $client,
			});
			
			$place->send($message);
		},

		SUBSCRIBE => sub {
			my $ack_type = $frame->headers->{ack} || 'auto';

			$self->log('notice',
				"RECV ($cid): SUBSCRIBE $destination (ack: $ack_type)");

			my $place = $self->get_place($destination);

			$client->subscribe($place, $ack_type);
			$self->notify(subscribe => {place => $place, client => $client});
			$place->pump() if $place->can('pump');
		},

		UNSUBSCRIBE => sub {
			$self->log('notice', "RECV ($cid): UNSUBSCRIBE $destination");
			my $place = $self->get_place($destination);
			$client->unsubscribe($place);
			$self->storage->disown($destination, $client) if $place->is_persistent;
			$self->notify(unsubscribe => {place => $place, client => $client});
		},

		ACK => sub {
			my $message_id = $frame->headers->{'message-id'};
			$self->log('notice', "RECV ($cid): ACK - message $message_id");
			$self->ack_message($client, $message_id);
		},
	);

	if (my $fn = $handlers{$frame->command})
	{
		$fn->();
		# Send receipt on anything but a connect
		if ($frame->command ne 'CONNECT' && 
				$frame->headers && 
				(my $receipt = $frame->headers->{receipt}))
		{
			$client->send_frame(Net::Stomp::Frame->new({
				command => 'RECEIPT',
				headers => {receipt => $receipt},
			}));
		}
	}
	else
	{
		$self->log('error', 
			"ERROR: Don't know how to handle frame: " . $frame->as_string);
	}
}

sub push_unacked_message
{
	my ($self, $message) = @_;

	$self->needs_ack->{$message->id} = $message;

	$self->log('notice', sprintf('MASTER: message %s needs ACK from client %s',
		$message->id, $message->claimant));
}

sub pop_unacked_message
{
	my ($self, $message_id, $client) = @_;

	my $unacked = $self->needs_ack->{$message_id};
	my $client_id = $client->id;
	my $claimant_id = $unacked->claimant;

	if ( $client_id != $claimant_id )
	{
		$self->log( 'alert', 
			"DANGER! Someone is trying to ACK a message that isn't theirs" );
		$self->log( 'alert', "message id: $message_id" );;
		$self->log( 'alert', "needs_ack says $claimant_id" );
		$self->log( 'alert', "but we got a message from $client_id" );
		return undef;
	}
	else
	{
		# remove from our needs ack list
		delete $self->needs_ack->{$message_id};
	}

	return $unacked;
}

sub ack_message
{
	my ($self, $client, $message_id) = @_;

	my $unacked = $self->pop_unacked_message( $message_id, $client );

	unless ($unacked)
	{
		$self->log( 'alert', "Error ACK'ing message: $message_id" );
		return;
	}
	
	my $queue = $self->get_queue(_destination_to_queue($unacked->destination));
	# ACK the subscriber back into ready mode.
	my $sub = $queue->subscriptions->{$client->id};
	$sub->ready(1) if $sub;

	# pump the queue, so that this subscriber will get another message
	$queue->pump();

	# notify stats
	$self->notify(ack => {
		place  => $queue,
		client => $client,
		message_info => {
			message_id => $unacked->id,
			timestamp  => $unacked->timestamp,
			size       => $unacked->size,
		}
	});

	# remove from the backing store
	$self->storage->remove([$message_id]);
}

sub _shutdown 
{
	my ($self, $kernel, $signal) = @_[ OBJECT, KERNEL, ARG0 ];
	$self->log('alert', "Got SIG$signal. Shutting down.");
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
			$self->log('emergency', $msg);
			print STDERR "$msg\n";
			$poe_kernel->stop();
		}
		return;
	}
	$self->{shutdown} = 1;

	$self->log('alert', 'Initiating message queue shutdown...');

	# stop listening for connections
	$poe_kernel->post( $self->alias => 'shutdown' );

	# shutdown all client connections
	my @client_ids = keys %{$self->clients};
	foreach my $client_id ( @client_ids )
	{
		$poe_kernel->post( $client_id => 'shutdown' );
	}

	$self->log('alert', 'Shutting down individual queues...');
	$_->shutdown() foreach (values %{$self->{queues}});

	# shutdown the storage
	$self->{storage}->storage_shutdown($self->__closure('_shutdown_complete'));
}

sub dispatch_message
{
	my ($self, $message, $subscriber) = @_;
	my $client = $subscriber->client;
	my $place = $self->get_place($message->destination);
	my $is_queue = $place->isa('POE::Component::MessageQueue::Queue');

	if($client && $client->send_frame($message->create_stomp_frame()))
	{
		$self->log('info', sprintf('Sending message %s to client %s', 
			$message->id, $client->id));

		if ($is_queue)
		{
			if ( $subscriber->ack_type eq 'client' )
			{
				$subscriber->ready(0);
				$self->push_unacked_message($message);
			}
			else
			{
				$self->storage->remove($message->id);
			}
			$place->pump();
		}

		$self->notify(dispatch => {
			place   => $place, 
			message => $message, 
			client  => $client,
		});
	}
	else
	{
		$self->log('warning', sprintf(
			"QUEUE: Message %s intended for %s on %s could not be delivered", 
			$message->id, $message->claimant, $message->destination,
		));

		# The message *NEEDS* to be disowned in the storage layer, otherwise
		# it will live forever as being claimed by a client that doesn't exist.
		if ($is_queue)
		{
			$self->storage->disown($message->destination, $message->claimant);
			$place->pump();
		}
	}
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
  use POE::Component::MessageQueue::Storage::Default;
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
    storage => POE::Component::MessageQueue::Storage::Default->new({
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

  POE::Component::MessageQueue version 0.1.8
  Copyright 2007, 2008 David Snopek (http://www.hackyourlife.org)
  Copyright 2007, 2008 Paul Driver <frodwith@gmail.com>
  Copyright 2007 Daisuke Maki <daisuke@endeworks.jp>
  
  mq.pl [--port|-p <num>] [--hostname|-h <host>]
        [--front-store <str>] [--nouuids]
        [--timeout|-i <seconds>]   [--throttle|-T <count>]
        [--data-dir <path_to_dir>] [--log-conf <path_to_file>]
        [--stats] [--stats-interval|-i <seconds>]
        [--background|-b] [--pidfile|-p <path_to_file>]
        [--crash-cmd <path_to_script>]
        [--debug-shell] [--version|-v] [--help|-h]
  
  SERVER OPTIONS:
    --port     -p <num>     The port number to listen on (Default: 61613)
    --hostname -h <host>    The hostname of the interface to listen on 
                            (Default: localhost)
  
  STORAGE OPTIONS:
    --front-store -f <str>  Specify which in-memory storage engine to use for
                            the front-store (can be memory or bigmemory).
    --timeout  -i <secs>    The number of seconds to keep messages in the 
                            front-store (Default: 4)
    --[no]uuids             Use (or do not use) UUIDs instead of incrementing
                            integers for message IDs.  Default: uuids 
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
  
    --crash-cmd  <path>     The path to a script to call when crashing.
                            A stacktrace will be printed to the script's STDIN.
                            (ex. 'mail root@localhost')
  
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

L<POE::Component::MessageQueue::Storage::DBI> -- Uses Perl L<DBI> to store messages.  Depending on your database configuration, using directly may not be recommended because the message bodies are stored directly in the database.  Wrapping with L<POE::Component::MessageQueue::Storage::FileSystem> allows you to store the message bodies on disk.  All messages are stored persistently.  (Underneath this is really just L<POE::Component::MessageQueue::Storage::Generic> and L<POE::Component::MessageQueue::Storage::Generic::DBI>)

=item *

L<POE::Component::MessageQueue::Storage::FileSystem> -- Wraps around another storage engine to store the message bodies on the filesystem.  This can be used in conjunction with the DBI storage engine so that message properties are stored in DBI, but the message bodies are stored on disk.  All messages are stored persistently regardless of whether a message has the persistent flag set or not.

=item *

L<POE::Component::MessageQueue::Storage::Generic> -- Uses L<POE::Component::Generic> to wrap storage modules that aren't asynchronous.  Using this module is the easiest way to write custom storage engines.

=item *

L<POE::Component::MessageQueue::Storage::Generic::DBI> -- A synchronous L<DBI>-based storage engine that can be used in side of Generic.  This provides the basis for the L<POE::Component::MessageQueue::Storage::DBI> module.

=item *

L<POE::Component::MessageQueue::Storage::Throttled> -- Wraps around another engine to limit the number of messages sent to be stored at once.  Use of this module is B<highly> recommend!  If the storage engine is unable to store the messages fast enough (ie. with slow disk IO) it can get really backed up and stall messages coming out of the queue, allowing execessive producers to basically monopolise the server, preventing any messages from getting distributed to subscribers.  Also, it will significantly cuts down the number of open FDs when used with L<POE::Component::MessageQueue::Storage::FileSystem>.

=item *

L<POE::Component::MessageQueue::Storage::Complex> -- A configurable storage engine that keeps a front-store (something fast) and a back-store (something persistent), allowing you to specify a timeout and an action to be taken when messages in the front-store expire, by default, moving them into the back-store.  It is capable of correctly handling a messages persistent flag.  This optimization allows for the possibility of messages being handled before ever having to be persisted.

=item *

L<POE::Component::MessageQueue::Storage::Default> -- A combination of the Complex, Memory, FileSystem, DBI and Throttled modules above.  It will keep messages in Memory and move them into FileSystem after a given number of seconds, throttling messages passed into DBI.  The DBI backend is configured to use SQLite.  It is capable of correctly handling a messages persistent flag.  This is the recommended storage engine and should provide the best performance in the most common case (ie. when both providers and consumers are connected to the queue at the same time).

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

=head1 UPGRADING FROM OLDER VERSIONS

If you used any of the following storage engines with PoCo::MQ 0.1.7 or older:

=over 4

=item *

L<POE::Component::MessageQueue::Storage::DBI>

=back

The database format has changed.

B<Note:> When using L<POE::Component::MessageQueue::Storage::Default> (meaning mq.pl)
the database will be automatically updated in place, so you don't need to worry
about this.

Included in the distribution, is a schema/ directory with a few SQL scripts for 
upgrading:

=over

=item *

upgrade-0.1.7.sql -- Apply if you are upgrading from version 0.1.6 or older.

=item *

ugrade-0.1.8.sql -- Apply if your are upgrading from version 0.1.7 or after applying
the above update script.

=back

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

Some kind of security based on username/password.

=item *

Optional add on module via L<POE::Component::IKC::Server> that allows to introspect the state of the message queue.

=back

=head1 SEE ALSO

I<External modules:>

L<POE>, L<POE::Component::Server::Stomp>, L<Net::Stomp>, L<POE::Component::Logger>, L<DBD::SQLite>,
L<POE::Component::Generic>, L<POE::Filter::Stomp>

I<Storage modules:>

L<POE::Component::MessageQueue::Storage>,
L<POE::Component::MessageQueue::Storage::Memory>,
L<POE::Component::MessageQueue::Storage::BigMemory>,
L<POE::Component::MessageQueue::Storage::DBI>,
L<POE::Component::MessageQueue::Storage::FileSystem>,
L<POE::Component::MessageQueue::Storage::Generic>,
L<POE::Component::MessageQueue::Storage::Generic::DBI>,
L<POE::Component::MessageQueue::Storage::Throttled>,
L<POE::Component::MessageQueue::Storage::Complex>,
L<POE::Component::MessageQueue::Storage::Default>

I<Statistics modules:>

L<POE::Component::MessageQueue::Statistics>,
L<POE::Component::MessageQueue::Statistics::Publish>,
L<POE::Component::MessageQueue::Statistics::Publish::YAML>

=head1 BUGS

We are serious about squashing bugs!  Currently, there are no known bugs, but
some probably do exist.  If you find any, please let us know at the Google group.

That said, we are using this in production in a commercial application for
thousands of large messages daily and we experience very few issues.

=head1 AUTHORS

Copyright 2007, 2008 David Snopek (L<http://www.hackyourlife.org>)

Copyright 2007, 2008 Paul Driver <frodwith@gmail.com>

Copyright 2007 Daisuke Maki <daisuke@endeworks.jp>

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

