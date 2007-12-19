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

package POE::Component::MessageQueue::Storage::Complex;
use base qw(POE::Component::MessageQueue::Storage);

use POE;
use POE::Component::MessageQueue::Storage::Throttled;
use POE::Component::MessageQueue::Storage::DBI;
use POE::Component::MessageQueue::Storage::FileSystem;
use POE::Component::MessageQueue::Storage::Memory;
use DBI;
use strict;

use Data::Dumper;

my $DB_CREATE = << "EOF";
CREATE TABLE messages
(
	message_id  int primary key,
	destination varchar(255) not null,
	persistent  char(1) default 'Y' not null,
	in_use_by   int,
	body        text,
	timestamp   int,
	size        int
);

CREATE INDEX destination_index ON messages ( destination );
CREATE INDEX in_use_by_index   ON messages ( in_use_by );
CREATE INDEX timestamp_index   ON messages ( timestamp );
EOF

sub new
{
	my $class = shift;
	my $args  = shift;

	my $data_dir;
	my $timeout;

	# we default to 2 because I think its a good idea
	my $throttle_max = 2;

	if ( ref($args) eq 'HASH' )
	{
		$data_dir = $args->{data_dir};
		$timeout  = $args->{timeout} || 4;

		# only set if the user set to preserve default
		$throttle_max = $args->{throttle_max} if exists $args->{throttle_max};
	}

	# create the datadir
	if ( not -d $data_dir )
	{
		mkdir $data_dir || die "Couldn't make data dir '$data_dir': $!";
	}

	my $db_file     = "$data_dir/mq.db";
	my $db_dsn      = "DBI:SQLite:dbname=$db_file";
	my $db_username = "";
	my $db_password = "";

	# setup sqlite for backstore
	my $create_db = (not -f $db_file);
	my $dbh = DBI->connect($db_dsn, $db_username, $db_password, { RaiseError => 1 });
	if ( $create_db )
	{
		# create initial database
		$dbh->do( $DB_CREATE );
	}
	else
	{
		eval
		{
			$dbh->selectrow_array("SELECT timestamp, size FROM messages LIMIT 1");
		};
		if ( $@ )
		{
			print STDERR "WARNING: User has pre-0.1.7 database format.\n";
			print STDERR "WARNING: Performing in place upgrade.\n";
			$dbh->do("ALTER TABLE messages ADD COLUMN timestamp INT");
			$dbh->do("ALTER TABLE messages ADD COLUMN size      INT");
			$dbh->do("CREATE INDEX timestamp_index ON messages ( timestamp )");
		}
	}
	$dbh->disconnect();

	# our memory-based front store
	my $front_store = POE::Component::MessageQueue::Storage::Memory->new();

	# setup the DBI backing store
	my $back_store = POE::Component::MessageQueue::Storage::Throttled->new({
		storage => POE::Component::MessageQueue::Storage::FileSystem->new({
			info_storage => POE::Component::MessageQueue::Storage::DBI->new({
				dsn       => $db_dsn,
				username  => $db_username,
				password  => $db_password,
			}),
			data_dir  => $data_dir,
		}),
		throttle_max => $throttle_max,
	});

	# the delay is half of the given timeout
	my $delay = int($timeout / 2);

	my $self = $class->SUPER::new( $args );

	$self->{front_store} = $front_store;
	$self->{back_store}  = $back_store;
	$self->{data_dir}    = $data_dir;
	$self->{timeout}     = $timeout;
	$self->{delay}       = $delay;
	$self->{timestamps}  = { };
	$self->{shutdown}    = 0;

	# our session that does the timed message check-up.
	my $session = POE::Session->create(
		inline_states => {
			_start => sub {
				$_[KERNEL]->yield('_check_messages');
			},
		},
		object_states => [
			$self => [
				'_check_messages',
			]
		]
	);
	$self->{session} = $session;

	return $self;
}

sub set_message_stored_handler
{
	my ($self, $handler) = @_;

	$self->SUPER::set_message_stored_handler( $handler );

	$self->{front_store}->set_message_stored_handler( $handler );
	$self->{back_store}->set_message_stored_handler( $handler );
}

sub set_dispatch_message_handler
{
	my ($self, $handler) = @_;
	
	$self->SUPER::set_dispatch_message_handler( $handler );

	$self->{front_store}->set_dispatch_message_handler( $handler );
	$self->{back_store}->set_dispatch_message_handler( $handler );
}

sub set_destination_ready_handler
{
	my ($self, $handler) = @_;

	$self->SUPER::set_destination_ready_handler( $handler );

	$self->{front_store}->set_destination_ready_handler( $handler );
	$self->{back_store}->set_destination_ready_handler( $handler );
}

sub set_shutdown_complete_handler
{
	my ($self, $handler) = @_;
	$self->{back_store}->set_shutdown_complete_handler( $handler );
}

sub set_logger
{
	my ($self, $logger) = @_;

	$self->SUPER::set_logger( $logger );

	$self->{front_store}->set_logger( $logger );
	$self->{back_store}->set_logger( $logger );
}

sub get_next_message_id
{
	my $self = shift;
	return $self->{back_store}->get_next_message_id();
}

sub store
{
	my ($self, $message) = @_;

	$self->{front_store}->store( $message );

	# mark the timestamp that this message was added
	if ( $message->{persistent} )
	{
		# don't add if not persistent so that non-persistent messages are
		# never considered for adding to the backing store.
		$self->{timestamps}->{$message->{message_id}} = time();
	}
}

sub remove
{
	my ($self, $message_id) = @_;

	if ( $self->{front_store}->remove( $message_id ) )
	{
		$self->_log( "STORE: MEMORY: Removed $message_id from in-memory store" );
	}
	else
	{
		$self->{back_store}->remove( $message_id );
	}

	# remove the timestamp
	delete $self->{timestamps}->{$message_id};
}

sub claim_and_retrieve
{
	my $self = shift;

	# first, try the front store.
	if ( $self->{front_store}->claim_and_retrieve(@_) )
	{
		# A message was claimed!  We're cool.
		return 1;
	}
	else
	{
		# then try the back store.
		return $self->{back_store}->claim_and_retrieve(@_);
	}
}

# unmark all messages owned by this client
sub disown
{
	my ($self, $destination, $client_id) = @_;

	$self->{front_store}->disown( $destination, $client_id );
	$self->{back_store}->disown( $destination, $client_id );
}

# our periodic check to move messages into the backing store
sub _check_messages
{
	my ($self, $kernel) = @_[ OBJECT, KERNEL ];

	if ( $self->{shutdown} )
	{
		# don't do anything and get out of here!
		return;
	}

	$self->_log( 'debug', 'STORE: COMPLEX: Checking for outdated messages' );

	my $threshold = time() - $self->{timeout};
	my @outdated;

	# get a list of message_ids that should be moved based on the timestamp list
	while( my ($message_id, $timestamp) = each %{$self->{timestamps}} )
	{
		if ( $threshold >= $timestamp )
		{
			push @outdated, $message_id;
		}
	}

	# remove the outdated messages from the front store and send them to the back store
	if ( scalar @outdated > 0)
	{
		my $messages = $self->{front_store}->remove_multiple( \@outdated );
		foreach my $message ( @$messages )
		{
			$self->_log( "STORE: COMPLEX: Moving message $message->{message_id} into backing store" );

			# do it, to it!
			$self->{back_store}->store( $message );

			# get off the timestamp list so they aren't considered again
			delete $self->{timestamps}->{$message->{message_id}};
		}
	}

	# keep us alive
	$kernel->delay( '_check_messages', $self->{delay} );
}

sub shutdown
{
	my $self = shift;

	if ( $self->{shutdown} )
	{
		return;
	}
	$self->{shutdown} = 1;

	# shutdown our check messages session
	$poe_kernel->signal( $self->{session}, 'TERM' );

	$self->_log('alert', 'Forcing all messages from the front-store into the back-store...');
	foreach my $message ( @{$self->{front_store}->empty_all()} )
	{
		if ( $message->{persistent} )
		{
			$self->_log( "STORE: COMPLEX: Moving message $message->{message_id} into backing store" );
			$self->{back_store}->store($message);
		}
	}

	# call the front-stores shutdown, just in case.  This really shouldn't do anything.
	$self->{front_store}->shutdown();

	# this should finish the job
	$self->{back_store}->shutdown();
}

1;

__END__

=pod

=head1 NAME

POE::Component::MessageQueue::Storage::Complex -- A storage engine that keeps messages in memory but moves them into persistent storage after a given number of seconds.

=head1 SYNOPSIS

  use POE;
  use POE::Component::MessageQueue;
  use POE::Component::MessageQueue::Storage::Complex;
  use strict;

  my $DATA_DIR = '/tmp/perl_mq';

  POE::Component::MessageQueue->new({
    storage => POE::Component::MessageQueue::Storage::Complex->new({
      data_dir     => $DATA_DIR,
      timeout      => 4,
      throttle_max => 2
    })
  });

  POE::Kernel->run();
  exit;

=head1 DESCRIPTION

This storage engine combines all the other provided engine.  It uses
L<POE::Component::MessageQueue::Storage::Memory> as the "front-end storage" and 
L<POE::Component::MessageQueue::Storage::FileSystem> as the "back-end storage".  Message
are initially put into the front-end storage and will be moved into the backend
storage after a given number of seconds.

The L<POE::Component::MessageQueue::Storage::FileSystem> component used internally 
uses L<POE::Component::MessageQueue::Storage::DBI> with a L<DBD::SQLite> database.
It is also throttled via L<POE::Component::MessageQueue::Storage::Throttled>.

This is the recommended storage engine.  It should provide the best performance while (if
configured sanely) still providing a reasonable amount of persistence with little
risk of eating all your memory under high load.  This is also the only storage
backend to correctly honor the persistent flag and will only persist those messages
with it set.

=head1 CONSTRUCTOR PARAMETERS

=over 2

=item data_dir => SCALAR

The directory to store the SQLite database file and the message body's.

=item timeout => SCALAR

The number of seconds a message will remain in non-persistent storage.  Ie. After this many seconds if the message hasn't been removed, it will be put to persistent storage.

=item throttle_max => SCALAR

The max number of messages that can be sent to the DBI store at once.  This value is passed directly to the underlying L<POE::Component::MessageQueue::Storage::Throttled>.

=back

=head1 SEE ALSO

L<DBI>,
L<DBD::SQLite>,
L<POE::Component::MessageQueue>,
L<POE::Component::MessageQueue::Storage>,
L<POE::Component::MessageQueue::Storage::Memory>,
L<POE::Component::MessageQueue::Storage::FileSystem>,
L<POE::Component::MessageQueue::Storage::DBI>,
L<POE::Component::MessageQueue::Storage::Generic>,
L<POE::Component::MessageQueue::Storage::Generic::DBI>,
L<POE::Component::MessageQueue::Storage::Throttled>

=cut

