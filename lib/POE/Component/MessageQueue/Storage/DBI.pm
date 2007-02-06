
package POE::Component::MessageQueue::Storage::DBI;
use base qw(POE::Component::MessageQueue::Storage);

use POE::Kernel;
use POE::Session;
use POE::Component::EasyDBI;
use strict;

use Data::Dumper;

sub new
{
	my $class = shift;
	my $args  = shift;

	my $dsn;
	my $username;
	my $password;
	my $options;

	if ( ref($args) eq 'HASH' )
	{
		$dsn      = $args->{dsn};
		$username = $args->{username};
		$password = $args->{password};
		$options  = $args->{options};
	}

	my $self = $class->SUPER::new( $args );

	$self->{message_id} = 0;
	$self->{claiming}   = { };

	my $easydbi = POE::Component::EasyDBI->spawn(
		alias    => 'MQ-EasyDBI',
		dsn      => $dsn,
		username => $username,
		password => $password
	);

	my $session = POE::Session->create(
		inline_states => {
			_start => sub {
				$_[KERNEL]->alias_set('MQ-Storage-DBI')
			},
		},
		object_states => [
			$self => [
				'_init_message_id',
				'_easydbi_handler',
				'_message_to_store',
				'_message_from_store',
				'_store_claim_message',
			]
		]
	);

	# store the sessions
	$self->{easydbi} = $easydbi;
	$self->{session} = $session;

	# clear the state
	$poe_kernel->post( $self->{easydbi},
		do => {
			sql     => 'UPDATE messages SET in_use_by = NULL',
			event   => '_easydbi_handler',
			session => $self->{session}
		}
	);

	# get the initial message id
	$poe_kernel->post( $self->{easydbi},
		single => {
			sql     => 'SELECT MAX(message_id) FROM messages',
			event   => '_init_message_id',
			session => $self->{session}
		}
	);

	return $self;
}

sub get_next_message_id
{
	my $self = shift;
	return ++$self->{message_id};
}

sub store
{
	my ($self, $message) = @_;

	# push the message into our persistent store
	$poe_kernel->post( $self->{easydbi},
		insert => {
			table   => 'messages',
			hash    => { %$message },
			session => $self->{session},
			event   => '_message_to_store',

			# baggage:
			_message_id  => $message->{message_id},
			_destination => $message->{destination},
		}
	);
}

sub remove
{
	my ($self, $message_id) = @_;

	# remove the message from the backing store
	$poe_kernel->post( $self->{easydbi},
		do => {
			sql          => 'DELETE FROM messages WHERE message_id = ?',
			placeholders => [ $message_id ],
			session      => $self->{session},
			event        => '_easydbi_handler'
		}
	);
}

sub claim_and_retrieve
{
	my $self = shift;
	my $args = shift;

	my $destination;
	my $client_id;

	if ( ref($args) eq 'HASH' )
	{
		$destination = $args->{destination};
		$client_id   = $args->{client_id};
	}
	else
	{
		$destination = $args;
		$client_id   = shift;
	}

	if ( $self->{claiming}->{$destination} )
	{
		# we are already attempting to claim a message for this destination!
		return 0;
	}
	else
	{
		# lock temporarily.
		$self->{claiming}->{$destination} = $client_id;
	}

	$poe_kernel->post( $self->{easydbi},
		arrayhash => {
			sql          => 'SELECT * FROM messages WHERE destination = ? AND in_use_by IS NULL ORDER BY message_id ASC LIMIT 1',
			placeholders => [ $destination ],
			session      => $self->{session},
			event        => '_message_from_store',

			# baggage:
			_destination => $destination,
			_client_id   => $client_id
		}
	);

	# let the caller know that this is actually going down.
	return 1;
}

# unmark all messages owned by this client
sub disown
{
	my ($self, $destination, $client_id ) = @_;

	$poe_kernel->post( $self->{easydbi},
		do => {
			sql          => 'UPDATE messages SET in_use_by = NULL WHERE destination = ? AND in_use_by = ?',
			placeholders => [ $destination, $client_id ],
			session      => $self->{session},
			event        => '_easydbi_handler',
		}
	);
}

#
# For handling responses from database:
#

sub _init_message_id
{
	my ($self, $kernel, $value) = @_[ OBJECT, KERNEL, ARG0 ];

	$self->{message_id} = $value->{result} || 0;
}

sub _easydbi_handler
{
	my ($self, $kernel, $event) = @_[ OBJECT, KERNEL, ARG0 ];

	if ( $event->{action} eq 'do' )
	{
		my $pretty = join ', ', @{$event->{placeholders}};
		$self->_log( 'debug', "STORE: DBI: $event->{sql} [ $pretty ]" );
	}
}

sub _message_to_store
{
	my ($self, $kernel, $value) = @_[ OBJECT, KERNEL, ARG0 ];

	my $message_id  = $value->{_message_id};
	my $destination = $value->{_destination};

	$self->_log( "STORE: DBI: Added message $message_id to backing store" );

	if ( defined $self->{message_stored} )
	{
		$self->{message_stored}->( $destination );
	}
}

sub _message_from_store
{
	my ($self, $kernel, $value) = @_[ OBJECT, KERNEL, ARG0 ];

	my $rows        = $value->{result};
	my $destination = $value->{_destination};
	my $client_id   = $value->{_client_id};

	if ( not defined $self->{dispatch_message} )
	{
		die "Pulled message from backstore, but there is no dispatch_message handler";
	}

	my $message;

	if ( defined $rows and scalar @$rows == 1 )
	{
		my $result = $rows->[0];

		$message = POE::Component::MessageQueue::Message->new({
			message_id  => $result->{message_id},
			destination => $result->{destination},
			persistent  => $result->{persistent},
			body        => $result->{body},
			in_use_by   => $client_id
		});

		# claim this message
		$kernel->post( $self->{easydbi},
			do => {
				sql          => "UPDATE messages SET in_use_by = ? WHERE message_id = ?",
				placeholders => [ $client_id, $message->{message_id} ],
				session      => $self->{session},
				event        => '_store_claim_message',

				# backage:
				_destination => $destination,
				_client_id   => $client_id,
			}
		);

	}
	else
	{
		# unlock claiming from this destination
		delete $self->{claiming}->{$destination};
	}

	# call the handler because the message is complete
	$self->{dispatch_message}->( $message, $destination, $client_id );
}

sub _store_claim_message
{
	my ($self, $kernel, $value) = @_[ OBJECT, KERNEL, ARG0 ];

	my $result      = $value->{result};
	my $destination = $value->{_destination};
	my $client_id   = $value->{_client_id};

	# unlock claiming from this destination
	delete $self->{claiming}->{$destination};

	# notify whoaver, that the destination is ready for another client to try to claim
	# a message.
	if ( defined $self->{destination_ready} )
	{
		$self->{destination_ready}->( $destination );
	}
}

1;

__END__

=pod

=head1 NAME

POE::Component::MessageQueue::Storage::DBI -- A storage backend that uses Perl L<DBI>

=head1 SYNOPSIS

  use POE;
  use POE::Component::MessageQueue;
  use POE::Component::MessageQueue::Storage::DBI;
  use strict;

  # For mysql:
  my $DB_DSN      = 'DBI:mysql:database=perl_mq';
  my $DB_USERNAME = 'perl_mq';
  my $DB_PASSWORD = 'perl_mq';
  my $DB_OPTIONS  = undef;

  POE::Component::MessageQueue->new({
    storage => POE::Component::MessageQueue::Storage::DBI->new({
      dsn      => $DB_DSN,
      username => $DB_USERNAME,
      password => $DB_PASSWORD,
      options  => $DB_OPTIONS
    })
  });

  POE::Kernel->run();
  exit;

=head1 DESCRIPTION

A storage backend that uses DBI.  All messages stored with this backend are persistent.

Rather than using this module directly, I would suggest using
L<POE::Component::MessageQueue::Storage::FileSystem>, which uses this module for
synchronization but keeps the message body on the filesystem, or
L<POE::Component::MessageQueue::Storage::Complex>, which is the overall recommended
storage backend.

If you are only going to deal with very small messages then, possibly, you could 
safely keep the message body in the database.  However, this is still not really
recommended for a couple of reasons:

=over 4

=item *

All database access is conducted through L<POE::Component::EasyDBI> which maintains
a single forked process to do database access.  So, not only must the message body be
communicated to this other proccess via a pipe, but only one database operation can
happen at once.  The best performance can be achieved by having this forked process
do as little as possible.

=item *

L<POE::Component::EasyDBI> has a limit to the size of queries it will take and query
results it will return.

=item *

A number of database have hard limits on the amount of data that can be stored in
a BLOB (namely, SQLite2 which sets an artificially lower limit than it is actually
capable of).

=item *

Keeping large amounts of BLOB data in a database is bad form anyway!  Let the database do what
it does best: index and look-up information quickly.

=back

=head1 CONSTRUCTOR PARAMETERS

=over 2

=item dsn => SCALAR

=item username => SCALAR

=item password => SCALAR

=item options => SCALAR

=back

=head1 SEE ALSO

L<DBI>,
L<POE::Component::EasyDBI>,
L<POE::Component::MessageQueue>,
L<POE::Component::MessageQueue::Storage>,
L<POE::Component::MessageQueue::Storage::Memory>,
L<POE::Component::MessageQueue::Storage::FileSystem>,
L<POE::Component::MessageQueue::Storage::Complex>

=cut

