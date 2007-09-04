
package POE::Component::MessageQueue::Storage::Generic::DBI;
use base qw(POE::Component::MessageQueue::Storage);

use DBI;
use Exception::Class::DBI;
use Exception::Class::TryCatch;
use strict;

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
	else
	{
		$dsn      = $args;
		$username = shift;
		$password = shift;
		$options  = shift;
	}

	# force use of exceptions
	$options->{'HandleError'} = Exception::Class::DBI->handler,
	$options->{'PrintError'} = 0;
	$options->{'RaiseError'} = 0;

	my $dbh = DBI->connect($dsn, $username, $password, $options);

	# before going any further, clear some old state
	$dbh->do( "UPDATE messages SET in_use_by = NULL" );

	my $self = $class->SUPER::new( $args );
	$self->{dbh}        = $dbh;
	$self->{message_id} = undef;

	bless  $self, $class;
	return $self;
}

sub _get_max_id
{
	my $self = shift;

	my $res = $self->{dbh}->selectrow_arrayref( "SELECT MAX(message_id) FROM messages" );

	return $res->[0] || 0;
}

sub get_next_message_id
{
	my $self = shift;

	if ( not defined $self->{message_id} )
	{
		$self->{message_id} = $self->_get_max_id();
	}

	return ++$self->{message_id};
}

sub store
{
	my ($self, $message) = @_;

	my $SQL = "INSERT INTO messages (message_id, destination, body, persistent, in_use_by) VALUES ( ?, ?, ?, ?, ? )";

	try eval
	{
		my $stmt;
		$stmt = $self->{dbh}->prepare($SQL);
		$stmt->execute(
			$message->{message_id},
			$message->{destination},
			$message->{body},
			$message->{persistent},
			$message->{in_use_by}
		);
	};
	my $err = catch;

	if ( $err )
	{
		$self->_log('error', "STORE: DBI: Error storing $message->{message_id} in $message->{destination}: $err");
	}
	else
	{
		$self->_log("STORE: DBI: Message $message->{message_id} stored in $message->{destination}");
	}

	if ( defined $self->{message_stored} )
	{
		$self->{message_stored}->( $message->{destination} );
	}

	undef;
}

sub remove
{
	my ($self, $message_id) = @_;

	my $SQL = "DELETE FROM messages WHERE message_id = ?";

	try eval
	{
		my $stmt;
		$stmt = $self->{dbh}->prepare($SQL);
		$stmt->execute($message_id);
	};
	my $err = catch;

	if ( $err )
	{
		$self->_log("STORE: DBI: Error deleting message $message_id: $err");
	}
	else
	{
		$self->_log("STORE: DBI: Message $message_id deleted");
	}

	undef;
}

sub _retrieve
{
	my ($self, $destination) = @_;

	my $SQL = "SELECT * FROM messages WHERE destination = ? AND in_use_by IS NULL ORDER BY message_id ASC LIMIT 1";

	my $result;

	try eval
	{
		my $stmt;
		$stmt = $self->{dbh}->prepare($SQL);
		$stmt->execute($destination);
		$result = $stmt->fetchrow_hashref;
	};
	my $err = catch;

	if ( $err )
	{
		$self->_log("error", "STORE: DBI: $err");
	}
	elsif ( defined $result )
	{
		return POE::Component::MessageQueue::Message->new({
			message_id  => $result->{message_id},
			destination => $result->{destination},
			persistent  => $result->{persistent},
			body        => $result->{body},
			in_use_by   => $result->{in_use_by}
		});
	}

	undef;
}

sub _claim
{
	my ($self, $message) = @_;

	my $SQL = "UPDATE messages SET in_use_by = ? WHERE message_id = ?";

	try eval
	{
		my $stmt;
		$stmt = $self->{dbh}->prepare($SQL);
		$stmt->execute($message->{in_use_by}, $message->{message_id});
	};
	my $err = catch;

	if ( $err )
	{
		$self->_log("error", "STORE: DBI: Error claiming message $message->{message_id} for $message->{in_use_by}: $err");
	}
	else
	{
		$self->_log("STORE: DBI: Message $message->{message_id} claimed by $message->{in_use_by}");
	}

	undef;
}

sub claim_and_retrieve
{
	my $self = shift;
	my $args = shift;

	if ( not defined $self->{dispatch_message} )
	{
		die "Pulled message from backstore, but there is no dispatch_message handler";
	}

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

	# first, we retrieve a message
	my $message = $self->_retrieve( $destination );
	
	# if we actually got a message, then we need to claim it.
	if ( defined $message )
	{
		# set to the client_id that's funna get it
		$message->{in_use_by} = $client_id;
	}

	# send the message to the handler, regardless if we actually got
	# one or not.
	# NOTE: We can do this before claiming the message, so I figure, why
	# not do it since it will give the other thread something to do.
	$self->{dispatch_message}->( $message, $destination, $client_id );

	if ( defined $message )
	{
		# claim away!
		$self->_claim( $message );

		# after it is claimed, we declare the destination ready for 
		# more action!
		$self->{destination_ready}->( $destination );
	}

	undef;
}

sub disown
{
	my ($self, $destination, $client_id) = @_;

	my $SQL = "UPDATE messages SET in_use_by = NULL WHERE destination = ? AND in_use_by = ?";

	try eval
	{
		my $stmt;
		$stmt = $self->{dbh}->prepare($SQL);
		$stmt->execute($destination, $client_id);
	};
	my $err = catch;

	if ( $err )
	{
		$self->_log("error", "STORE: DBI: Error disowning all messages on $destination for $client_id: $err");
	}
	else
	{
		$self->_log("STORE: DBI: All messages on $destination disowned for client $client_id");
	}

	undef;
}

1;

__END__

=pod

=head1 NAME

POE::Component::MessageQueue::Storage::Generic::DBI -- A storage engine that uses L<DBI>

=head1 SYNOPSIS

  use POE;
  use POE::Component::MessageQueue;
  use POE::Component::MessageQueue::Storage::Generic;
  use POE::Component::MessageQueue::Storage::Generic::DBI;
  use strict;

  # For mysql:
  my $DB_DSN      = 'DBI:mysql:database=perl_mq';
  my $DB_USERNAME = 'perl_mq';
  my $DB_PASSWORD = 'perl_mq';
  my $DB_OPTIONS  = undef;

  POE::Component::MessageQueue->new({
    storage => POE::Component::MessageQueue::Storage::Generic->new({
      package => 'POE::Component::MessageQueue::Storage::DBI',
      options => [{
        dsn      => $DB_DSN,
        username => $DB_USERNAME,
        password => $DB_PASSWORD,
        options  => $DB_OPTIONS
      }],
    })
  });

  POE::Kernel->run();
  exit;

=head1 DESCRIPTION

A storage engine that uses L<DBI>.  All messages stored with this backend are
persistent.

This module is not itself asynchronous and must be run via 
L<POE::Component::MessageQueue::Storage::Generic> as shown above.

Rather than using this module "directly" [1], I would suggest wrapping it inside of
L<POE::Component::MessageQueue::Storage::FileSystem>, to keep the message bodys on
the filesystem, or L<POE::Component::MessageQueue::Storage::Complex>, which is the
overall recommended storage engine.

If you are only going to deal with very small messages then, possibly, you could 
safely keep the message body in the database.  However, this is still not really
recommended for a couple of reasons:

=over 4

=item *

All database access is conducted through L<POE::Component::Generic> which maintains
a single forked process to do database access.  So, not only must the message body be
communicated to this other proccess via a pipe, but only one database operation can
happen at once.  The best performance can be achieved by having this forked process
do as little as possible.

=item *

A number of databases have hard limits on the amount of data that can be stored in
a BLOB (namely, SQLite, which sets an artificially lower limit than it is actually
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

=head1 FOOTNOTES

=over 4

=item [1] 

By "directly", I still mean inside of L<POE::Component::MessageQueue::Storage::Generic> because
that is the only way to use this module.

=back

=head1 SEE ALSO

L<DBI>,
L<POE::Component::Generic>,
L<POE::Component::MessageQueue>,
L<POE::Component::MessageQueue::Storage>,
L<POE::Component::MessageQueue::Storage::Memory>,
L<POE::Component::MessageQueue::Storage::FileSystem>,
L<POE::Component::MessageQueue::Storage::DBI>,
L<POE::Component::MessageQueue::Storage::Generic>,
L<POE::Component::MessageQueue::Storage::Throttled>,
L<POE::Component::MessageQueue::Storage::Complex>

=cut

