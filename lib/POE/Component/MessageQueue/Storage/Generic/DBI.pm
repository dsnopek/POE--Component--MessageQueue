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

package POE::Component::MessageQueue::Storage::Generic::DBI;
use Moose;

with qw(POE::Component::MessageQueue::Storage::Generic::Base);

use DBI;
use Exception::Class::DBI;
use Exception::Class::TryCatch;

has 'dsn' => (
	is       => 'ro',
	isa      => 'Str',
	required => 1,	
);

has 'username' => (
	is       => 'ro',
	isa      => 'Str',
	required => 1,	
);

has 'password' => (
	is       => 'ro',
	isa      => 'Str',
	required => 1,	
);

has 'options' => (
	is => 'ro',
	isa => 'HashRef',
	default => sub { {} },
	required => 1,
);

has 'dbh' => (
	is => 'ro',
	isa => 'Object',
	lazy => 1,
	default => sub {
		my $self = shift;
		DBI->connect($self->dsn, $self->username, $self->password, $self->options);
	},
);

make_immutable();

sub BUILD 
{
	my ($self, $args) = @_;
	# Force exception handling
  $self->options->{'HandleError'} = Exception::Class::DBI->handler,
  $self->options->{'PrintError'} = 0;
  $self->options->{'RaiseError'} = 0;

	# This actually makes DBH connect, and makes sure there's no claims left
	# over from the last time we shut down MQ.
	$self->dbh->do( "UPDATE messages SET in_use_by = NULL" );
}

sub _make_message { 
	my $h = shift;
	POE::Component::MessageQueue::Message->new(
		id          => $h->{message_id},
		destination => $h->{destination},
		body        => $h->{body},
		persistent  => $h->{persistent},
		claimant    => $h->{in_use_by},
		size        => $h->{size},
		timestamp   => $h->{timestamp},
	);
};

sub store
{
	my ($self, $message, $callback) = @_;

	my $SQL = "INSERT INTO messages (message_id, destination, body, persistent, in_use_by, timestamp, size) VALUES ( ?, ?, ?, ?, ?, ?, ? )";

	try eval
	{
		my $m = $message;
		my $stmt;
		$stmt = $self->{dbh}->prepare($SQL);
		$stmt->execute(
			$m->id,         $m->destination, $m->body, 
			$m->persistent, $m->claimant, 
			$m->timestamp,  $m->size,
		);
	};
	my $err = catch;

	if ( $err )
	{
		$self->log('error', sprintf("Error storing %s in %s: $err", 
			$message->id, $message->destination));
	}
	else
	{
		$self->log('info', sprintf('Message %s stored in %s', 
			$message->id, $message->destination));
	}

	# Call the callback, even if we just send it undef (that's the interface).
	$callback->($message) if $callback;

	return;
}

sub _remove_underneath
{
	my ($self, $get, $where, $errdesc) = @_;
	my @messages = ();
	try eval {
		if ($get)
		{
			my $sth = $self->dbh->prepare('SELECT * FROM messages'.$where); 
			$sth->execute();
	
			while(my $result = $sth->fetchrow_hashref())
			{
				push(@messages, _make_message($result));
			}
		}
		$self->dbh->do('DELETE FROM messages'.$where);
	};
	my $err = catch;
	$self->log('error', "Error $errdesc: $err") if ($err);

	return \@messages;
}

sub remove
{
	my ($self, $message_ids, $callback) = @_;
	my $ret = $self->_remove_underneath(
		$callback,
		' WHERE '. join(' OR ', map { "message_id = '$_'" } (@$message_ids)),
		'removing multiple messages',
	);
	$callback->($ret) if $callback;
	return;	
}

sub empty
{
	my ($self, $callback) = @_;
	my $ret = $self->_remove_underneath($callback, '', 'removing all messages');
	$callback->($ret) if $callback;
	return;
}

sub _retrieve
{
	my ($self, $destination) = @_;

	my $SQL = "SELECT * FROM messages WHERE destination = ? AND in_use_by IS NULL ORDER BY timestamp ASC LIMIT 1";

	my $result = undef;

	try eval
	{
		my $stmt;
		$stmt = $self->dbh->prepare($SQL);
		$stmt->execute($destination);
		$result = $stmt->fetchrow_hashref;
	};
	my $err = catch;
	$self->log("error", "$err") if $err;

	return $result && _make_message($result);
}

sub _claim
{
	my ($self, $message) = @_;

	my $SQL = "UPDATE messages SET in_use_by = ? WHERE message_id = ?";

	try eval
	{
		my $stmt;
		$stmt = $self->dbh->prepare($SQL);
		$stmt->execute($message->claimant, $message->id);
	};
	my $err = catch;

	if ( $err )
	{
		$self->log('error', sprintf("Error claiming message %s for client %s: $err",
			$message->id, $message->claimant));
	}
	else
	{
		$self->log('info', sprintf('Message %s claimed by %s', 
			$message->id, $message->claimant));
	}

	return;
}

sub claim_and_retrieve
{
	my ($self, $destination, $client_id, $dispatch) = @_;

	my $message = $self->_retrieve( $destination );
	
	# if we actually got a message, claim it
	$message->claim($client_id) if ($message);

	# Might as well do this now so the other thread can get on its way. :)
	$dispatch->($message, $destination, $client_id);

	# Write the claim info to database (if we got one)
	$self->_claim($message) if $message;

	return;
}

sub disown
{
	my ($self, $destination, $client_id) = @_;

	my $SQL = "UPDATE messages SET in_use_by = NULL WHERE destination = ? AND in_use_by = ?";

	try eval
	{
		my $stmt = $self->dbh->prepare($SQL);
		$stmt->execute($destination, $client_id);
	};
	my $err = catch;

	if ( $err )
	{
		$self->log('error', 
			"Error disowning all messages on $destination for $client_id: $err");
	}
	else
	{
		$self->log('info', 
			"All messages on $destination disowned for client $client_id");
	}

	return;
}

sub storage_shutdown
{
	my ($self, $complete) = @_;

	$self->log('alert', 'Shutting down DBI storage engine...');

	# close the database handle.
	$self->dbh->disconnect();

	# call the shutdown handler.
	$complete->();
	return;
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

