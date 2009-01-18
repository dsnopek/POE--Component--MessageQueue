#
# Copyright 2007, 2008, 2009 David Snopek <dsnopek@gmail.com>
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

sub _wrap
{
	my ($self, $name, $action) = @_;
	try eval {
		$action->();
	};
	if (my $err = catch)
	{
		$self->log(error => "Error in $name(): $err");
	}
	return;
}

sub _make_where
{
	my $ids = shift;
	return join(' OR ', map "message_id = '$_'", @$ids);
}

sub _wrap_ids
{
	my ($self, $ids, $name, $action) = @_;
	$self->_wrap(name => sub {$action->(_make_where($ids))}) if (@$ids > 0);
}

sub _make_message { 
	my $h = $_[0];
	my %map = (
		id          => 'message_id',
		destination => 'destination',
		body        => 'body',
		persistent  => 'persistent',
		claimant    => 'in_use_by',
		size        => 'size',
		timestamp   => 'timestamp',
		deliver_at  => 'deliver_at',
	);
	my %args;
	foreach my $field (keys %map) 
	{
		my $val = $h->{$map{$field}};
		$args{$field} = $val if (defined $val);
	}
	return POE::Component::MessageQueue::Message->new(%args);
};

# Note:  We explicitly set @_ in all the storage methods in this module,
# because when we do our tail-calls (goto $method), we don't want to pass them
# anything unneccessary, particulary $callbacks.

sub store
{
	my ($self, $m, $callback) = @_;

	$self->_wrap(store => sub {
		my $sth = $self->dbh->prepare(q{
			INSERT INTO messages (
				message_id, destination, body, 
				persistent, in_use_by,  
				timestamp,  size,
				deliver_at
			) VALUES (
				?, ?, ?, 
				?, ?, 
				?, ?,
				?
			)
		});
		$sth->execute(
			$m->id,         $m->destination, $m->body, 
			$m->persistent, $m->claimant, 
			$m->timestamp,  $m->size,
			$m->deliver_at
		);
	});

	@_ = ();
	goto $callback if $callback;
}

sub _get
{
	my ($self, $name, $clause, $callback) = @_;
	my @messages;
	$self->_wrap($name => sub {
		my $sth = $self->dbh->prepare("SELECT * FROM messages	$clause");
		$sth->execute;
		my $results = $sth->fetchall_arrayref({});
		@messages = map _make_message($_), @$results;
	});
	@_ = (\@messages);
	goto $callback;
}

sub _get_one
{
	my ($self, $name, $clause, $callback) = @_;
	$self->_get($name, $clause, sub {
		my $messages = $_[0];
		@_ = (@$messages > 0 ? $messages->[0] : undef);
		goto $callback;
	});
}

sub get
{
	my ($self, $message_ids, $callback) = @_;
	$self->_get(get => 'WHERE '._make_where($message_ids), $callback);
}

sub get_all
{
	my ($self, $callback) = @_;
	$self->_get(get_all => '', $callback);
}

sub get_oldest
{
	my ($self, $callback) = @_;
	$self->_get_one(get_oldest => 'ORDER BY timestamp ASC LIMIT 1', $callback);
}

sub claim_and_retrieve
{
	my ($self, $destination, $client_id, $callback) = @_;
	my $time = time();
	$self->_get_one(claim_and_retrieve => qq{
		WHERE destination = '$destination' AND in_use_by IS NULL AND
		      (deliver_at IS NULL OR deliver_at < $time)
		ORDER BY timestamp ASC LIMIT 1
	}, sub {
		if(my $message = $_[0])
		{
			$self->claim($message->id, $client_id)
		}
		goto $callback;
	});
}

sub remove
{
	my ($self, $message_ids, $callback) = @_;
	$self->_wrap_ids($message_ids, remove => sub {
		my $where = shift;
		$self->dbh->do("DELETE FROM messages WHERE $where");
	});
	@_ = ();
	goto $callback if $callback;
}

sub empty
{
	my ($self, $callback) = @_;
	$self->_wrap(empty => sub {$self->dbh->do("DELETE FROM messages")});
	@_ = ();
	goto $callback if $callback;
}

sub claim
{
	my ($self, $message_ids, $client_id, $callback) = @_;
	$self->_wrap_ids($message_ids, claim => sub {
		my $where = shift;
		$self->dbh->do(qq{
			UPDATE messages SET in_use_by = '$client_id' WHERE $where
		});
	});
	@_ = ();
	goto $callback if $callback;
}

sub disown_destination
{
	my ($self, $destination, $client_id, $callback) = @_;
	$self->_wrap(disown_destination => sub {
		$self->dbh->do(qq{
			UPDATE messages SET in_use_by = NULL WHERE in_use_by = '$client_id'
			AND destination = '$destination'
		});
	});
	@_ = ();
	goto $callback if $callback;
}

sub disown_all
{
	my ($self, $client_id, $callback) = @_;
	$self->_wrap(disown_all => sub {
		$self->dbh->do(qq{
			UPDATE messages SET in_use_by = NULL WHERE in_use_by = '$client_id'
		});
	});
	@_ = ();
	goto $callback if $callback;
}

sub storage_shutdown
{
	my ($self, $callback) = @_;

	$self->log(alert => 'Shutting down DBI storage engine...');

	$self->dbh->disconnect();
	@_ = ();
	goto $callback if $callback;
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

=head1 SUPPORTED STOMP HEADERS

=over 4

=item B<persistent>

I<Ignored>.  All messages are persisted.

=item B<expire-after>

I<Ignored>.  All messages are kept until handled.

=item B<deliver-after>

I<Fully Supported>.

=back

=head1 FOOTNOTES

=over 4

=item [1] 

By "directly", I still mean inside of L<POE::Component::MessageQueue::Storage::Generic> because
that is the only way to use this module.

=back

=head1 SEE ALSO

L<POE::Component::MessageQueue>,
L<POE::Component::MessageQueue::Storage>,
L<DBI>

I<Other storage engines:>

L<POE::Component::MessageQueue::Storage::Memory>,
L<POE::Component::MessageQueue::Storage::BigMemory>,
L<POE::Component::MessageQueue::Storage::DBI>,
L<POE::Component::MessageQueue::Storage::FileSystem>,
L<POE::Component::MessageQueue::Storage::Generic>,
L<POE::Component::MessageQueue::Storage::Throttled>,
L<POE::Component::MessageQueue::Storage::Complex>,
L<POE::Component::MessageQueue::Storage::Default>

=cut

