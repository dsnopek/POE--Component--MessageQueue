
package Local::Storage::Generic::DBI;
use base qw(Test::Class);

use POE::Component::MessageQueue::Message;
use POE::Component::MessageQueue::Storage::Generic::DBI;
use File::Temp;
use Test::More;
use strict;

use Data::Dumper;

my $DB_CREATE = << "EOF";
CREATE TABLE messages
(
	message_id  int primary key,
	destination varchar(255) not null,
	persistent  char(1) default 'Y' not null,
	in_use_by   int,
	body        text
);
EOF

sub setup : Test(setup)
{
	my $self = shift;

	# get a temporary file
	my $fh = File::Temp->new( UNLINK => 0 );
	$self->{dbfile} = $fh->filename();
	$fh->print;
	$fh->close();

	# create initial database
	my $dsn = "dbi:SQLite:dbname=$self->{dbfile}";
	my $dbh = DBI->connect($dsn, '', '');
	$dbh->do( $DB_CREATE );
	$dbh->disconnect();

	# setup the storage object
	$self->{storage} = POE::Component::MessageQueue::Storage::Generic::DBI->new({
		dsn      => $dsn,
		username => '',
		password => '',
	});
}

sub teardown : Test(teardown)
{
	my $self = shift;

	if ( defined $self->{dbfile} )
	{
		unlink $self->{dbfile};
		$self->{dbfile} = undef;
	}
}

sub _make_message
{
	my $self = shift;

	my $message = POE::Component::MessageQueue::Message->new(@_);
	if ( not defined $message->{message_id} )
	{
		$message->{message_id} = $self->{storage}->get_next_message_id();
	}

	return $message;
}

sub testStoreRemove : Test(9)
{
	my $self = shift;
	
	my $message = $self->_make_message({
		destination => '/queue/a_wicked_cool.queue_name',
		body        => 'A test message',
		persistent  => 1,
		in_use_by   => 27,
	});

	my $handler = sub
	{
		my ($destination) = @_;
		is( $destination, '/queue/a_wicked_cool.queue_name' );
	};

	is( $self->{storage}->set_message_stored_handler($handler), undef );
	is( $self->{storage}->store($message), undef );

	my $r;

	$r = $self->{storage}->{dbh}->selectrow_hashref("SELECT * FROM messages");
	is( $r->{message_id},  1 );
	is( $r->{destination}, '/queue/a_wicked_cool.queue_name' );
	is( $r->{body},        'A test message' );
	is( $r->{persistent},  1 );
	is( $r->{in_use_by},   27 );

	$self->{storage}->remove( 1 );

	$r = $self->{storage}->{dbh}->selectrow_hashref("SELECT * FROM messages");
	is( $r, undef );
}

sub testClaimAndRetrieve : Test(14)
{
	my $self = shift;
	
	my $message = $self->_make_message({
		destination => '/queue/a_wicked_cool.queue_name',
		body        => 'Another test message',
		persistent  => 0,
	});

	my $handler = sub
	{
		my ($message, $destination, $client_id) = @_;

		is( $destination, "/queue/a_wicked_cool.queue_name" );
		is( $client_id,   10 );

		is( $message->{message_id},  1 );
		is( $message->{destination}, "/queue/a_wicked_cool.queue_name" );
		is( $message->{body},        "Another test message" );
		is( $message->{persistent},  0 );
		is( $message->{in_use_by},   10 );

	};

	$self->{storage}->set_message_stored_handler(sub { });
	is( $self->{storage}->set_dispatch_message_handler($handler), undef );

	$self->{storage}->store( $message );
	is( $self->{storage}->claim_and_retrieve("/queue/a_wicked_cool.queue_name", 10), undef );

	my $r = $self->{storage}->{dbh}->selectrow_hashref("SELECT * FROM messages");
	is( $r->{message_id},  1 );
	is( $r->{destination}, '/queue/a_wicked_cool.queue_name' );
	is( $r->{body},        'Another test message' );
	is( $r->{persistent},  0 );
	is( $r->{in_use_by},   10 );
}

sub testDisown : Test(15)
{
	my $self = shift;

	my $stmt = $self->{storage}->{dbh}->prepare( "INSERT INTO messages (message_id, destination, persistent, in_use_by, body) VALUES ( ?, ?, ?, ?, ? )" );

	# artificially create two messages
	$stmt->execute( 777, '/queue/queue_one', 1, 27, 'jeden' );
	$stmt->execute( 778, '/queue/queue_one', 1, 28, 'dwa' );
	$stmt->execute( 779, '/queue/queue_two', 1, 27, 'trzy' );

	# test that only one of these messages is disowned
	$self->{storage}->disown('/queue/queue_one', 27);

	my $r;

	$r = $self->{storage}->{dbh}->selectrow_hashref("SELECT * FROM messages WHERE message_id = 777");
	is( $r->{message_id},  777 );
	is( $r->{destination}, '/queue/queue_one' );
	is( $r->{body},        'jeden' );
	is( $r->{persistent},  1 );
	is( $r->{in_use_by},   undef );

	$r = $self->{storage}->{dbh}->selectrow_hashref("SELECT * FROM messages WHERE message_id = 778");
	is( $r->{message_id},  778 );
	is( $r->{destination}, '/queue/queue_one' );
	is( $r->{body},        'dwa' );
	is( $r->{persistent},  1 );
	is( $r->{in_use_by},   28 );

	$r = $self->{storage}->{dbh}->selectrow_hashref("SELECT * FROM messages WHERE message_id = 779");
	is( $r->{message_id},  779 );
	is( $r->{destination}, '/queue/queue_two' );
	is( $r->{body},        'trzy' );
	is( $r->{persistent},  1 );
	is( $r->{in_use_by},   27 );
}

Test::Class->runtests;

1;


