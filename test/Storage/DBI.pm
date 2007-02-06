
package Local::Storage::DBI;

use POE::Kernel;
use POE::Session;
use POE::Component::MessageQueue::Storage::DBI;
use POE::Component::MessageQueue::Message;
use File::Temp;
use Test::More;
use DBI;
use strict;

use Data::Dumper;

BEGIN
{
	plan tests => 17;
}

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

my $TESTS = [
	'testStore', 'testRemove', 'testClaim'
];
my $TEST_INDEX = 0;

POE::Session->create(
	package_states => [
		'Local::Storage::DBI' => [ 
			# boiler placte
			'_start', '_stop', 'setup', '_runTest0', 'teardown',
			# actual tests
			'testStore', 'testStore_post',
			'testRemove', 'testRemove_0', 'testRemove_1',
			'testClaim',
		]
	]
);

POE::Kernel->run();
exit;

sub _start
{
	my ($kernel, $heap) = @_[ KERNEL, HEAP ];
	
	# get a temporary file
	my $fh = File::Temp->new( UNLINK => 0 );
	$heap->{dbfile} = $fh->filename();
	$fh->print;
	$fh->close();

	# create initial database
	my $dsn = "dbi:SQLite2:dbname=$heap->{dbfile}";
	my $dbh = DBI->connect($dsn, '', '');
	$dbh->do( $DB_CREATE );
	$dbh->disconnect();

	# setup the storage object
	$heap->{storage} = POE::Component::MessageQueue::Storage::DBI->new({
		dsn      => $dsn,
		username => '',
		password => '',
	});

	# list here all the 
	runTest( $TESTS->[0] );
}

sub _stop
{
	my ($kernel, $heap) = @_[ KERNEL, HEAP ];

	if ( defined $heap->{dbfile} )
	{
		unlink $heap->{dbfile};
		$heap->{dbfile} = undef;
	}
}

sub runTest
{
	my $test_name = shift;

	$poe_kernel->yield( 'setup' );
	$poe_kernel->yield( '_runTest0', $test_name );
}

sub _runTest0
{
	my ($kernel, $heap, $test_name) = @_[ KERNEL, HEAP, ARG0 ];

	print "$test_name\n";
	eval( "$test_name(\@_)" );

	$kernel->yield( 'teardown' );
}

sub nextTest
{
	# do the next test
	if ( ++$TEST_INDEX < scalar @$TESTS )
	{
		runTest( $TESTS->[$TEST_INDEX] );
	}
	else
	{
		$poe_kernel->stop();
	}
}

sub setup
{
	my ($kernel, $heap) = @_[ KERNEL, HEAP ];

	# remove handlers
	$heap->{storage}->set_dispatch_message_handler( undef );
	$heap->{storage}->set_destination_ready_handler( undef );
}

sub teardown
{
	my ($kernel, $heap) = @_[ KERNEL, HEAP ];

	$kernel->post( 'MQ-EasyDBI',
		do => {
			sql   => 'DELETE FROM messages',
			event => 'nothing'
		}
	);
}

sub testStore
{
	my ($kernel, $heap) = @_[ KERNEL, HEAP ];

	my $message = POE::Component::MessageQueue::Message->new({
		message_id  => 27,
		destination => '/queue/something',
		body        => 'A message',
		persistent  => 'Y',
	});

	$heap->{storage}->store( $message );

	$kernel->post( 'MQ-EasyDBI',
		hash => {
			sql   => 'SELECT * FROM messages WHERE message_id = 27',
			event => 'testStore_post'
		}
	);
}

sub testStore_post
{
	my ($kernel, $value) = @_[ KERNEL, ARG0 ];

	my $result = $value->{result};

	is( $result->{message_id},  27 );
	is( $result->{destination}, '/queue/something' );
	is( $result->{persistent},  'Y' );
	is( $result->{in_use_by},   undef );
	is( $result->{body}, 'A message' );

	# advance the testing, yo!
	nextTest();
}

sub testRemove
{
	my ($kernel, $heap) = @_[ KERNEL, HEAP ];

	my $message = POE::Component::MessageQueue::Message->new({
		message_id  => 27,
		destination => '/queue/something',
		body        => 'A message',
		persistent  => 'Y',
	});

	$heap->{storage}->store( $message );

	$kernel->post( 'MQ-EasyDBI',
		hash => {
			sql   => 'SELECT * FROM messages WHERE message_id = 27',
			event => 'testRemove_0'
		}
	);
}

sub testRemove_0
{
	my ($kernel, $heap, $value) = @_[ KERNEL, HEAP, ARG0 ];

	my $result = $value->{result};

	is( $result->{message_id},  27 );
	is( $result->{destination}, '/queue/something' );
	is( $result->{persistent},  'Y' );
	is( $result->{in_use_by},   undef );
	is( $result->{body}, 'A message' );

	$heap->{storage}->remove( 27 );

	$kernel->post( 'MQ-EasyDBI',
		hash => {
			sql   => 'SELECT * FROM messages WHERE message_id = 27',
			event => 'testRemove_1'
		}
	);
}

sub testRemove_1
{
	my ($kernel, $value) = @_[ KERNEL, ARG0 ];
	my $result = $value->{result};

	# has no items!
	is( scalar %$result, 0 );

	# advance the testing, yo!
	nextTest();
}

sub testClaim
{
	my ($kernel, $heap) = @_[ KERNEL, HEAP ];

	my $message = POE::Component::MessageQueue::Message->new({
		message_id  => 27,
		destination => '/queue/something',
		body        => 'A message',
		persistent  => 'Y',
	});

	$heap->{storage}->store( $message );

	# set up the handlers
	$heap->{storage}->set_dispatch_message_handler( \&testClaim_0 );
	$heap->{storage}->set_destination_ready_handler( \&testClaim_1 );
	
	# attempt to claim the message
	$heap->{storage}->claim_and_retrieve( '/queue/something', 10 );
}

sub testClaim_0
{
	my $message = shift;

	is( $message->{message_id},  27 );
	is( $message->{destination}, '/queue/something' );
	is( $message->{body},        'A message' );
	is( $message->{persistent},  'Y' );
	is( $message->{in_use_by},   10 );
}

sub testClaim_1
{
	my $destination = shift;

	is ( $destination, '/queue/something' );

	# carry on!
	nextTest();
}

1;

