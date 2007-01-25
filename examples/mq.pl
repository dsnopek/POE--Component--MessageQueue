
use POE;
use POE::Component::Logger;
use POE::Component::MessageQueue;
use POE::Component::MessageQueue::Storage::DBI;
use strict;

my $DATA_DIR = '/tmp/perl_mq';

# For sqlite:
my $DB_FILE     = "$DATA_DIR/mq.db";
my $DB_DSN      = "DBI:SQLite2:dbname=$DB_FILE";
my $DB_USERNAME = "";
my $DB_PASSWORD = "";

sub _init_sqlite
{
	my $DB_CREATE = << "EOF";
CREATE TABLE messages
(
	message_id  int primary key,
	destination varchar(255) not null,
	persistent  char(1) default 'Y' not null,
	in_use_by   int,
	body        text
);

CREATE INDEX destination_index ON messages ( destination );
CREATE INDEX in_use_by_index   ON messages ( in_use_by );
EOF

	# create initial database
	my $dbh = DBI->connect($DB_DSN, '', '');
	$dbh->do( $DB_CREATE );
	$dbh->disconnect();
}
mkdir $DATA_DIR unless ( -d $DATA_DIR );
_init_sqlite    unless ( -f $DB_FILE );

# we create a logger, because a production message queue would
# really need one.
POE::Component::Logger->spawn(
	ConfigFile => 'log.conf',
	Alias      => 'mq_logger'
);

POE::Component::MessageQueue->new({
	# configure to use a logger
	logger_alias => 'mq_logger',

	storage => POE::Component::MessageQueue::Storage::DBI->new({
		dsn      => $DB_DSN,
		username => $DB_USERNAME,
		password => $DB_PASSWORD,

		# configure to keep the body not in the database but in a file
		data_dir  => $DATA_DIR,
		use_files => 1
	})
});

POE::Kernel->run();
exit;

