
use POE;
use POE::Component::MessageQueue;
use POE::Component::MessageQueue::Storage::DBI;
use POE::Component::MessageQueue::Logger;
use strict;

# Force some logger output without using the real logger.
$POE::Component::MessageQueue::Logger::LEVEL = 0;

my $DATA_DIR = '/tmp/perl_mq';

# For mysql:
#my $DB_DSN      = 'DBI:mysql:database=perl_mq';
#my $DB_USERNAME = 'perl_mq';
#my $DB_PASSWORD = 'glupiludzie';

 For sqlite:
my $DB_FILE     = "$DATA_DIR/mq.db";
my $DB_DSN      = "DBI:SQLite:dbname=$DB_FILE";
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

POE::Component::MessageQueue->new({
	storage => POE::Component::MessageQueue::Storage::DBI->new({
		dsn      => $DB_DSN,
		username => $DB_USERNAME,
		password => $DB_PASSWORD,
	})
});

POE::Kernel->run();
exit;

