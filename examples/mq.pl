
use POE;
use POE::Component::MessageQueue;
use POE::Component::MessageQueue::Storage::DBI;
use strict;

my $DB_DSN      = 'DBI:mysql:database=perl_mq';
my $DB_USERNAME = 'perl_mq';
my $DB_PASSWORD = 'glupiludzie';

POE::Component::MessageQueue->new({
	storage => POE::Component::MessageQueue::Storage::DBI->new({
		dsn      => $DB_DSN,
		username => $DB_USERNAME,
		password => $DB_PASSWORD
	})
});

POE::Kernel->run();
exit;

