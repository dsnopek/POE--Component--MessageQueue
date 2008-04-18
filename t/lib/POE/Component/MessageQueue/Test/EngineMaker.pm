package POE::Component::MessageQueue::Test::EngineMaker;
use strict;
use warnings;

use Exporter qw(import);
use POSIX qw(tmpnam);
use POE::Component::MessageQueue::Storage::Default;
our @EXPORT = qw(
	make_engine engine_names make_db engine_package DATA_DIR LOG_LEVEL
);

my $data_dir = tmpnam();

sub DATA_DIR { 
	if (my $dir = shift) {
		$data_dir = $dir;
	}
	return $data_dir;
}
sub DB_FILE { DATA_DIR.'/mq.db' }
sub DSN { 'DBI:SQLite:dbname='.DB_FILE }

my $level = 7;
sub LOG_LEVEL {
	if (my $nl = shift) {
		$level = $nl;
	}
	return $level;
}

my %engines = (
	DBI        => {
		args    => sub {(
			dsn      => DSN,
			username => q(),
			password => q(),
		)},
	},
	FileSystem => {
		args     => sub {(
			info_storage => make_engine('DBI'),
			data_dir     => DATA_DIR,
		)},
	},
	Throttled  => {
		args    => sub {(
			throttle_max => 2,
			back         => make_engine('FileSystem'),
		)},
	},
	Complex    => {
		args    => sub {(
			timeout     => 4,
			granularity => 2,
			front_max   => 1024,
			front       => make_engine('BigMemory'),
			back        => make_engine('Throttled'),
		)}
	},
	BigMemory => {},
	Memory    => {},
);

sub engine_package {'POE::Component::MessageQueue::Storage::'.shift} 
sub engine_names { keys %engines }

sub make_engine {
	my $name = shift;
	my $eargs = $engines{$name}->{args} || sub {};
	return engine_package($name)->new($eargs->(),
		logger => POE::Component::MessageQueue::Logger->new(level=>LOG_LEVEL),
	);
}

sub make_db {
	POE::Component::MessageQueue::Storage::Default::_make_db(
		DB_FILE, DSN, q(), q());
}

1;
