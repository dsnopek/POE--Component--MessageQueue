
use inc::Module::Install;

name 'POE-Component-MessageQueue';
abstract 'A Perl message queue based on POE that uses STOMP as its communication protocol.';
author 'David Snopek';
version '0.1.0';
license 'gpl';

build_requires 'Data::Dumper';
# next 2 for the unit-tests
build_requires 'Test::More';
build_requires 'File::Temp';
requires 'POE::Component::Server::TCP';
requires 'POE::Component::Logger';
requires 'POE::Wheel::ReadWrite';
requires 'IO::String';
requires 'IO::File';
requires 'Net::Stomp';
requires 'Carp';
requires 'DBI';
requires 'DBD::SQLite2';

no_index directory => 'test';
no_index directory => 'examples';

install_script 'bin/mq.pl';

if ( $ARGV[0] ne 'dist' )
{
	auto_install;
}

WriteAll;

