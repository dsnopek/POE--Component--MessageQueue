
use inc::Module::Install;

name 'POE-Component-MessageQueue';
abstract 'A Perl message queue based on POE that uses STOMP as its communication protocol.';
author 'David Snopek';
version '0.1.6';
license 'gpl';

build_requires 'Data::Dumper';
# next 2 for the unit-tests
build_requires 'Test::More';
build_requires 'File::Temp';
requires 'POE' => 0.38;
requires 'POE::Component::Server::TCP';
requires 'POE::Component::Logger';
requires 'POE::Component::Generic' => 0.1001;
requires 'POE::Wheel::ReadWrite';
requires 'Net::Stomp';
requires 'Exception::Class::DBI';
requires 'Exception::Class::TryCatch';
requires 'DBI';
requires 'DBD::SQLite' => 1.13;
requires 'IO::String';
requires 'IO::File';
requires 'Carp';

no_index directory => 'test';
no_index directory => 'examples';

install_script 'bin/mq.pl';

if ( $ARGV[0] ne 'dist' )
{
	auto_install;
}

WriteAll;

