
use inc::Module::Install;

name 'POE-Component-MessageQueue';
abstract 'A Perl message queue based on POE that uses STOMP as its communication protocol.';
author 'David Snopek';
version '0.2.9';
license 'gpl';

build_requires 'Data::Dumper';
# next 2 for the unit-tests
build_requires 'Test::More';
build_requires 'Test::Exception';
build_requires 'Test::MockObject';
build_requires 'File::Temp';
requires 'POE' => 0.38;
requires 'POE::Component::Server::TCP';
requires 'POE::Component::Logger';
requires 'POE::Component::Generic' => 0.1001;
requires 'POE::Wheel::ReadWrite';
requires 'POE::Filter::Stomp' => 0.04;
requires 'Net::Stomp';
requires 'Exception::Class::DBI';
requires 'Exception::Class::TryCatch';
requires 'Event::Notify';
requires 'DBI';
requires 'DBD::SQLite' => 1.13;
requires 'IO::String';
requires 'IO::File';
requires 'Carp';
requires 'Best';
requires 'YAML';
requires 'Data::UUID';
requires 'Heap' => 0.80;
requires 'Heap::Fibonacci' => 0.80;
requires 'Moose';
requires 'MooseX::AttributeHelpers' => 0.08;
requires 'MooseX::MultiInitArg';

recommends 'YAML::Syck';

no_index directory => 'test';
no_index directory => 'examples';

install_script 'bin/mq.pl';

if ( $ARGV[0] ne 'dist' )
{
	auto_install;
}

WriteAll;

