
use inc::Module::Install;

name 'POE-Component-Server-MessageQueue';
abstract 'A Perl message queue based on POE that uses STOMP as its communication protocol.';
author 'David Snopek';
version '0.1';
license 'gpl';

#build_requires '';
requires 'POE::Component::Server::TCP';
requires 'IO::String';
requires 'Net::Stomp::Frame';

if ( $ARGV[0] ne 'dist' )
{
	auto_install;
}

WriteAll;

