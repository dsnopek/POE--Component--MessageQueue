
package POE::Component::Server::Stomp;

use POE::Session;
use POE::Component::Server::TCP;
use POE::Filter::Line;
use IO::String;
use Net::Stomp::Frame;

use Carp qw(croak);
use strict;

my $VERSION = '0.1';

use Data::Dumper;

sub new
{
	my $class = shift;

	# NOTE: Quite a bit of code lifted from POE::Component::Server::Stomp ...

	croak "$class->new() requires an even number of arguments" if (@_ & 1);

	my $args = { @_ };

	# TCP server options
	my $alias   = delete $args->{Alias};
	my $address = delete $args->{Address};
	my $hname   = delete $args->{Hostname};
	my $port    = delete $args->{Port};
	my $domain  = delete $args->{Domain};

	if ( not defined $port )
	{
		# default Stomp port
		$port = 61613;
	}

	# user callbacks.
	my $handle_frame        = delete $args->{HandleFrame};
	my $client_disconnected = delete $args->{ClientDisconnected};
	my $client_error        = delete $args->{ClientError};

	# A closure?  In Perl!?  Hrm...
	my $client_input = sub 
	{
		my ($kernel, $input) = @_[ KERNEL, ARG0 ];

		my $io = IO::String->new( $input );

		my $command;
		my $headers;
		my $body;

		# read the command
		$command = $io->getline;
		chop $command;

		# read headers
		while (1)
		{
			my $line = $io->getline;
			chop $line;
			last if $line eq "";
			my ( $key, $value ) = split /: ?/, $line, 2;
			$headers->{$key} = $value;
		}

		# read the body (all the remaining data)
		$io->read( $body, (length($input) - $io->tell()) );

		# create the frame.
		my $frame = Net::Stomp::Frame->new({
			command => $command,
			headers => $headers,
			body    => $body
		});

		# Replace ARG0 with the parsed frame.
		splice(@_, ARG0, 1, $frame);

		# pass to the user handler
		$handle_frame->(@_);
	};

	# create the TCP server.
	POE::Component::Server::TCP->new(
		Port => 61613,

		ClientInput        => $client_input,
		ClientError        => $client_error,
		ClientDisconnected => $client_disconnected,

		ClientFilter => [ "POE::Filter::Line", Literal => "\000" ],
	);

	# POE::Component::Server::TCP does it!  So, I do it too.
	return undef;
}

1;

