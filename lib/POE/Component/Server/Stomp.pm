
package POE::Component::Server::Stomp;

use POE::Session;
use POE::Component::Server::TCP;
use POE::Filter::Line;
use IO::String;
use Net::Stomp::Frame;

use Carp qw(croak);
use strict;

my $VERSION = '0.1';

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

__END__

=pod

=head1 NAME

POE::Component::Server::Stomp - A generic Stomp server for POE

=head1 SYNOPSIS

	use POE qw(Component::Server::Stomp);
	use Net::Stomp::Frame;
	use strict;

	POE::Component::Server::Stomp->new(
		HandleFrame        => \&handle_frame,
		ClientDisconnected => \&client_disconnected,
		ClientErrorr       => \&client_error
	);

	POE::Kernel->run();
	exit;

	sub handle_frame
	{
		my ($kernel, $heap, $frame) = @_[ KERNEL, HEAP, ARG0 ];

		print "Recieved frame:\n";
		print $frame->as_string() . "\n";

		# allow Stomp clients to connect by playing along.
		if ( $frame->command eq 'CONNECT' )
		{
			my $response = Net::Stomp::Frame->new({
				command => 'CONNECTED'
			});
			$heap->{client}->put( $response->as_string . "\n" );
		}
	}

	sub client_disconnected
	{
		my ($kernel, $heap) = @_[ KERNEL, HEAP ];

		print "Client disconnected\n";
	}

	sub client_error
	{
		my ($kernel, $name, $number, $message) = @_[ KERNEL, ARG0, ARG1, ARG2 ];

		print "ERROR: $name $number $message\n";
	}

=head1 DESCRIPTION

A thin layer over L<POE::Component::Server::TCP> that parses out L<Net::Stomp::Frame>s.  The
synopsis basically covers everything you are capable to do.

For information on the STOMP protocol:

L<http://stomp.codehaus.org/Protocol>

For a full-fledged message queue that uses this module:

L<POE::Component::MessageQueue>

=head1 BUGS

Probably.

=head1 AUTHORS

Copyright 2007 David Snopek.

=cut

