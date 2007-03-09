
package POE::Component::Server::Stomp;

use POE::Session;
use POE::Component::Server::TCP;
use POE::Filter::Stream;
use IO::String;
use Net::Stomp::Frame;

use Carp qw(croak);
use strict;

my $VERSION = '0.2';

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

	# our tinsy object
	my $self = {
		handle_frame => $handle_frame
	};
	bless $self, $class;

	# pushes data onto the per-client buffers and initiates processing
	my $client_input = sub 
	{
		my ($kernel, $input, $heap) = @_[ KERNEL, ARG0, HEAP ];

		# append input to existing buffer
		$heap->{buffer} .= $input;

		# if not already in a process cycle, then start one!
		if ( $heap->{processing_buffer} != 1 )
		{
			$kernel->yield('process_buffer');
		}
	};

	# create the TCP server.
	POE::Component::Server::TCP->new(
		Port => 61613,

		ClientInput        => $client_input,
		ClientError        => $client_error,
		ClientDisconnected => $client_disconnected,

		ObjectStates => [
			$self => [ 'process_buffer' ]
		],

		ClientFilter => "POE::Filter::Stream"
	);

	# POE::Component::Server::TCP does it!  So, I do it too.
	return undef;
}

sub process_buffer
{
	my ($self, $kernel, $heap) = @_[ OBJECT, KERNEL, HEAP ];

	# we extract the first message at the front of the buffer
	if ( $heap->{buffer} =~ /(.*?)\0(.*)/s )
	{
		$heap->{processing_buffer} = 1;

		my $frame_data;

		# adjust the buffer, grab our data
		$frame_data     = $1;
		$heap->{buffer} = $2;

		# parse the frame 
		my $frame = $self->parse_frame( $frame_data );

		# call the user handler
		splice @_, ARG0, 1, $frame;
		$self->{handle_frame}->(@_);

		# generate another parse buffer event
		$kernel->yield('process_buffer');
	}
	else
	{
		# we're cool!  Nothing more interesting in the buffer.
		$heap->{processing_buffer} = 0;
	}
}

sub parse_frame
{
	my ($self, $input) = @_;

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

	return $frame;
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

