
package POE::Component::MessageQueue;

use POE;
use POE::Component::Server::Stomp;
use strict;

use Carp qw(croak);

sub new
{
	my $class = shift;
	my $args  = shift;

	my $address;
	my $hostname;
	my $port;
	my $domain;

	my $storage;

	if ( ref($args) eq 'HASH' )
	{
		$address  = $args->{address};
		$hostname = $args->{hostname};
		$port     = $args->{port};
		$domain   = $args->{domain};
		
		$storage  = $args->{storage};
	}

	if ( not defined $storage )
	{
		# TODO: We could do some kind of default, like using SQLite or memory 
		# or something.  But for now, require that the storage engine be specified.
		croak "$class->new(): Must pass a storage object for the message queue to operate on."
	}

	my $self = {
		storage => $storage,
	};
	bless $self, $class;

	# setup our stomp server
	POE::Component::Server::Stomp->new(
		Address  => $address,
		Hostname => $hostname,
		Port     => $port,
		Domain   => $domain,

		HandleFrame        => $self->__closure('_handle_frame'),
		ClientDisconnected => $self->__closure('_client_disconnected'),
		ClientError        => $self->__closure('_client_error')
	);

	# TODO: We will probably need to setup a custom session for 'master' tasks.

	return $self;
}

sub __closure
{
	my ($self, $method_name) = @_;
	my $func = sub {
		return $self->$method_name(@_);
	};
	return $func;
}

sub _handle_frame
{
	my $self = shift;
	my ($kernel, $heap, $frame) = @_[ KERNEL, HEAP, ARG0 ];

	print "Recieved frame:\n";
	print $frame->as_string . "\n";

	if ( $frame->command eq 'CONNECT' )
	{
		my $response = Net::Stomp::Frame->new({
			command => 'CONNECTED'
		});
		$heap->{client}->put( $response->as_string . "\n" );
	}
}

sub route_frame
{
	my $self = shift;
	my $args = shift;

	my $client;
	my $frame;

	if ( ref($args) eq 'HASH' )
	{
		$client = $args->{client};
		$frame  = $args->{frame};
	}
	else
	{
		$client = $args;
		$frame  = shift;
	}
}

sub _client_disconnected
{
	my $self = shift;
	my ($kernel, $heap) = @_[ KERNEL, HEAP ];

	print "Client disconnected\n";
}

sub _client_error
{
	my $self = shift;
	my ($kernel, $name, $number, $message) = @_[ KERNEL, ARG0, ARG1, ARG2 ];

	print "ERROR: $name $number $message\n";
}

1;

