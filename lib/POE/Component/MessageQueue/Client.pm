
package POE::Component::MessageQueue::Client;

use POE::Kernel;
use strict;

sub new
{
	my $class = shift;
	my $args  = shift;

	my $client_id;

	if ( ref($args) eq 'HASH' )
	{
		$client_id = $args->{client_id};
	}
	else
	{
		$client_id = $args;
	}

	my $self = 
	{
		client_id   => $client_id,
		# a list of queues we are subscribed to.
		queue_names => [ ],
		connected   => 0,
		login       => '',
		passcode    => '',
	};

	bless  $self, $class;
	return $self;
}

sub _add_queue_name
{
	my ($self, $queue_name) = @_;
	push @{$self->{queue_names}}, $queue_name;
}

sub _remove_queue_name
{
	my ($self, $queue_name) = @_;

	my $i;
	my $max = scalar @{$self->{queue_names}};

	for( $i = 0; $i < $max; $i++ )
	{
		if ( $self->{queue_names}->[$i] == $queue_name )
		{
			splice @{$self->{queue_names}}, $i, 1;
			return;
		}
	}
}

sub send_frame
{
	my $self  = shift;
	my $frame = shift;

	my $client_session = $poe_kernel->alias_resolve( $self->{client_id} );

	# Check to see if the client's session is still around
	if ( defined $client_session )
	{
		my $client = $client_session->get_heap()->{client};

		# Check to see if the socket's Wheel is still around
		if ( defined $client )
		{
			$client->put( $frame->as_string() . "\n" );

			return 1;
		}
	}

	return 0;
}

sub connect
{
	my $self = shift;
	my $args = shift;

	my $login;
	my $passcode;

	if ( ref($args) eq 'HASH' )
	{
		$login    = $args->{login};
		$passcode = $args->{passcode};
	}
	else
	{
		$login    = $args;
		$passcode = shift;
	}

	# set variables, yo!
	$self->{login}     = $login;
	$self->{passcode}  = $passcode;
	$self->{connected} = 1;

	# send connection confirmation
	my $response = Net::Stomp::Frame->new({
		command => "CONNECTED",
		headers => {
			session => "client-$self->{client_id}",
		},
	});
	$self->send_frame( $response );
}

sub shutdown
{
	my $self = shift;

	$poe_kernel->post( $self->{client_id}, "shutdown" );
}

1;

