
package POE::Component::MessageQueue::Logger;

use POE::Kernel;
use strict;

my $LEVELS = {
	debug     => 0,
	info      => 1,
	notice    => 2,
	warning   => 3,
	error     => 4,
	critical  => 5,
	alert     => 6,
	emergency => 7
};

our $LEVEL = 3;

sub new
{
	my $class = shift;
	my $args  = shift;

	my $logger_alias;

	if ( ref($args) eq 'HASH' )
	{
		$logger_alias = $args->{logger_alias};
	}

	my $self = {
		logger_alias => $logger_alias
	};

	bless  $self, $class;
	return $self;
}

sub log
{
	my $self = shift;
	my $type = shift;
	my $msg  = shift;

	if ( not defined $msg )
	{
		$msg  = $type;
		$type = 'info';
	}

	if ( defined $self->{logger_alias} )
	{
		$poe_kernel->post( $self->{logger_alias}, $type, "$msg\n" );
	}
	elsif ( $LEVELS->{$type} >= $LEVEL )
	{
		print STDERR "$msg\n";
	}
}

1;

