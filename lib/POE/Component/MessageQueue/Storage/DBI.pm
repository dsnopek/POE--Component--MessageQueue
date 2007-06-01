package POE::Component::MessageQueue::Storage::DBI;
use base qw(POE::Component::MessageQueue::Storage::Generic);

use POE::Component::MessageQueue::Storage::Generic::DBI;
use strict;

sub new
{
	my $class = shift;
	my $args  = $_[0];

	my $throttle_max = 0;

	if ( ref($args) eq 'HASH' )
	{
		$throttle_max = delete $args->{throttle_max} || 0;

		# package up for passing through the generic object
		$args = [ $args ];
	}
	else
	{
		$args = \@_;
	}

	my $self = $class->SUPER::new({
		package      => 'POE::Component::MessageQueue::Storage::Generic::DBI',
		options      => $args,
		throttle_max => $throttle_max
	});

	bless  $self, $class;
	return $self;
}

1;

