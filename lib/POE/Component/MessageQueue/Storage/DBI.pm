package POE::Component::MessageQueue::Storage::DBI;
use base qw(POE::Component::MessageQueue::Storage::Generic);

use POE::Component::MessageQueue::Storage::Generic::DBI;
use strict;

sub new
{
	my $class = shift;

	my $self = $class->SUPER::new({
		package      => 'POE::Component::MessageQueue::Storage::Generic::DBI',
		options      => \@_,
	});

	bless  $self, $class;
	return $self;
}

1;

