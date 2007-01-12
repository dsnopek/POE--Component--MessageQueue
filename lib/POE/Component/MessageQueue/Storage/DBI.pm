
package POE::Component::MessageQueue::Storage::DBI;

use POE::Component::EasyDBI;
use strict;

sub new
{
	my $class = shift;
	my $args  = shift;

	my $dsn;
	my $username;
	my $password;
	my $options;

	if ( ref($args) eq 'HASH' )
	{
		$dsn      = $args->{dsn};
		$username = $args->{username};
		$password = $args->{password};
		$options  = $args->{options};
	}

	# TODO: This needs to make an EasyDBI session and a custom session
	# to recieve all of its events.

	my $self = {
	};

	bless  $self, $class;
	return $self;
}

1;

