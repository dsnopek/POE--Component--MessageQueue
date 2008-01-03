#
# Copyright 2007 David Snopek <dsnopek@gmail.com>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

package POE::Component::MessageQueue::Storage::Generic;
use base qw(POE::Component::MessageQueue::Storage);

use POE;
use POE::Component::Generic 0.1001;
use POE::Component::MessageQueue::Logger;
use strict;

use Data::Dumper;

sub new
{
	my $class = shift;
	my $args  = shift;

	my $package;
	my $options;

	if ( ref($args) eq 'HASH' )
	{
		$package = $args->{package};
		$options = $args->{options};
	}
	else
	{
		$package = $args;
		$options = shift;
	}

	my $self = $class->SUPER::new( $args );

	$self->{claiming}     = { };

	my $generic = POE::Component::Generic->spawn(
		package => $package,
		object_options => $options,
		packages => {
			$package => {
				postbacks => {
					set_callback     => 1,
					set_log_function => 0,
				},
				factories => [ 'get_logger' ],
			},
			'POE::Component::MessageQueue::Logger' =>
			{
				postbacks => [ 'set_log_function' ]
			}
		},
		error => {
			session => 'MQ-Storage-Generic',
			event   => '_error'
		},
		#debug => 1,
		#verbose => 1
	);

	$self->{session_alias} = 'MQ-Storage-Generic';

	my $session = POE::Session->create(
		inline_states => {
			_start => sub {
				$_[KERNEL]->alias_set($self->{session_alias})
			},
		},
		object_states => [
			$self => [
				'_general_handler',
				'_log_proxy',
				'_message_stored',
				'_dispatch_message',
				'_destination_ready',
				'_finished_claiming',
				'_shutdown_complete',
				'_error',
			]
		]
	);

	# store the sessions
	$self->{generic} = $generic;
	$self->{session} = $session;

	sub make_postback { return { session => $session->ID(), event => shift } }

	# before anything else, set the log function
	$generic->set_log_function(
		$self->_data_hashref(), make_postback('_log_proxy')
	);

	# set up postbacks
	foreach my $event qw(message_stored     dispatch_message
	                     destination_ready  shutdown_complete)
	{
		$generic->set_callback(
			$self->_data_hashref(), $event, make_postback("_$event")
		);
	}

	return bless $self, $class;
}

# Internal shortcut: second argument is extra stuff to add to the data
# hashref, but by default it just sets up the "_general_handler" stuff.
sub _data_hashref
{
	my ($self, $extras) = @_;
	my $data_hashref = {
		session => $self->{session}->ID(),
		event   => '_general_handler',
	};

	if ($extras)
	{
		while (my ($key, $val) = each(%$extras))
		{
			$data_hashref->{$key} = $val;	
		}
	}	
	return $data_hashref;
}

sub store
{
	my ($self, $message) = @_;

	$self->{generic}->store($self->_data_hashref(), $message);
	return;
}

sub remove
{
	my ($self, $message_id) = @_;

	$self->{generic}->remove($self->_data_hashref(), $message_id);
	return;
}

sub claim_and_retrieve
{
	my $self = shift;
	my $args = shift;

	my $destination;
	my $client_id;

	if ( ref($args) eq 'HASH' )
	{
		$destination = $args->{destination};
		$client_id   = $args->{client_id};
	}
	else
	{
		$destination = $args;
		$client_id   = shift;
	}

	if ( $self->{claiming}->{$destination} )
	{
		# we are already attempting to claim a message for this destination!
		return 0;
	}
	else
	{
		# lock temporarily.
		$self->{claiming}->{$destination} = $client_id;
	}

	$self->{generic}->claim_and_retrieve(
		$self->_data_hashref({
			event => '_finished_claiming',
			data  => {
				destination => $destination,
			}
		}),
		{destination => $destination, client_id => $client_id},
	);

	# let the caller know that this is actually going down.
	return 1;
}

sub disown
{
	my ($self, $destination, $client_id) = @_;

	$self->{generic}->disown(
		$self->_data_hashref(), $destination, $client_id
	);
	return;
}

sub shutdown
{
	my $self = shift;

	$self->_log('alert', 'Shutting down generic storage engine...');

	$self->{shutdown} = 1;

	# Send the shutdown message.  When the underlying object calls
	# the callback, we will then be sure that all message before it
	# have gotten through and handled.
	$self->{generic}->yield('storage_shutdown', $self->_data_hashref());
	return;
}

sub _general_handler
{
	my ($self, $kernel, $ref, $result) = @_[ OBJECT, KERNEL, ARG0, ARG1 ];

	if ( $ref->{error} )
	{
		$self->_log("error", "Generic error: $ref->{error}");
	}
	return;
}

sub _error
{
	my ( $self, $err ) = @_[ OBJECT, ARG0 ];

	if ( $err->{stderr} )
	{
		$self->_log('debug', $err->{stderr});
	}
	else
	{
		$self->_log('error', "Generic error:  $err->{operation} $err->{errnum} $err->{errstr}");

		if ( $self->{shutdown} )
		{
			# if any error occurs while attempting to shutdown, then
			# we simply force a shutdown.
			$self->_log('error', 'Forcing shutdown from error');
			$poe_kernel->post( $self->{session}, '_shutdown_complete' );
		}
	}
	return;
}

sub _log_proxy
{
	my ($self, $type, $msg) = @_[ OBJECT, ARG0, ARG1 ];

	$self->_log($type, $msg);
	return;
}

sub _finished_claiming
{
	my ($self, $ref, $result) = @_[ OBJECT, ARG0, ARG1 ];

	my $destination = $ref->{data}->{destination};

	# unlock claiming from this destination.  We need to do this here
	# because _destination_ready will only occure after a message has been
	# fully claimed, but not if no message was claimed.  This covers the
	# empty queue case.
	delete $self->{claiming}->{$destination};
	return;
}

sub _message_stored
{
	my ($self, $message) = @_[ OBJECT, ARG0 ];
	$self->call_back('message_stored', $message);
	return;
}

sub _dispatch_message
{
	my ($self, $message, $destination, $client_id) = @_[ OBJECT, ARG0, ARG1, ARG2 ];

	die 'Pulled message from backstore, but there is no dispatch_message handler'
		unless exists $self->{callbacks}->{dispatch_message};

	# call the handler because the message is complete
	$self->call_back('dispatch_message', $message, $destination, $client_id );
	return;
}

sub _destination_ready
{
	my ($self, $destination) = @_[ OBJECT, ARG0 ];

	# NOTE: This will happen after a message is fully claimed.

	# unlock claiming from this destination
	delete $self->{claiming}->{$destination};

	# notify whoaver, that the destination is ready for another client to 
	# try to claim a message.
	$self->call_back('destination_ready', $destination);
	return;
}

sub _shutdown_complete
{
	my ($self) = @_[ OBJECT ];

	# shutdown the generic object
	$self->{generic}->shutdown();

	# clear our alias, so this session should end.
	$poe_kernel->alias_remove($self->{session_alias});

	$self->_log('alert', 'Generic storage engine is shutdown!');

	# We are shutdown!  Hurray!  Start passing it up the chain.
	$self->call_back('shutdown_complete');
	return;
}

1;

__END__

=pod

=head1 NAME

POE::Component::MessageQueue::Storage::Generic -- Wraps storage engines that aren't asynchronous via L<POE::Component::Generic> so they can be used.

=head1 SYNOPSIS

  use POE;
  use POE::Component::MessageQueue;
  use POE::Component::MessageQueue::Storage::Generic;
  use POE::Component::MessageQueue::Storage::Generic::DBI;
  use strict;

  # For mysql:
  my $DB_DSN      = 'DBI:mysql:database=perl_mq';
  my $DB_USERNAME = 'perl_mq';
  my $DB_PASSWORD = 'perl_mq';
  my $DB_OPTIONS  = undef;

  POE::Component::MessageQueue->new({
    storage => POE::Component::MessageQueue::Storage::Generic->new({
      package => 'POE::Component::MessageQueue::Storage::DBI',
      options => [{
        dsn      => $DB_DSN,
        username => $DB_USERNAME,
        password => $DB_PASSWORD,
        options  => $DB_OPTIONS
      }],
    })
  });

  POE::Kernel->run();
  exit;

=head1 DESCRIPTION

Wraps storage engines that aren't asynchronous via L<POE::Component::Generic> so they can be used.

Using this module is by far the easiest way to write custom storage engines because you don't have to worry about making your operations asynchronous.  This approach isn't without its down-sides, but on the whole, the simplicity is worth it.

There is only one package currently provided designed to work with this module: L<POE::Component::MessageQueue::Storage::Generic::DBI>.

=head1 CONSTRUCTOR PARAMETERS

=over 2

=item package => SCALAR

The name of the package to wrap.

=item options => ARRAYREF

The arguments to pass to the new() function of the above package.

=back

=head1 SEE ALSO

L<DBI>,
L<POE::Component::Generic>,
L<POE::Component::MessageQueue>,
L<POE::Component::MessageQueue::Storage>,
L<POE::Component::MessageQueue::Storage::Memory>,
L<POE::Component::MessageQueue::Storage::FileSystem>,
L<POE::Component::MessageQueue::Storage::DBI>,
L<POE::Component::MessageQueue::Storage::Generic::DBI>,
L<POE::Component::MessageQueue::Storage::Throttled>,
L<POE::Component::MessageQueue::Storage::Complex>

=cut

