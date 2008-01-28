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

	my $alias = 'MQ-Storage-Generic';

	my $generic = $self->{generic} = POE::Component::Generic->spawn(
		package => $package,
		object_options => $options,
		packages => {
			$package => {
				callbacks => [qw(
					remove    remove_multiple     remove_all
					store     claim_and_retrieve  storage_shutdown
				)],
				postbacks => {
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
			session => $alias,
			event   => '_error'
		},
		#debug => 1,
		#verbose => 1
	);

	my $session = $self->{session} = POE::Session->create(
		inline_states => {
			_start => sub {
				$_[KERNEL]->alias_set($alias);
			},
			_shutdown => sub {
				my $callback = $_[ARG0];
				$generic->shutdown();
				$_[KERNEL]->alias_remove($alias);
				$self->_log('alert', 'Generic storage engine is shutdown!');
				$callback->();
			},
		},
		object_states => [
			$self => [
				'_general_handler',
				'_log_proxy',
				'_error',
			]
		]
	);
	$generic->set_log_function(
		$self->_data_hashref(), 
		{session => $session->ID(), event =>'_log_proxy'},
	);

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
	my ($self, $message, $callback) = @_;

	$self->{generic}->store($self->_data_hashref(), $message, $callback);
	return;
}

sub remove
{
	my ($self, $message_id, $callback) = @_;

	$self->{generic}->remove($self->_data_hashref(), $message_id, $callback);
	return;
}

sub remove_multiple
{
	my ($self, $message_ids, $callback) = @_;

	$self->{generic}->remove($self->_data_hashref(), $message_ids, $callback);
	return;
}

sub remove_all
{
	my ($self, $callback) = @_;

	$self->{generic}->remove($self->_data_hashref(), $callback);
	return;
}

sub claim_and_retrieve
{
	my ($self, $destination, $client_id, $dispatch) = @_;

	# Skip if we're already claiming for this destination
	if ($self->{claiming}->{$destination})
	{
		$dispatch->(undef, $destination, $client_id);
	}
	else
	{
		# Lock destination 
		$self->{claiming}->{$destination} = $client_id;

		my $done_claiming = sub {
			# Unlock and move along
			delete $self->{claiming}->{$destination};
			$dispatch->(@_);
		};

		$self->{generic}->claim_and_retrieve($self->_data_hashref(),
			$destination, $client_id, $done_claiming);
	}

	return;
}

sub disown
{
	my ($self, $destination, $client_id) = @_;

	$self->{generic}->disown(
		$self->_data_hashref(), $destination, $client_id
	);
	return;
}

sub storage_shutdown
{
	my ($self, $complete) = @_;

	$self->_log('alert', 'Shutting down generic storage engine...');

	# Send the shutdown message to generic - it will come back when it's cleaned
	# up its resources, and we can stop it for reals (as well as stop our own
	# session).  
	$self->{generic}->yield(storage_shutdown => $self->_data_hashref(), sub {
		$poe_kernel->post($self->{session}, '_shutdown', $complete);
	});

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

