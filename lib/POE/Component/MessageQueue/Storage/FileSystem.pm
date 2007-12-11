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

package POE::Component::MessageQueue::Storage::FileSystem;
use base qw(POE::Component::MessageQueue::Storage);

use POE::Kernel;
use POE::Session;
use POE::Filter::Stream;
use POE::Wheel::ReadWrite;
use IO::File;
use strict;

use Data::Dumper;

sub new
{
	my $class = shift;
	my $args  = shift;

	my $info_storage;
	my $data_dir;

	if ( ref($args) eq 'HASH' )
	{
		$info_storage = $args->{info_storage};
		$data_dir     = $args->{data_dir};
	}

	my $self = $class->SUPER::new( $args );

	# for storing message properties
	$self->{info_storage} = $info_storage;
	$self->{info_storage}->set_dispatch_message_handler( $self->__closure('_dispatch_message') );
	$self->{info_storage}->set_shutdown_complete_handler( $self->__closure('_shutdown_complete') );

	# for keeping the message body on the FS
	$self->{data_dir}    = $data_dir;
	$self->{file_wheels} = { };
	$self->{wheel_to_message_map} = { };

	# marks when we are shutting down
	$self->{shutdown} = 0;

	my $session = POE::Session->create(
		inline_states => {
			_start => sub {
				$_[KERNEL]->alias_set('MQ-Storage-FileSystem')
			},
		},
		object_states => [
			$self => [
				'_write_message_to_disk',
				'_read_message_from_disk',
				'_read_input',
				'_read_error',
				'_write_flushed_event',

				# for debug!
				'_log_state'
			]
		]
	);

	# store sessions
	$self->{session} = $session;

	# DEBUG!
	#$poe_kernel->post( $self->{session}, '_log_state' );

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

# set_dispatch_message_handler()  -- We maintain the parents version.
# set_shutdown_complete_handler() -- We maintain the parents version.

sub set_message_stored_handler
{
	my ($self, $handler) = @_;

	# We never need to call this directly, info_storage will!
	#$self->SUPER::set_message_stored_handler( $handler );

	$self->{info_storage}->set_message_stored_handler( $handler );
}

sub set_destination_ready_handler
{
	my ($self, $handler) = @_;

	# We never need to call this directly, info_storage will!
	#$self->SUPER::set_destination_ready_handler( $handler );

	$self->{info_storage}->set_destination_ready_handler( $handler );
}

sub set_logger
{
	my ($self, $logger) = @_;

	$self->SUPER::set_logger( $logger );
	$self->{info_storage}->set_logger( $logger );
}

sub get_next_message_id
{
	my $self = shift;
	return $self->{info_storage}->get_next_message_id();
}

sub store
{
	my ($self, $message) = @_;

	# grab the masseg body
	my $body = $message->{body};
	
	# remake the message, but without the body
	my $temp = POE::Component::MessageQueue::Message->new({
		message_id  => $message->{message_id},
		destination => $message->{destination},
		persistent  => $message->{persistent},
		in_use_by   => $message->{in_use_by},
		size        => $message->{size},
		body        => undef,
	});
	$message = $temp;

	# DRS: To avaid a race condition where:
	#
	#  (1) We post _writer_message_to_disk
	#  (2) Message is "removed" from disk (even though it isn't there yet)
	#  (3) We start writting message to disk
	#
	# Mark message as needing to be written.
	$self->{file_wheels}->{$message->{message_id}} = { write_message => 1 };

	# initiate file writting process
	$poe_kernel->post( $self->{session}, '_write_message_to_disk', $message, $body );

	# hand-off to the info storage
	$self->{info_storage}->store( $message );
}

sub remove
{
	my ($self, $message_id) = @_;

	if ( exists $self->{file_wheels}->{$message_id} )
	{
		if ( defined $self->{file_wheels}->{$message_id}->{write_message} )
		{
			$self->_log( 'debug', 'STORE: FILE: Removing message before we could start writting' );
			$self->{file_wheels}->{$message_id}->{write_message} = 0;
		}
		else
		{
			$self->_log( 'debug', "STORE: FILE: Stopping wheels for mesasge $message_id (deleting)" );
			
			my $infos = $self->{file_wheels}->{$message_id};
			my $wheel = $infos->{write_wheel} || $infos->{read_wheel};

			my $wheel_id = $wheel->ID();

			# stop the wheel
			$wheel->shutdown_input();
			$wheel->shutdown_output();

			# mark to actually delete message, but don't do it now, in order
			# to not leak FD's!
			$self->{file_wheels}->{$message_id}->{delete_me} = 1;
		}
	}
	else
	{
		# Actually delete the file, but *only* if there are no open wheels.
		my $fn = "$self->{data_dir}/msg-$message_id.txt";
		$self->_log( 'debug', "STORE: FILE: Deleting $fn" );
		unlink $fn || $self->_log( 'error', "STORE: FILE: Unable to remove $fn: $!" );
	}

	# remove from the info store
	$self->{info_storage}->remove( $message_id );
}

sub claim_and_retrieve
{
	my $self = shift;
	return $self->{info_storage}->claim_and_retrieve( @_ );
}

sub disown
{
	my ($self, $destination, $client_id ) = @_;
	return $self->{info_storage}->disown( $destination, $client_id );
}

sub shutdown
{
	my $self = shift;

	$self->{shutdown} = 1;

	# stop ALL message reading (at this point we only care
	# about finishing writing messages).
	my @message_ids = keys %{$self->{file_wheels}};
	foreach my $message_id ( @message_ids )
	{
		my $wheel = $self->{file_wheels}->{$message_id}->{read_wheel};

		if ( $wheel )
		{
			# stop the wheel
			$wheel->shutdown_input();
			$wheel->shutdown_output();

			# remove from the list, so that we can get a count
			# of how much writing is still left to do.
			delete $self->{file_wheels}->{$message_id};
		}
	}

	my $wheel_count = scalar keys %{$self->{file_wheels}};
	if ( $wheel_count == 0 )
	{
		# send shutdown to the info storage!
		$self->{info_storage}->shutdown();
	}
	else
	{
		$self->_log('alert', 'Waiting for messages to finish writing to disk...');
	}
}

#
# For handling responses from database:
#

sub _dispatch_message
{
	my ($self, $message, $destination, $client_id) = @_;

	if ( defined $message )
	{
		# check to see if we even finished writting to disk
		if ( defined $self->{file_wheels}->{$message->{message_id}} )
		{
			$self->_log( 'debug', "STORE: FILE: Returning message before in store: $message->{message_id}" );
			# attach the saved body to the message
			$message->{body} = $self->{file_wheels}->{$message->{message_id}}->{body};

			# NOTE: We don't stop writting, because if the message is not 
			# removed (ie. no ACK) we want it to get saved to disk.

			# distribute the message
			$self->{dispatch_message}->( $message, $destination, $client_id );
		}
		else
		{
			# pull the message body from disk
			$poe_kernel->post( $self->{session}, '_read_message_from_disk',
				$message, $destination, $client_id );
		}
	}
	else
	{
		$self->{dispatch_message}->( undef, $destination, $client_id );
	}
}

sub _shutdown_complete
{
	my $self = shift;

	# Ok!  This means that the info storage is totally shutdown, so we
	# are ready to kill our internal session.
	$poe_kernel->signal( $self->{session}, 'TERM' );

	if ( defined $self->{shutdown_complete} )
	{
		$self->{shutdown_complete}->();
	}
}

#
# For handling disk access
#

sub _write_message_to_disk
{
	my ($self, $kernel, $message, $body) = @_[ OBJECT, KERNEL, ARG0, ARG1 ];

	if ( not defined $self->{file_wheels}->{$message->{message_id}}->{write_message} )
	{
		$self->_log( 'emergency', "POE::Component::MessageQueue::Storage::FileSystem::_write_message_to_disk(): A wheel already exists for this messages $message->{message_id}!  This should never happen!" );
		return;
	}
	if ( not $self->{file_wheels}->{$message->{message_id}}->{write_message} )
	{
		$self->_log( 'debug', "STORE: FILE: Abort write of message $message->{message_id} to disk" );

		delete $self->{file_wheels}->{$message->{message_id}};

		return;
	}

	# setup the wheel
	my $fn = "$self->{data_dir}/msg-$message->{message_id}.txt";
	my $fh = IO::File->new( ">$fn" )
		|| die "Unable to save message in $fn: $!";
	my $wheel = POE::Wheel::ReadWrite->new(
		Handle       => $fh,
		Filter       => POE::Filter::Stream->new(),
		FlushedEvent => '_write_flushed_event'
	);

	# initiate the write to disk
	$wheel->put( $body );

	# stash the wheel in our maps
	$self->{file_wheels}->{$message->{message_id}} = {
		write_wheel => $wheel,
		body        => $body
	};
	$self->{wheel_to_message_map}->{$wheel->ID()} = $message->{message_id};
}

sub _read_message_from_disk
{
	my ($self, $kernel, $message, $destination, $client_id) = @_[ OBJECT, KERNEL, ARG0..ARG2 ];

	if ( defined $self->{file_wheels}->{$message->{message_id}} )
	{
		$self->_log( 'emergency', "POE::Component::MessageQueue::Storage::FileSystem::_read_message_from_disk(): A wheel already exists for this messages $message->{message_id}!  This should never happen!" );
		return;
	}

	# setup the wheel
	my $fn = "$self->{data_dir}/msg-$message->{message_id}.txt";
	my $fh = IO::File->new( $fn );
	
	$self->_log( 'debug', "STORE: FILE: Starting to read $fn from disk" );

	# if we can't find the message body.  This usually happens as a result
	# of crash recovery.
	if ( not defined $fh )
	{
		$self->_log( 'warning', "STORE: FILE: Can't find $fn on disk!  Discarding message." );

		# we simply discard the message
		$self->remove( $message->{message_id} );

		# we need to send a null message to this client to mark it is ready again (it is
		# waiting for a message).
		$self->{dispatch_message}->( undef, $destination, $client_id );

		return;
	}
	
	my $wheel = POE::Wheel::ReadWrite->new(
		Handle       => $fh,
		Filter       => POE::Filter::Stream->new(),
		InputEvent   => '_read_input',
		ErrorEvent   => '_read_error'
	);

	# stash the wheel in our maps
	$self->{file_wheels}->{$message->{message_id}} = {
		read_wheel  => $wheel,
		message     => $message,
		destination => $destination,
		client_id   => $client_id
	};
	$self->{wheel_to_message_map}->{$wheel->ID()} = $message->{message_id};
}

sub _read_input
{
	my ($self, $kernel, $input, $wheel_id) = @_[ OBJECT, KERNEL, ARG0, ARG1 ];

	if ( $self->{shutdown} )
	{
		# when shutting down, we don't care about reading.
		return;
	}

	my $message_id = $self->{wheel_to_message_map}->{$wheel_id};
	my $message    = $self->{file_wheels}->{$message_id}->{message};

	$message->{body} .= $input;
}

sub _read_error
{
	my ($self, $op, $errnum, $errstr, $wheel_id) = @_[ OBJECT, ARG0..ARG3 ];

	if ( $self->{shutdown} )
	{
		# when shutting down, we don't care about reading.
		return;
	}

	if ( $op eq 'read' and $errnum == 0 )
	{
		# EOF!  Our message is now totally assembled.  Hurray!

		my $message_id  = $self->{wheel_to_message_map}->{$wheel_id};
		my $infos       = $self->{file_wheels}->{$message_id};
		my $message     = $infos->{message};
		my $destination = $infos->{destination};
		my $client_id   = $infos->{client_id};

		$self->_log( 'debug', "STORE: FILE: Finished reading $self->{data_dir}/msg-$message_id.txt" );

		# send the message out!
		$self->{dispatch_message}->( $message, $destination, $client_id );

		# clear our state
		delete $self->{wheel_to_message_map}->{$wheel_id};
		delete $self->{file_wheels}->{$message_id};

		if ( $infos->{delete_me} )
		{
			# NOTE:  I have never seen this called, but it seems theoretically possible
			# and considering the former problem with leaking FD's, I'd rather keep this
			# here just in case.

			my $fn = "$self->{data_dir}/msg-$message_id.txt";
			$self->_log( 'debug', "STORE: FILE: Actually deleting $fn (on read error)" );
			unlink $fn || $self->_log( 'error', "Unable to remove $fn: $!" );
		}
	}
	else
	{
		$self->_log( 'error', "STORE: $op: Error $errnum $errstr" );
	}
}

sub _write_flushed_event
{
	my ($self, $kernel, $wheel_id) = @_[ OBJECT, KERNEL, ARG0 ];

	# remove from the first map
	my $message_id = delete $self->{wheel_to_message_map}->{$wheel_id};

	$self->_log( 'debug', "STORE: FILE: Finished writting message $message_id to disk" );

	# remove from the second map
	my $infos = delete $self->{file_wheels}->{$message_id};

	if ( $infos->{delete_me} )
	{
		# NOTE: If we were actively writting the file when the message to delete
		# came, we cannot actually delete it until the FD gets flushed, or the FD
		# will live until the program dies.

		my $fn = "$self->{data_dir}/msg-$message_id.txt";
		$self->_log( 'debug', "STORE: FILE: Actually deleting $fn (on write flush)" );
		unlink $fn || $self->_log( 'error', "Unable to remove $fn: $!" );
	}

	if ( $self->{shutdown} )
	{
		my $wheel_count = scalar keys %{$self->{file_wheels}};
		if ( $wheel_count == 0 )
		{
			# send shutdown to the info storage!
			$self->{info_storage}->shutdown();
		}
	}
}

sub _log_state
{
	my ($self, $kernel) = @_[ OBJECT, KERNEL ];

	my $wheel_count = scalar keys %{$self->{file_wheels}};
	$self->_log('debug', "STORE: FILE: Currently there are $wheel_count wheels in action.");

	my $wheel_to_message_map = Dumper($self->{wheel_to_message_map});
	$wheel_to_message_map =~ s/\n//g;
	$wheel_to_message_map =~ s/\s+/ /g;
	$self->_log('debug', "STORE: FILE: wheel_to_message_map: $wheel_to_message_map");

	while ( my ($key, $value) = each %{$self->{file_wheels}} )
	{
		my %tmp = ( %$value );
		$tmp{write_wheel} = "$tmp{write_wheel}" if exists $tmp{write_wheel};
		$tmp{read_wheel}  = "$tmp{read_wheel}"  if exists $tmp{read_wheel};

		my $wheel = Dumper(\%tmp);
		$wheel =~ s/\n//g;
		$wheel =~ s/\s+/ /g;
		
		$self->_log('debug', "STORE: FILE: wheel ($key): $wheel");
	}

	$kernel->delay_set('_log_state', 5);
}

1;

__END__

=pod

=head1 NAME

POE::Component::MessageQueue::Storage::FileSystem -- A storage engine that keeps message bodies on the filesystem

=head1 SYNOPSIS

  use POE;
  use POE::Component::MessageQueue;
  use POE::Component::MessageQueue::Storage::FileSystem;
  use POE::Component::MessageQueue::Storage::DBI;
  use strict;

  # For mysql:
  my $DB_DSN      = 'DBI:mysql:database=perl_mq';
  my $DB_USERNAME = 'perl_mq';
  my $DB_PASSWORD = 'perl_mq';

  POE::Component::MessageQueue->new({
    storage => POE::Component::MessageQueue::Storage::FileSystem->new({
      info_storage => POE::Component::MessageQueue::Storage::DBI->new({
        dsn      => $DB_DSN,
        username => $DB_USERNAME,
        password => $DB_PASSWORD,
      }),
      data_dir => $DATA_DIR,
    })
  });

  POE::Kernel->run();
  exit;

=head1 DESCRIPTION

A storage engine that wraps around another storage engine in order to store the message bodies on the file system.  The other message properties are stored with the wrapped storage engine.

While I would argue that using this module is less efficient than using
L<POE::Component::MessageQueue::Storage::Complex>, using it directly would make sense if
persistance was your primary concern.  All messages stored via this backend will be
persistent regardless of whether they have the persistent flag set or not.  Every message
is stored, even if it is handled right away and will be removed immediately after
having been stored.

=head1 CONSTRUCTOR PARAMETERS

=over 2

=item info_storage => L<POE::Component::MessageQueue::Storage>

The storage engine used to store message properties.

=item data_dir => SCALAR

The directory to store the files containing the message body's.

=back

=head1 SEE ALSO

L<DBI>,
L<POE::Component::EasyDBI>,
L<POE::Component::MessageQueue>,
L<POE::Component::MessageQueue::Storage>,
L<POE::Component::MessageQueue::Storage::DBI>,
L<POE::Component::MessageQueue::Storage::Memory>,
L<POE::Component::MessageQueue::Storage::Generic>,
L<POE::Component::MessageQueue::Storage::Generic::DBI>,
L<POE::Component::MessageQueue::Storage::Throttled>,
L<POE::Component::MessageQueue::Storage::Complex>

=cut

