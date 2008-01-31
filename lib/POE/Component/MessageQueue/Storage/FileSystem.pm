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
use Moose;

use POE::Kernel;
use POE::Session;
use POE::Filter::Stream;
use POE::Wheel::ReadWrite;
use IO::File;

has 'info_store' => (
	is       => 'ro',
	required => 1,	
	does     => qw(POE::Component::MessageQueue::Storage),
	handles  => [qw(disown)],
);

# The role application must come after the has, or it won't pick up the
# delegation.
with qw(POE::Component::MessageQueue::Storage);

has 'data_dir' => (
	is       => 'ro',
	isa      => 'Str',
	required => 1,
);

use constant empty_hashref => (
	is       => 'ro',
	isa      => 'HashRef',
	default  => sub{ {} },
);
has 'file_wheels'          => empty_hashref;
has 'wheel_to_message_map' => empty_hashref;
has 'pending_writes'       => empty_hashref;

has 'shutdown_callback' => (
	is        => 'rw',
	predicate => 'shutting_down',
);

has 'alias' => (
	is       => 'ro',
	isa      => 'Str',
  default  => 'MQ-Storage-Filesystem',
	required => 1,
);

has 'session' => (
	is      => 'ro',
	isa     => 'POE::Session',
	default => sub {
		my $self = shift;
		POE::Session->create(
			inline_states => {
				_start => sub {
					$_[KERNEL]->alias_set($self->alias)
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
			],
		);
	},
);

override 'new' => sub {
	my $self = super();
	$self->children({INFO => $self->info_store});
	return $self;
};

after 'set_logger' => sub {
	my ($self, $logger) = @_;
	$self->info_store->set_logger($logger);
};

sub store
{
	my ($self, $message, $callback) = @_;

	# Grab the body and delete it from the message
	my $body = delete $message->{body};

	# DRS: To avaid a race condition where:
	#
	#  (1) We post _write_message_to_disk
	#  (2) Message is "removed" from disk (even though it isn't there yet)
	#  (3) We start writing message to disk
	#
	# Mark message as needing to be written.
	$self->pending_writes->{$message->{message_id}} = $body;

	# initiate file writing process (only the body will be written)
	$poe_kernel->post( $self->session, '_write_message_to_disk', 
		$message, $body );

	# hand-off the rest of the message to the info storage
	$self->info_store->store($message, $callback);
}

sub _get_filename
{
	my ($self, $message_id) = @_;
	return sprintf('%s/msg-%s.txt', $self->data_dir, $message_id);
}

sub _hard_delete
{
	my ($self, $id) = @_; 
	
	# Just unlink it unless there are pending writes
	return $self->_unlink_file($id) unless delete $self->pending_writes->{$id};

	my $info = $self->file_wheels->{$id};
	if ($info) 
	{
		$self->log('debug', "Stopping wheels for message $id (removing)");
		my $wheel = $info->{write_wheel} || $info->{read_wheel};
		$wheel->shutdown_input();
		$wheel->shutdown_output();

		# Mark for deletion: we'll detect this primarly in 
		# _write_flushed_event and unlink the file at that time 
		# (prevents a file descriptor leak)
		$info->{delete_me} = 1;
	}
	else
	{
		# If we haven't started yet, _write_message_to_disk will just abort.
		$self->log('debug', "Removing message $id before writing started");
	}
}

sub _unlink_file
{
	my ($self, $message_id) = @_;
	my $fn = $self->_get_filename($message_id);
	$self->log( 'debug', "Deleting $fn" );
	unlink $fn || 
		$self->log( 'error', "Unable to remove $fn: $!" );
	return;
}

# A word to the wise: the remove functions make heavy use of continuation
# passing style to deal with the async nature of getting data out of stores in
# this API.  If you don't understand CPS, read about it before you mess with
# this or you'll break it!
sub _remove_underneath
{
	my ($self, $remover, $callback) = @_;

	# Remover does the info-storage remove call and sends us an aref of messages
	# (if necessary - if $callback wasn't specified, then we just discard them).
	$remover->($callback && sub {
		my $messages = shift;
		my @output = ();
		my $max = scalar (@$messages);

		# We loop by recursing so that we can get to the next iteration from a
		# callback in case of needing to read from disk.  Isn't CPS fun?
		# You can pretend this is a for loop, if it makes you feel better.
		my $loop = sub {
			# We can't refer to $loop while we're defining it, so we'll have to pass
			# it in as a paremeter.  Is your mind bent yet?
			my ($recurse, $index) = @_;

			# This is where the loop ends and we have a result.
			return $callback->(\@output) unless $index < $max;

			my $message = $messages->[$index];
			my $id = $message->{message_id};
			my $body = $self->pending_writes->{$id};
			if ($body) 
			{
				$self->_hard_delete($id);
				$message->{body} = $body;
				push(@output, $message);
				$recurse->($recurse, $index++)
			}
			else
			{
				# Don't have the body any more, so read from disk.
				$poe_kernel->post($self->session, '_read_message_from_disk', $id, 
					sub { 
						$body = shift;
						$message->{body} = $body;
						# If didn't get a body, there wasn't one to get.
						if ($body)
						{
							push(@output, $message);
							$self->_unlink_file($id);
						}
						$recurse->($recurse, $index++);
						return;
					},
				);
			}
		};
		$loop->($loop, 0);
		return;
	});
}

sub remove
{
	my ($self, $message_id, $callback) = @_;
	
	# Kill the file if we're not going to care what's in it
	$self->_hard_delete($message_id) unless $callback;

	$self->_remove_underneath(sub {
		# remover: Remove single message and pack it up in an aref 
		my $send = shift;
	
		$self->info_store->remove($message_id, $send && sub {
			my $message = shift;
			$send->([$message]);
		});
	}, $callback && sub {
		# Unpack from aref and send along
		my $result = shift;
		my $val = scalar @{$result} ? $result->[0] : undef;
		$callback->($val);
	});
}

# These two expect to receive and send arefs, so we don't need to instrument
# them like we did with remove.

sub remove_multiple
{
	my ($self, $message_ids, $callback) = @_;

	unless ($callback)
	{
		$self->_hard_delete($_) foreach (@$message_ids);
	}

	$self->_remove_underneath(sub {
		$self->info_store->remove_multiple($message_ids, shift)
	}, $callback);
}

sub remove_all
{
	my ($self, $callback) = @_;

	unless ($callback)
	{
		# Delete all the message files that don't have writes pending
		use DirHandle;
		my $dh = DirHandle->new($self->data_dir);
		foreach my $fn ($dh->read())
		{
			if ($fn =~ /msg-\(.*\)\.txt/)
			{
				my $id = $1;
				$self->_unlink_file($id) unless exists $self->pending_writes->{$id};	
			}
		}
		# Do the special dance for deleting those that are pending
		$self->_hard_delete($_) foreach (keys %{$self->pending_writes});
	}

	$self->_remove_underneath(sub {
		$self->info_store->remove_all(shift);
	}, $callback);	
}

sub claim_and_retrieve
{
	my ($self, @args) = @_;
	my $old_callback = pop(@args);
	my $new_callback = sub { $self->_dispatch_message(@_, $old_callback) };
	return $self->info_store->claim_and_retrieve(@args, $new_callback);
}

sub storage_shutdown
{
	my ($self, $complete) = @_;

	$self->shutdown_callback(sub {
		$self->info_store->storage_shutdown(sub {
			$poe_kernel->signal($self->session, 'TERM');
			$complete->();
		});
	});

	$self->_wheel_check();
}

#
# For handling responses from database:
#
sub _dispatch_message
{
	my ($self, $message, $destination, $client_id, $dispatch) = @_;

	return $dispatch->(undef, $destination, $client_id) unless $message;

	my $id = $message->{message_id};
	my $body = $self->pending_writes->{$id};

	# check to see if we even finished writing to disk
	if ( $body )
	{
		$self->log('debug', "Dispatching message $id before disk write"); 
		$message->{body} = $body;

		# We don't stop writing, because if the message is not ACK'd,
		# we want it to get saved to disk.

		$dispatch->($message, $destination, $client_id);
	}
	else
	{
		# pull the message body from disk
		$poe_kernel->post($self->session, '_read_message_from_disk', $id, sub {
			my $body = shift;
			if ($body)
			{
				$message->{body} = $body;
			}
			else
			{
				$self->log('warning', "Can't find message $id!  Discarding"); 
				$self->remove($id);
				$message = undef;
			}

			$dispatch->($message, $destination, $client_id); 
		});
	}
}

#
# For handling disk access
#
sub _write_message_to_disk
{
	my ($self, $kernel, $message, $body) = @_[ OBJECT, KERNEL, ARG0, ARG1 ];
	my $id = $message->{message_id};

	if ($self->file_wheels->{$id})
	{
		my $here = __PACKAGE__.'::_write_message_to_disk()';
		$self->log('emergency',
			"$here: wheel already exists for message $id! This shouldn't happen!");
		return;
	}
	 
	unless ($self->pending_writes->{$id})
	{
		$self->log('debug', "Abort write of message $id to disk");
		delete $self->file_wheels->{$id};
		return;
	}

	# Yes, we do want to die if we can't open the file for writing.  It
	# means something is wrong and it's very unlikely we can persist other
	# messages.
	my $fn = $self->_get_filename($id);
	my $fh = IO::File->new( ">$fn" ) || die "Unable to save message in $fn: $!";

	my $wheel = POE::Wheel::ReadWrite->new(
		Handle       => $fh,
		Filter       => POE::Filter::Stream->new(),
		FlushedEvent => '_write_flushed_event'
	);

	# initiate the write to disk
	$wheel->put( $body );

	# stash the wheel in our maps
	$self->file_wheels->{$id} = {write_wheel => $wheel};

	$self->wheel_to_message_map->{$wheel->ID()} = $id;
}

sub _read_message_from_disk
{
	my ($self, $kernel, $id, $callback) = @_[ OBJECT, KERNEL, ARG0, ARG1 ];

	if ($self->file_wheels->{$id})
	{
		my $here = __PACKAGE__.'::_read_message_from_disk()';
		$self->log('emergency',
			"$here: A wheel already exists for this message ($id)! ".
			'This should never happen!'
		);
		return;
	}

	# setup the wheel
	my $fn = $self->_get_filename($id);
	my $fh = IO::File->new( $fn );
	
	$self->log( 'debug', "Starting to read $fn from disk" );

	# if we can't find the message body.  This usually happens as a result
	# of crash recovery.
	return $callback->(undef) unless ($fh);
	
	my $wheel = POE::Wheel::ReadWrite->new(
		Handle       => $fh,
		Filter       => POE::Filter::Stream->new(),
		InputEvent   => '_read_input',
		ErrorEvent   => '_read_error'
	);

	# stash the wheel in our maps
	$self->file_wheels->{$id} = {
		read_wheel  => $wheel,
		accumulator => q{},
		callback    => $callback
	};
	$self->wheel_to_message_map->{$wheel->ID()} = $id;
}

sub _read_input
{
	my ($self, $kernel, $input, $wheel_id) = @_[ OBJECT, KERNEL, ARG0, ARG1 ];

	# We do care about reading during shutdown! Maybe.  We may be using this as
	# a front-store (HA!), and doing remove_all.

	my $id = $self->wheel_to_message_map->{$wheel_id};
	$self->file_wheels->{$id}->{accumulator} .= $input;	
}

sub _read_error
{
	my ($self, $op, $errnum, $errstr, $wheel_id) = @_[ OBJECT, ARG0..ARG3 ];

	if ( $op eq 'read' and $errnum == 0 )
	{
		# EOF!  Our message is now totally assembled.  Hurray!

		my $id       = $self->wheel_to_message_map->{$wheel_id};
		my $info     = $self->file_wheels->{$id};
		my $body     = $info->{accumulator};
		my $callback = $info->{callback};

		my $fn = $self->_get_filename($id);
		$self->log('debug', "Finished reading $fn");

		# clear our state
		delete $self->wheel_to_message_map->{$wheel_id};
		delete $self->file_wheels->{$id};

		# NOTE:  I have never seen this happen, but it seems theoretically 
		# possible.  Considering the former problem with leaking FD's, I'd 
		# rather keep this here just in case.
		$self->_unlink_file($id) if ($info->{delete_me});

		# send the message out!
		$callback->($body);

		# Might be time to shut down.
		$self->_wheel_check();
	}
	else
	{
		$self->log( 'error', "$op: Error $errnum $errstr" );
	}
}

sub _wheel_check
{
	my $self = shift;
	if ($self->shutting_down)
	{
		if (scalar keys %{$self->file_wheels})
		{
			$self->log('alert', 'Waiting for disk...');
		}
		else
		{
			$self->shutdown_callback->();
		}
	}
}

sub _write_flushed_event
{
	my ($self, $kernel, $wheel_id) = @_[ OBJECT, KERNEL, ARG0 ];

	# remove from the first map
	my $id = delete $self->wheel_to_message_map->{$wheel_id};

	$self->log( 'debug', "Finished writing message $id to disk" );

	# remove from the second map
	my $info = delete $self->file_wheels->{$id};

	# Write isn't pending anymore. :)
	delete $self->pending_writes->{$id};

	# If we were actively writing the file when the message to delete
	# came, we cannot actually delete it until the FD gets flushed, or the FD
	# will live until the program dies.
	$self->_unlink_file($id) if ($info->{delete_me});

	# Shutdown if wheels are done
	$self->_wheel_check();
}

sub _log_state
{
	my ($self, $kernel) = @_[ OBJECT, KERNEL ];

	use Data::Dumper;

	my $wheel_count = scalar keys %{$self->file_wheels};
	$self->log('debug', "Currently there are $wheel_count wheels in action.");

	my $wheel_to_message_map = Dumper($self->wheel_to_message_map);
	$wheel_to_message_map =~ s/\n//g;
	$wheel_to_message_map =~ s/\s+/ /g;
	$self->log('debug', "wheel_to_message_map: $wheel_to_message_map");

	while ( my ($key, $value) = each %{$self->file_wheels} )
	{
		my %tmp = ( %$value );
		$tmp{write_wheel} = "$tmp{write_wheel}" if exists $tmp{write_wheel};
		$tmp{read_wheel}  = "$tmp{read_wheel}"  if exists $tmp{read_wheel};

		my $wheel = Dumper(\%tmp);
		$wheel =~ s/\n//g;
		$wheel =~ s/\s+/ /g;
		
		$self->log('debug', "wheel ($key): $wheel");
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
      info_store => POE::Component::MessageQueue::Storage::DBI->new({
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

=item info_store => L<POE::Component::MessageQueue::Storage>

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

