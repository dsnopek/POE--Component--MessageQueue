#
# Copyright 2007, 2008 David Snopek <dsnopek@gmail.com>
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

package POE::Component::MessageQueue::Storage::Throttled;
use base qw(POE::Component::MessageQueue::Storage);

use POE;
use strict;

use Data::Dumper;

sub new
{
	my $class = shift;
	my $args  = shift;

	my $storage;
	my $throttle_max = 2;

	if ( ref($args) eq 'HASH' )
	{
		$storage      = $args->{storage};
		$throttle_max = $args->{throttle_max} if exists $args->{throttle_max};
	}
	else
	{
		$storage = $args;
	}

	my $self = $class->SUPER::new( $args );

	$self->{storage} = $storage;

	# for throttling data to the engine
	$self->{throttle_buffer} = { };
	$self->{throttle_order}  = [ ];
	$self->{throttle_max}    = $throttle_max;
	$self->{throttle_count}  = 0;

	# a flag for shutting down
	$self->{shutdown} = 0;

	return bless $self, $class;
}

sub set_logger
{
	my ($self, $logger) = @_;
	$self->SUPER::set_logger( $logger );
	$self->{storage}->set_logger( $logger );
}

sub _throttle_push
{
	my ($self, $message) = @_;

	# stash in an ordered-lookup kind of way
	$self->{throttle_buffer}->{$message->{message_id}} = $message;
	push @{$self->{throttle_order}}, $message->{message_id};
}

sub _throttle_pop
{
	my ($self) = @_;

	while ( scalar @{$self->{throttle_order}} > 0 )
	{
		my $message_id = shift @{$self->{throttle_order}};

		# if there is still a message with that id in the buffer
		# then return it.
		if ( exists $self->{throttle_buffer}->{$message_id} )
		{
			return delete $self->{throttle_buffer}->{$message_id};
		}
	}

	undef;
}

sub _message_stored
{
	my ($self, $message, $callback) = @_;

	# first, check if there are any throttled messages we can now push to
	# the underlying storage engine.
	if ( $self->{throttle_max} )
	{
		my $to_store = $self->_throttle_pop();
		if ( $to_store )
		{
			my $count = (scalar @{$self->{throttle_order}});

			# if we have a throttled message then send it!
			$self->_log('STORE: Sending throttled message from the buffer to the '. 
			            "storage engine.  (Total throttled: $count)");
			$self->{storage}->store($to_store, sub {
				$self->_message_stored(shift, $callback);
			});
		}
		else
		{
			# else, simple decrease the throttle count
			$self->{throttle_count}--;
		}
	}

	# Then, call the user handler!
	$callback->($message) if $callback;

	# if we are shutting down and there are no more message throttled, then
	# we shutdown the underlying engine.
	$self->_shutdown_throttle_check();
}

sub store
{
	my ($self, $message, $callback) = @_;

	if ( $self->{throttle_max} )
	{
		if ( $self->{throttle_count} >= $self->{throttle_max} )
		{
			my $c = (scalar @{$self->{throttle_order}}) + 1;

			$self->_log("STORE: THROTTLED: Already have sent $self->{throttle_max} messages to store engine.  Throttling.  Message will be buffered until engine has stored some messages.  (Total throttled: $c)");

			# push into buffer
			$self->_throttle_push($message);
			
			# don't send, yet!
			return;
		}
		else
		{
			# increment so we know that another message was sent to the 
			# underlying engine.
			$self->{throttle_count} ++;
		}
	}

	# We have our own routine to run after the message is stored.
	$self->{storage}->store($message, sub {
		$self->_message_stored(shift, $callback);
	});
}

sub remove
{
	my ($self, $message_id, $callback) = @_;
	my $message = delete $self->{throttle_buffer}->{$message_id};
	if ($message)
	{
		$callback->($message) if $callback;
	}
	else
	{
		$self->{storage}->remove($message_id, $callback);
	}
}

sub remove_multiple
{
	my ($self, $message_ids, $callback) = @_;
	my @result = ();
	my @in_storage = ();
	# Collect the ones in the throttle buffer
	foreach my $id (@$message_ids) {
		my $message = delete $self->{throttle_buffer}->{$id};
		if ($message)
		{
			push(@result, $message);
		}
		else
		{
			push(@in_storage, $id);
		}
	}
	
	# Pull the rest out of real storage, if there is any such thing as "rest"
	if (scalar @in_storage)
	{
		$self->{storage}->remove_multiple(\@in_storage, $callback && sub {
			my $aref = shift;
			push(@result, @$aref);
			$callback->(\@result);
		});
	}
	# If there was stuff in storage, the callback has already been handled.
	elsif ($callback)
	{
		$callback->(\@result);
	}
	return;
}

sub remove_all
{
	my ($self, $callback) = @_;
	my @result = ();
	push(@result, values %{$self->{throttle_buffer}} );
	$self->{storage}->remove_all($callback && sub {
		my $aref = shift;
		push(@result, @$aref);
		$callback->(\@result);
	});
	return;
}

sub claim_and_retrieve
{
	return shift->{storage}->claim_and_retrieve(@_);
}

sub disown
{
	return shift->{storage}->disown(@_);
}

sub _shutdown_throttle_check
{
	my $self = shift;
	my $shutdown = $self->{shutdown};
	$shutdown->() if ($shutdown && $self->{throttle_count} == 0);
	return;
}

sub storage_shutdown
{
	my ($self, $complete) = @_;

	$self->{shutdown} = sub {
		$self->{storage}->storage_shutdown($complete);
	};

	$self->_shutdown_throttle_check();
}

1;

__END__

=pod

=head1 NAME

POE::Component::MessageQueue::Storage::Throttled -- Wraps around another storage engine to throttle the number of messages sent to be stored at one time.

=head1 SYNOPSIS

  use POE;
  use POE::Component::MessageQueue;
  use POE::Component::MessageQueue::Storage::Throttled;
  use POE::Component::MessageQueue::Storage::DBI;
  use strict;

  my $DATA_DIR = '/tmp/perl_mq';

  POE::Component::MessageQueue->new({
    storage => POE::Component::MessageQueue::Storage::Throttled->new({
      storage => POE::Component::MessageQueue::Storage::DBI->new({
        dsn      => $DB_DSN,
        username => $DB_USERNAME,
        password => $DB_PASSWORD,
      }),
      throttle_max => 2
    }),
  });

  POE::Kernel->run();
  exit;

=head1 DESCRIPTION

Wraps around another engine to limit the number of messages sent to be stored at once.

Use of this module is B<highly> recommend!

If the storage engine is unable to store the messages fast enough (ie. with slow disk IO) it can get really backed up and stall messages coming out of the queue.  This allows a client producing execessive amounts of messages to basically monopolize the server, preventing any messages from getting distributed to subscribers.

It is suggested to keep the throttle_max very low.  In an ideal situation, the underlying storage engine would be able to write each message immediately.  This means that there will never be more than one message sent to be stored at a time.  The purpose of this module is make the message act as though this were the case even if it isn't.  So, a throttle_max of 1, will strictly enforce this, however, for a little bit of leniancy, the suggested default is 2.

=head1 CONSTRUCTOR PARAMETERS

=over 2

=item storage => L<POE::Component::MessageQueue::Storage>

The storage engine to wrap.

=item throttle_max => SCALAR

The max number of messages that can be sent to the DBI store at one time.

=back

=head1 SEE ALSO

L<POE::Component::MessageQueue>,
L<POE::Component::MessageQueue::Storage>

I<Other storage engines:>

L<POE::Component::MessageQueue::Storage::Memory>,
L<POE::Component::MessageQueue::Storage::BigMemory>,
L<POE::Component::MessageQueue::Storage::FileSystem>,
L<POE::Component::MessageQueue::Storage::DBI>,
L<POE::Component::MessageQueue::Storage::Generic>,
L<POE::Component::MessageQueue::Storage::Generic::DBI>,
L<POE::Component::MessageQueue::Storage::Complex>,
L<POE::Component::MessageQueue::Storage::Default>

=cut

