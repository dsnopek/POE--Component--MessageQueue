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

package POE::Component::MessageQueue::Storage::Throttled;
use Moose;
with qw(POE::Component::MessageQueue::Storage::Double);
use POE;

has 'throttle_max' => (
	is       => 'ro',
	isa      => 'Int',
	default  => 2,
	required => 1,
);

has 'sent' => (
	isa     => 'Int',
	default => 0,
);

has 'queue' => (
	is      => 'ro',
  isa     => 'POE::Component::MessageQueue::Storage::Structure::DLList',
	default => sub {
		POE::Component::MessageQueue::Storage::Structure::DLList->new();
	},
);

has 'messages' => (
	is  => 'ro',
	isa => 'HashRef',
	default => sub { {} },
);

has 'shutdown_callback' => (
	is        => 'rw',
	isa       => 'CodeRef',
	clearer   => 'disable_shutdown',
	predicate => 'shutting_down',
);

override 'new' => sub {
	my $self = super();
	$self->children({THROTTLED => $self->front, STORAGE => $self->back});
	$self->add_names qw(THROTTLED);
	return $self;
};

sub _message_stored
{
	my ($self, $message, $callback) = @_;

	$callback->($message) if $callback;

	# Send the next throttled message off to the backing store.
	if (my $msg = $self->queue->shift())
	{
		my $id = $msg->{message_id};
		delete $self->messages->{$id};	

		my $count = keys %{$self->messages};
		$self->log('info', "Sending throttled message ($count left)");

		$self->front->remove($id, sub {
			my $message = shift;
			$self->back->store($message, sub {
				$self->_message_stored($message, $callback);
			});	
		});
	}
	else
	{
		$self->{sent}--;
		$self->_shutdown_throttle_check();
	}
}

sub throttle_remove
{
	my ($self, $id) = @_;
	my $cell = delete $self->messages->{$id};
	$cell->delete() if $cell;
}

after 'remove' => sub {
	my ($self, $id) = @_;
	$self->throttle_remove($id);
};

after qw(remove_multiple remove_all) => sub {
	my ($self, $aref) = @_;
	$self->throttle_remove($_) foreach (@$aref);
};

sub store
{
	my ($self, $message, $callback) = @_;

	if ($self->{sent} < $self->throttle_max)
	{
		$self->back->store($message, sub {
			$self->_message_stored(shift, $callback);
		});
		$self->{sent}++;
	}
	else
	{
		$self->front->store($message, sub {
			$self->messages->{$message->{message_id}} = $self->queue->push($message);
			$self->log('info', sprintf('Throttling (Total throttled: %d)',
				scalar keys %{$self->messages}));
		});
	}
}

sub _shutdown_throttle_check
{
	my $self = shift;
	if ($self->shutting_down && (scalar keys %{$self->messages}) == 0)
	{
		# We have now finished sending things out of throttled, so -WE- are done.
		# However, we'll still get message_storeds as our backstore finishes, and
		# we don't want to continue calling shutdown_callback.
		$self->shutdown_callback->();
		$self->disable_shutdown();
	}
	return;
}

sub storage_shutdown
{
	my ($self, $complete) = @_;
	$self->shutdown_callback(sub {
		$self->back->storage_shutdown($complete);
	});

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

=item back => L<POE::Component::MessageQueue::Storage>

The storage engine to wrap.

=item throttle_max => SCALAR

The max number of messages that can be sent to the DBI store at one time.

=back

=head1 SEE ALSO

L<DBI>,
L<DBD::SQLite>,
L<POE::Component::MessageQueue>,
L<POE::Component::MessageQueue::Storage>,
L<POE::Component::MessageQueue::Storage::Memory>,
L<POE::Component::MessageQueue::Storage::FileSystem>,
L<POE::Component::MessageQueue::Storage::DBI>,
L<POE::Component::MessageQueue::Storage::Generic>,
L<POE::Component::MessageQueue::Storage::Generic::DBI>,
L<POE::Component::MessageQueue::Storage::Complex>

=cut

