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

package POE::Component::MessageQueue::Storage::Memory;
use Moose;
with qw(POE::Component::MessageQueue::Storage);

# destination => @messages
has 'messages' => (is => 'ro', default => sub { {} });
has 'claims' => (is => 'ro', default => sub { {} });

make_immutable;

sub store
{
	my ($self, $messages, $callback) = @_;

	# push onto our array
	foreach my $msg (@$messages)
	{
		my $destination = $msg->destination;
		my $aref = ($self->messages->{$destination} ||= []);
		push(@$aref, $msg);
		$self->log('info', sprintf('Added %s', $msg->id));
	}

	goto $callback if $callback;
}

sub _msg_foreach
{
	my ($self, $action) = @_;
	foreach my $messages_in_dest (values %{$self->messages})
	{
		foreach my $message (@$messages_in_dest)
		{
			$action->($message);
		}
	}
}

sub _msg_foreach_ids
{
	my ($self, $ids, $action) = @_;
	my %id_hash = map { ($_, 1) } (@$ids);
	$self->_msg_foreach(sub {
		my $msg = $_[0];
		$action->($msg) if (exists $id_hash{$msg->id});
	});
}

sub get
{
	my ($self, $ids, $callback) = @_;
	my @messages;
	$self->_msg_foreach_ids($ids, sub {push(@messages, $_[0])});
	@_ = (\@messages);
	goto $callback;
}

sub get_all
{
	my ($self, $callback) = @_;
	my @messages;
	$self->_msg_foreach(sub {push(@messages, $_[0])});
	@_ = (\@messages);
	goto $callback;
}

sub claim_next
{
	my ($self, $destination, $client_id, $callback) = @_;
	my $oldest;
	foreach my $msg (@{$self->messages->{$destination}} || ())
	{
		unless ($msg->claimed || 
		        ($oldest && $oldest->timestamp < $msg->timestamp))
		{
			$oldest = $msg;
		}
	}
	$self->_claim_it_yo($oldest, $client_id) if $oldest;
	@_ = ($oldest);
	goto $callback;
}

sub get_oldest
{
	my ($self, $callback) = @_;
	my $oldest;
	$self->_msg_foreach(sub {
		my $msg = shift;
		$oldest = $msg unless ($oldest && ($oldest->timestamp < $msg->timestamp));
	});
	@_ = ($oldest);
	goto $callback;
}

sub get_by_client
{
	my ($self, $client_id, $callback) = @_;
	my @messages;
	$self->_msg_foreach(sub {
		my $msg = shift;
		push(@messages, $msg) if ($msg->claimant eq $client_id);
	});
	@_ = (\@messages);
	goto $callback;
}

sub remove
{
	my ($self, $message_ids, $callback) = @_;
	# Stuff IDs into a hash so we can quickly check if a message is on the list
	my %id_hash = map { ($_, 1) } (@$message_ids);

	foreach my $messages (values %{$self->messages})
	{
		my $max = scalar @{$messages};

		for ( my $i = 0; $i < $max; $i++ )
		{
			my $message = $messages->[$i];
			# Check if this messages is in the "remove" list
			next unless exists $id_hash{$message->id};
			splice @$messages, $i, 1;
			$i--; $max--;
		}
	}

	goto $callback if $callback;
}

sub empty 
{
	my ($self, $callback) = @_;
	%{$self->messages} = ();
	%{$self->claims} = ();
	goto $callback if $callback;
}

sub _claim_it_yo
{
	my ($self, $msg, $client_id) = @_;;
	$msg->claim($client_id);
	$self->log('info', sprintf('Message %s claimed by client %s',
		$msg->id, $client_id));
}

sub claim
{
	my ($self, $ids, $client_id, $callback) = @_;

	$self->_msg_foreach_ids($ids, sub {
		$self->_claim_it_yo($_[0], $client_id);
	});

	goto $callback if $callback;
}

sub disown
{
	my ($self, $ids, $callback) = @_;
	$self->_msg_foreach_ids($ids, sub {
		my $msg = shift;
		$msg->disown();
	});
	goto $callback if $callback;
}

sub storage_shutdown
{
	my ($self, $callback) = @_;
	goto $callback if $callback;
}

1;

__END__

=pod

=head1 NAME

POE::Component::MessageQueue::Storage::Memory -- In memory storage engine.

=head1 SYNOPSIS

  use POE;
  use POE::Component::MessageQueue;
  use POE::Component::MessageQueue::Storage::Memory;
  use strict;

  POE::Component::MessageQueue->new({
    storage => POE::Component::MessageQueue::Storage::Memory->new()
  });

  POE::Kernel->run();
  exit;

=head1 DESCRIPTION

A storage engine that keeps all the messages in memory.  Provides no persistence
what-so-ever.

For an alternative in-memory storage engine optimized for a large number of 
messages, please see L<POE::Component::MessageQueue::Storage::Memoray>.

I wouldn't suggest using this as your main storage engine because if messages aren't
removed by consumers, it will continue to consume more memory until it explodes.  Check-out
L<POE::Component::MessageQueue::Storage::Complex> which uses this module internally to keep
messages in memory for a period of time before moving them into persistent storage.

=head1 CONSTRUCTOR PARAMETERS

None to speak of!

=head1 SEE ALSO

L<POE::Component::MessageQueue::Storage::BigMemory> -- Alternative memory-based storage engine.

L<POE::Component::MessageQueue>,
L<POE::Component::MessageQueue::Storage>

I<Other storage engines:>

L<POE::Component::MessageQueue::Storage::BigMemory>,
L<POE::Component::MessageQueue::Storage::FileSystem>,
L<POE::Component::MessageQueue::Storage::DBI>,
L<POE::Component::MessageQueue::Storage::Generic>,
L<POE::Component::MessageQueue::Storage::Generic::DBI>,
L<POE::Component::MessageQueue::Storage::Throttled>,
L<POE::Component::MessageQueue::Storage::Complex>,
L<POE::Component::MessageQueue::Storage::Default>

=cut
