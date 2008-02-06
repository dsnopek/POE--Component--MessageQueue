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

package POE::Component::MessageQueue::Storage::Memory;
use Moose;
with qw(POE::Component::MessageQueue::Storage);

# destination => @messages
has 'messages' => (is => 'ro', default => sub { {} });

sub store
{
	my ($self, $message, $callback) = @_;
	my $destination = $message->destination;

	# push onto our array
	my $aref = ($self->messages->{$destination} ||= []);
	push(@$aref, $message);
	$self->log('info', sprintf('Added %s', $message->id));

	$callback->($message) if $callback;
}

sub remove
{
	my ($self, $message_id, $callback) = @_;
	my $removed;

	OUTER: foreach my $dest ( keys %{$self->messages} )
	{
		my $messages = $self->messages->{$dest};
		my $max = scalar @$messages;

		# find the message and remove it
		for ( my $i = 0; $i < $max; $i++ )
		{
			my $message = $messages->[$i];
			if ( $message->id eq $message_id )
			{
				splice(@$messages, $i, 1);
				$self->log('info', "Removed $message_id");
				$removed = $message;
				last OUTER;
			}
		}
	}

	$callback->($removed) if $callback;
	return;
}

sub remove_multiple
{
	my ($self, $message_ids, $callback) = @_;
	my @removed = ();
	# Stuff IDs into a hash so we can quickly check if a message is on the list
	my %id_hash = map { ($_, 1) } (@$message_ids);

	while ( my ($dest, $messages) = each %{$self->messages} ) 
	{
		my $max = scalar @{$messages};

		for ( my $i = 0; $i < $max; $i++ )
		{
			my $message = $messages->[$i];
			# Check if this messages is in the "remove" list
			next unless exists $id_hash{$message->id};
			splice @$messages, $i, 1;
			$i--; $max--;
			push(@removed, $message) if $callback;
		}
	}

	$callback->(\@removed) if $callback;

	return;
}

sub remove_all 
{
	my ($self, $callback) = @_;
	my $destinations = $self->messages;
	if ($callback) 
	{
		my @result = ();
		push(@result, @$_) foreach (values %$destinations);	
		$callback->(\@result);
	}
	%$destinations = ();
	return;
}

sub claim_and_retrieve
{
	my ($self, $destination, $client_id, $dispatch) = @_;
	my $messages = $self->messages->{$destination} || [];

	# look for an unclaimed message and take it
	foreach my $message (@$messages)
	{
		unless ($message->claimed)
		{
			# claim it, yo!
			$message->claim($client_id);
			$self->log('info', sprintf('Message %s claimed by client %s',
				$message->id, $client_id));

			$dispatch->($message, $destination, $client_id);
			return;
		}
	}

	# Spec says to do this on failure.
	$dispatch->(undef, $destination, $client_id);
	return;
}

# unmark all messages owned by this client
sub disown
{
	my ($self, $destination, $client_id) = @_;
	my $messages = $self->messages->{$destination} || return;

	foreach my $msg (grep { 
		$_->claimed && ($_->claimant == $client_id) 
	} (@$messages))
	{
		$msg->disown();
	}
}

sub storage_shutdown
{
	my ($self, $complete) = @_;
	$complete->();
	return;
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

I wouldn't suggest using this as your main storage engine because if messages aren't
removed by consumers, it will continue to consume more memory until it explodes.  Check-out
L<POE::Component::MessageQueue::Storage::Complex> which uses this module internally to keep
messages in memory for a period of time before moving them into persistent storage.

=head1 CONSTRUCTOR PARAMETERS

None to speak of!

=head1 SEE ALSO

L<POE::Component::MessageQueue>,
L<POE::Component::MessageQueue::Storage>,
L<POE::Component::MessageQueue::Storage::FileSystem>,
L<POE::Component::MessageQueue::Storage::DBI>,
L<POE::Component::MessageQueue::Storage::Generic>,
L<POE::Component::MessageQueue::Storage::Generic::DBI>,
L<POE::Component::MessageQueue::Storage::Throttled>,
L<POE::Component::MessageQueue::Storage::Complex>

=cut
