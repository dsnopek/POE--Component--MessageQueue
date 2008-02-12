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

package POE::Component::MessageQueue::Message;

use Net::Stomp::Frame;
use strict;

sub new
{
	my $class = shift;
	my $args  = shift;

	my $message_id;
	my $destination;
	my $body;
	my $persistent;
	my $in_use_by;
	my $size;
	my $timestamp;

	if ( ref($args) eq 'HASH' )
	{
		$message_id  = $args->{message_id};
		$destination = $args->{destination};
		$body        = $args->{body};
		$persistent  = $args->{persistent};
		$in_use_by   = $args->{in_use_by};
		$timestamp   = $args->{timestamp};
		$size        = $args->{size};
	}
	else
	{
		$message_id  = $args;
		$destination = shift;
		$body        = shift;
		$persistent  = shift;
	}

	if ( not defined $size )
	{
		$size = ($body) ? length $body : 0;
	}

	my $self =
	{
		message_id  => $message_id,
		destination => $destination,
		body        => $body       || '',
		persistent  => $persistent || 0,
		in_use_by   => $in_use_by  || undef,
		timestamp   => $timestamp  || time(),
		size        => $size,
	};

	bless  $self, $class;
	return $self;
}

sub get_message_id  { return shift->{message_id}; }
sub get_destination { return shift->{destination}; }
sub get_body        { return shift->{body}; }
sub get_persistent  { return shift->{persistent}; }
sub get_in_use_by   { return shift->{in_use_by}; }
sub get_timestamp   { return shift->{timestamp}; }
sub get_size        { return shift->{size}; }

sub is_in_queue
{
	return shift->{destination} =~ /^\/queue\//;
}

sub is_in_topic
{
	return shift->{destination} =~ /^\/topic\//;
}

sub set_in_use_by
{
	my ($self, $in_use_by) = @_;
	$self->{in_use_by} = $in_use_by;
}

sub get_queue_name
{
	my $self = shift;

	if ( $self->{destination} =~ /^\/queue\/(.*)$/ )
	{
		return $1;
	}

	return undef;
}

sub create_stomp_frame
{
	my $self = shift;

	my $frame = Net::Stomp::Frame->new({
		command => 'MESSAGE',
		headers => {
			'destination'    => $self->{destination},
			'message-id'     => $self->{message_id},
			'content-length' => $self->{size},
		},
		body => $self->{body}
	});

	return $frame;
}

1;

