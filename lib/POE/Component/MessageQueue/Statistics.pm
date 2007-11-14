# $Id$
#
# Copyright (c) 2007 Daisuke Maki <daisuke@endeworks.jp>
# All rights reserved.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

package POE::Component::MessageQueue::Statistics;
use strict;
use warnings;

use Data::Dumper;

sub new
{
    my $class = shift;
    my $self  = bless {
        statistics => {
            total_stored => 0,
            queues => {},
        }
    }, $class;

    $self;
}

sub register
{
    my ($self, $mq) = @_;
    $mq->register_event( $_, $self ) for qw(store recv dispatch ack pump);
}

my %METHODS = (
    store    => 'notify_store',
    'recv'   => 'notify_recv',
    dispatch => 'notify_dispatch',
    ack      => 'notify_ack',
    pump     => 'notify_pump',
);

sub get_queue
{
	my ($self, $name) = @_;

	my $queue = $self->{statistics}{queues}{$name};

	if ( not defined $queue )
	{
		$queue = $self->{statistics}{queues}{$name} = {
			stored          => 0,
			total_stored    => 0,
			total_recvd     => 0,
			avg_secs_stored => 0,
			avg_size_recvd  => 0,
		};
	}

	return $queue;
}

sub notify
{
    my ($self, $event, $data) = @_;

    my $method = $METHODS{ $event };
    return unless $method;
    $self->$method($data);
}

sub notify_store
{
    my ($self, $data) = @_;
    my $h = $self->{statistics};
    $h->{total_stored}++;

	my $stats = $self->get_queue($data->{queue}->{queue_name});
	$stats->{stored}++;
	$stats->{total_stored}++;
}

sub notify_recv
{
	my ($self, $data) = @_;

	my $stats = $self->get_queue( $data->{queue}->{queue_name} );
	$stats->{total_recvd}++;

	my $size = $data->{message}->{size};

	# recalc the average
	$stats->{avg_size_recvd} = (($stats->{avg_size_recvd} * ($stats->{total_recvd} - 1)) + $size) / $stats->{total_recvd};
}

sub message_handled
{
	my ($self, $data) = @_;

	my $info = $data->{message} || $data->{message_info};

	my $stats = $self->get_queue( $data->{queue}->{queue_name} );

	$stats->{stored}--;
	
	my $secs_stored = (time() - $info->{timestamp});

	# recalc the average
	$stats->{avg_secs_stored} = (($stats->{avg_secs_stored} * ($stats->{total_stored} - 1)) + $secs_stored) / $stats->{total_stored};
}

sub notify_dispatch
{
    my ($self, $data) = @_;

    my $receiver = $data->{client};

    my $sub;
    if ( ref($receiver) eq 'POE::Component::MessageQueue::Client' )
    {
        # automatically convert clients to subscribers!
        $sub = $data->{queue}->get_subscription( $receiver );
    }
    else
    {
        $sub = $receiver;
    }

    if ($sub->{ack_type} eq 'auto') {
        $self->message_handled($data);
    }
}

sub notify_ack {
    my ($self, $data) = @_;

    my $receiver = $data->{client};

    my $sub;
    if ( ref($receiver) eq 'POE::Component::MessageQueue::Client' )
    {
        # automatically convert clients to subscribers!
        $sub = $data->{queue}->get_subscription( $receiver );
    }
    else
    {
        $sub = $receiver;
    }

    if ($sub->{ack_type} eq 'client') {
        $self->message_handled($data);
    }
}

sub notify_pump {
    my ($self, $data) = @_;
    # Now dumped as via PoCo::MQ::Statistics::Publish
    #$self->dump_as_string;
}

sub dump_as_string
{
    my ($self, $output) = @_;

    $output ||= \*STDERR;
    print $output "TOTAL STORED (cumul): $self->{statistics}{total_stored}\n";
    my $queues = $self->{statistics}{queues};

    print $output "QUEUES:\n";
    foreach my $name (sort keys %$queues) {
        my $queue = $queues->{$name};
        print $output " + $name\n";
		print $output "   - Stored: $queue->{stored}\n";
		print $output "   - Recv'd: $queue->{total_recvd}\n";
		print $output "   - Avg. secs stored: $queue->{avg_secs_stored}\n";
		print $output "   - Avg. size recv'd: $queue->{avg_size_recvd}\n";
    }
}

1;

__END__

=head1 NAME

POE::Component::MessageQueue::Statistics - Gather MQ Usage Statistics 

=head1 SYNOPSIS

  my $statistics = POE::Component::MessageQueue::Statistics->new();
  $mq->register( $statistics );

=head1 DESCRIPTION

POE::Component::MessageQueue::Statistics is a simple observer that receives
events from the main POE::Component::MessageQueue object to collect usage
statistics.

By itself it will only *gather* statistics, and will not output anything.

To enable outputs, you need to create a separate Publish object:

  POE::Component::MessageQueue::Statistics::Publish::YAML->new(
    output => \*STDERR,
    statistics => $statistics
  );

Please refer to POE::Component::MessageQueue::Statistics::Publish for details
on how to enable output

=head1 AUTHOR

Daisuke Maki E<lt>daisuke@endeworks.jpE<gt>

=cut
