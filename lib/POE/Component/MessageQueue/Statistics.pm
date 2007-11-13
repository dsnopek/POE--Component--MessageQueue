# $Id$
#
# Copyright (c) 2007 Daisuke Maki <daisuke@endeworks.jp>
# All rights reserved.

package POE::Component::MessageQueue::Statistics;
use strict;
use warnings;

use Data::Dumper;

sub new
{
    my $class = shift;
    my $self  = bless {}, $class;
    $self->{statistics} = {
        total_stored => 0,
        queues => {},
    };
    $self;
}

sub register
{
    my ($self, $mq) = @_;
    $mq->register_event( $_, $self ) for qw(store dispatch ack pump);
}

my %METHODS = (
    store    => 'notify_store',
    dispatch => 'notify_dispatch',
    ack      => 'notify_ack',
    pump     => 'notify_pump',
);

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

    my $queue = $data->{queue};
    $h->{queues}{ $queue->{queue_name} } ||= {};
    $h->{queues}{ $queue->{queue_name} }{stored}++;
    $h->{queues}{ $queue->{queue_name} }{total_stored}++;
    $h->{queues}{ $queue->{queue_name} }{avg_secs_stored} = 0;
    $h->{queues}{ $queue->{queue_name} }{avg_size_received} = 0;
}


sub message_handled
{
	my ($self, $data) = @_;

	my $info = $data->{message} || $data->{message_info};

	my $stats = $self->{statistics}{queues}{ $data->{queue}->{queue_name} };

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
    $self->dump_as_string;
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
		print $output "   - Avg. secs stored: $queue->{avg_secs_stored}\n";
    }
}

1;
