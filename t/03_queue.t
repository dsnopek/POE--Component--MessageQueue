use strict;
use Test::More;
use Test::MockObject;
use Test::MockObject::Extends;

BEGIN
{
    plan tests => 43;

    use_ok("POE::Component::MessageQueue::Message");
    use_ok("POE::Component::MessageQueue::Queue");
    use_ok("Event::Notify");
}

my $mq      = Test::MockObject::Extends->new( 'POE::Component::MessageQueue' );
my $storage = Test::MockObject->new;
my $qname   = '/queue/test';

$mq->set_always(_log => 1);
$mq->set_always(get_storage => $storage);

{   # The consturctor can take hashref or two scalars

    my $queue1 = POE::Component::MessageQueue::Queue->new( $mq, $qname );
    my $queue2 = POE::Component::MessageQueue::Queue->new({ parent => $mq, queue_name => $qname });

    is_deeply($queue1, $queue2, "different constructor parameters should yield the same object");

    # Whilte we're at it, make sure that the values are initialized
    is($queue1->{parent}, $mq);
    is($queue1->{queue_name}, $qname);
    isa_ok($queue1->{subscriptions}, 'ARRAY');
    is(scalar @{ $queue1->{subscriptions} }, 0);
    isa_ok($queue1->{sub_map}, 'HASH');
    is(scalar keys %{ $queue1->{sub_map} }, 0);
    is($queue1->{has_pending_messages}, 1);
    is($queue1->{pumping}, 0, "pumping should be 0");

    is($queue1->get_parent, $mq)
}

{
    my $ack_type = 'client';
    my $notify   = Event::Notify->new;
    my @clients;
    foreach (1..2) {
        my $client   = Test::MockObject->new;
        $client->{client_id} = 'mock-client-' . $_;
        $client->mock(_add_queue_name => sub {
            my ($self, $name) = @_;
            is ($name, $qname);
        });
        push @clients, $client;
    }

    local $mq->{notify} = $notify;
    $storage->mock(claim_and_retrieve => sub {
        ok(1, 'claim_and_retrieve called');
        return ()
    });

    my $queue    = POE::Component::MessageQueue::Queue->new( $mq, $qname );

    foreach my $client (@clients) {
        $queue->add_subscription( $client, $ack_type );
        $client->mock(send_frame => sub {
            my ($self, $frame) = @_;
            is( $frame->headers->{destination}, $qname, "correct queue" );
            is( $frame->body, 'DUMMY', "correct body" );

            return 1;
        });
    }

    $mq->mock( push_unacked_message => sub {
        my ($self, $message, $cl) = @_;

        ok( $message->{message_id} eq 1 || $message->{message_id} eq 2, "correct message IDs (1 or 2)" );
    } );
    foreach my $message_id (1..10) {
        my $message =  POE::Component::MessageQueue::Message->new( {
            message_id  => $message_id,
            destination => $qname,
            body        => "DUMMY"
        } );
        $storage->mock(store => sub {
            my ($self, $msg) = @_;
            is($msg->{message_id}, $message_id, "got the right message ($msg->{message_id} <-> $message_id)");
        } );
        $queue->enqueue($message);

        # Make sure that there are no subscribers available
        if ($message_id < 2) {
            ok($queue->get_available_subscriber(), "There should be one more subscriber" );
        } else {
            ok(! $queue->get_available_subscriber(), "No subscriber should be available after 2 enqueues" );
        }
    }
}

