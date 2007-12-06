use strict;
use Test::More (tests => 11);

BEGIN
{
    use_ok("POE::Component::MessageQueue::Message");
}

my $message_id  = 'test-message';
my $destination = '/queue/foo/bar';
my $body        = '0123456789abcdefghijklmnopqrstuvwxyz';
my $persistent  = 1;
my $in_use_by   = "client-id";
my $timestamp   = time();
my $size        = length($body);

can_ok(
    'POE::Component::MessageQueue::Message',
    qw(new get_message_id get_destination get_body get_persistent get_in_use_by get_timestamp get_size),
    qw(is_in_queue is_in_topic set_in_use_by get_queue_name create_stomp_frame)
);

{
    my $msg1 = POE::Component::MessageQueue::Message->new( {
        message_id  => $message_id,
        destination => $destination,
        body        => $body,
        persistent  => $persistent,
    });

    my $msg2 = POE::Component::MessageQueue::Message->new(
        $message_id, $destination, $body, $persistent
    );

    is_deeply( $msg1, $msg2, "HASHREF constructor and LIST constructor yields the same");
}

{
    my $msg = POE::Component::MessageQueue::Message->new({
        message_id  => $message_id,
        destination => $destination,
        body        => $body,
        persistent  => $persistent,
    });

    is( $msg->get_message_id, $message_id );
    is( $msg->get_destination, $destination );
    is( $msg->get_body, $body );
    is( $msg->get_persistent, $persistent );
    is( $msg->get_size, $size );

    my $queue_name = $destination;
    $queue_name =~ s{^/queue/}{};
    is( $msg->get_queue_name, $queue_name );
}

{
    my $msg = POE::Component::MessageQueue::Message->new({
        message_id  => $message_id,
        destination => $destination,
        body        => $body,
        persistent  => $persistent,
    });

    my $want = Net::Stomp::Frame->new( {
        command => 'MESSAGE',
        headers => {
            'destination' => $destination,
            'message-id'  => $message_id
        },
        body    => $body
    });
    is_deeply( $msg->create_stomp_frame, $want );
}

{
    my $msg = POE::Component::MessageQueue::Message->new({
        message_id  => $message_id,
        destination => $destination,
        body        => $body,
        persistent  => $persistent,
    });
    ok($msg->is_in_queue)
}
