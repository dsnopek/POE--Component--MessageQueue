
use strict;
use Test::More;
use Test::MockObject;
use Net::Stomp::Frame; 

BEGIN
{
    eval "use IO::String";
    if ($@) {
        plan skip_all => "IO::String not installed";
    } else {
        plan tests => 20;
    }

    use_ok("POE::Component::MessageQueue::Client");
}

my $client_id = 'mq_client_id';

{   # The constructor can take hashref or a string
    # Make sure that they create the same things
    my $client1 = POE::Component::MessageQueue::Client->new($client_id);
    my $client2 = POE::Component::MessageQueue::Client->new({ client_id => $client_id });

    is_deeply($client1, $client2, "different constructor parameters should yield to the same object");

    # Whilte we're at it, make sure that the values are initialized
    is($client1->{client_id}, $client_id);
    isa_ok($client1->{queue_names}, 'ARRAY');
    is(scalar @{ $client1->{queue_names} }, 0);
    is($client1->{connected}, 0);
    is($client1->{login}, '');
    is($client1->{passcode}, '');
}

{
    # Make mock objects for the various POE stuff

    my $wheel      = Test::MockObject->new();
    my $session    = Test::MockObject->new();
    my $poe_kernel = Test::MockObject->new();

    # These will be used a few times
    $session->set_always(get_heap => { client => $wheel });
    $poe_kernel->mock(alias_resolve => sub {
        my ($self, $id) = @_;
        if (is($id, $client_id)) {
            return $session;
        }
        return ();
    });

    # Prepare PoCo::MQ::Client by placing a mock object in its namespace
    # acting as POE::Kernel
    $POE::Component::MessageQueue::Client::poe_kernel = $poe_kernel;


    # Finally, commence testing

    my $login    = 'foo';
    my $passcode = 'bar';
    my $client   = POE::Component::MessageQueue::Client->new($client_id);

    { # Test connect
        $wheel->mock(put => sub {
            my ($self, $serialized) = @_;
            my $frame = Net::Stomp::Frame->parse(IO::String->new($serialized));
            is($frame->command, 'CONNECTED');
            is($frame->headers->{session}, "client-$client_id");
        });

        $client->connect({ login => $login, 'passcode' => $passcode });
        is($client->{login}, $login, "login is $login");
        is($client->{passcode}, $passcode, "passcode is $passcode");
        is($client->{connected}, 1, "connected is true");
        $wheel->remove('put');
    }

    # Test shutdown
    $poe_kernel->mock(post => sub {
        my ($self, $session_id, $event) = @_;
        is($session_id, $client_id, "target session matches");
        is($event, 'shutdown', "event is 'shutdown'");
    } );
    $client->shutdown();

    # Test send frame
    {
        $wheel->mock(put => sub {
            my ($self, $serialized) = @_;
            my $frame = Net::Stomp::Frame->parse(IO::String->new($serialized));
            is($frame->command, 'MESSAGE');
            is($frame->headers->{session}, "client-$client_id");
            is($frame->body, "DUMMY");
        });
        $client->send_frame( Net::Stomp::Frame->new({
            command => "MESSAGE",
            headers => { session => "client-$client_id" },
            body    => "DUMMY"
        } ) );
        $wheel->remove('put');
    }
}