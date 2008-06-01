use strict;
use warnings;
use lib 't/lib';

use POE::Component::MessageQueue::Test::Stomp;
use POE::Component::MessageQueue::Test::MQ;
use POE::Component::MessageQueue::Test::EngineMaker;

use File::Path;
use IO::Dir qw(DIR_UNLINK);
use Test::Exception;
use Test::More tests => 37;

# Our testing agenda:
#
# 1) Start MQ with Complex
# 2) Subscribe two consumers on the same queue
# 3) Send 30 messages
# 4) Check that both got exactly 15
# 5) Disconnect and reconnect a few times
# 6) Reverify that no new messages were received.

lives_ok { 
	rmtree(DATA_DIR); 
	mkpath(DATA_DIR); 
	make_db() 
} 'setup data dir';

my $pid = start_mq('Complex');
ok($pid, 'MQ started');
sleep 2;

sub setup_consumer
{
	my $stomp = stomp_connect();
	$stomp->subscribe({
		destination => '/queue/test',
		'ack'       => 'auto',
	});
	return $stomp;
}

my @clients = ( setup_consumer, setup_consumer );

# send our 30 messages
lives_ok {
	my $producer = stomp_connect();
	foreach my $index (1..30)
	{
		$producer->send({
			persistent  => 'true',
			destination => '/queue/test',
			body        => "$index"
		});
	}
	$producer->disconnect();
} 'messages sent';

sub even { grep { $_ % 2 == 0 } (1..30) }
sub odd  { grep { $_ % 2 != 0 } (1..30) }

sub consumer_receive
{
	my ($consumer, @ids) = @_;

	while (scalar @ids > 0)
	{
		#my $can_read;
		#ok ($can_read = $consumer->can_read({ timeout => 5 }), 'can read');
		#last if (not $can_read);

		my $frame = $consumer->receive_frame();
		is ($frame->body, $ids[0], 'correct message order');
		
		shift @ids;
	}

	# returns true value if we got all the messages we expected
	return scalar @ids == 0;
}

ok(consumer_receive($clients[1], even), 'second consumer got even messages');
ok(consumer_receive($clients[0], odd),  'first consumer got odd messages');

$_->disconnect() for (@clients);

ok(stop_mq($pid), 'MQ shut down');

lives_ok { rmtree(DATA_DIR) } 'Data dir removed';

