use strict;
use warnings;
use Test::More tests => 7;
use POE;
use POE::Session;

BEGIN {
	my $mq = "POE::Component::MessageQueue";
	require_ok($mq."::Message");
	require_ok($mq."::Logger");
	require_ok($mq."::Storage::Complex");
	require_ok($mq."::Storage::BigMemory");
};

my $logger = POE::Component::MessageQueue::Logger->new;
$logger->set_log_function(sub{});
my $complex = POE::Component::MessageQueue::Storage::Complex->new(
	front       => POE::Component::MessageQueue::Storage::BigMemory->new,
	back        => POE::Component::MessageQueue::Storage::BigMemory->new,
	timeout     => 2,
	granularity => 1,
	front_max   => 64 * 8, # should hold 8 messages!
);
$complex->set_logger($logger);

ok($complex, "store created");
my $now = time();
my $p = 1;
my @messages = map POE::Component::MessageQueue::Message->new(
	id          => $_,
	timestamp   => ($now + $_),
	persistent  => ($p = !$p),
	destination => "/queue/completely/unimportant",
	body        => "." x 64,
), (1..64);

sub _count {
	$complex->front->get_all(sub {
		my $aref = shift;
		is(@$aref, 8, "front store size");
		$complex->back->get_all(sub {
			is(@{$_[0]}, 32, "persistent messages stored.");
			$complex->storage_shutdown();
		});
	});
}

sub _store {
	my ($kernel, $session, $messages, $done) = @_[KERNEL, SESSION, ARG0..ARG1];
	if (my $m = pop(@$messages)) {
		$complex->store($m, sub {
			$kernel->post($session, '_store', $messages, $done);
		});	
	}
	else {
		$kernel->delay($done, $complex->timeout+1);
	}
}

sub _start {
	my ($kernel, $session) = @_[KERNEL, SESSION];
	$kernel->post($session, '_store', \@messages, '_count'); 	
}

POE::Session->create(inline_states => { 
	_start => \&_start,
	_store => \&_store,
	_count => \&_count,
});
$poe_kernel->run();
