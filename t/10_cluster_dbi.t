use strict;
use warnings;

use lib 't/lib';
use POE::Component::MessageQueue::Test::Stomp;
use POE::Component::MessageQueue::Test::MQ;
use POE::Component::MessageQueue::Test::DBI;
use Test::More;
use Test::Exception;
use DBI;

# Make sure that there are no conflicts between in_use_by values when two
# seperate MQs are using Storage::DBI pointing at the same database.

BEGIN {
	check_environment_vars_dbi();
}

clear_messages_dbi();

plan tests => 4; 

my $pid1 = start_mq(storage => storage_factory_dbi(mq_id => 'mq1'));
ok($pid1, "MQ1 started");
sleep 2;

my $pid2 = start_mq(
	storage => storage_factory_dbi(mq_id => 'mq2'),
	port    => '8100'
);
ok($pid2, "MQ2 started");
sleep 2;

# Stop both MQ's, we're done
ok(stop_fork($pid1), 'MQ1 shut down.');
ok(stop_fork($pid2), 'MQ2 shut down.');

