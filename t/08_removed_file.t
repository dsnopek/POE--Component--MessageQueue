use strict;
use warnings;
use lib 't/lib';

use POE::Component::MessageQueue::Test::Stomp;
use POE::Component::MessageQueue::Test::MQ;
use POE::Component::MessageQueue::Test::EngineMaker;

use File::Path;
use IO::Dir;
use Test::Exception;
use Test::More qw(no_plan);

# 1) Start MQ with Filesystem
# 2) send some messages
# 3) shutdown MQ
# 4) delete the message bodies of a few
# 5) Start MQ back up
# 6) receive remaining messages without gumming anything up

lives_ok { 
	rmtree(DATA_DIR); 
	mkpath(DATA_DIR); 
	make_db() 
} 'setup data dir';

my $pid = start_mq('FileSystem');
ok($pid, 'MQ started');
sleep 2;

lives_ok {
	my $sender = stomp_connect();
	stomp_send($sender) for (1..100);
	$sender->disconnect;
} 'messages sent';

ok(stop_mq($pid), 'MQ shut down.');

my $dh = IO::Dir->new(DATA_DIR);
my @files = grep { /msg-.*\.txt/ } (IO::Dir->new(DATA_DIR)->read());
is(scalar @files, 100, "100 messages stored.");

# Pick up here
