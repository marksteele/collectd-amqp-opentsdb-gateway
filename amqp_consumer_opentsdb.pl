#!/usr/bin/perl

use strict;
use Data::Dumper;
use Net::RabbitMQ;
use IO::Socket;

my $rabbit_host = '127.0.0.1';
my $rabbit_port = 5672;
my $opentsdb_host = '127.0.0.1';
my $opentsdb_port = 4242;

my $user = 'amqpuser';
my $password = 'amqppass';
my $vhost = '/';
my $exchange = 'stats';
my $queue = 'consumerqueue' . $$;

my $mq = Net::RabbitMQ->new();
$mq->connect($rabbit_host , { port => $rabbit_port, user => $user, password => $password, vhost => $vhost });
$mq->channel_open(1);
$mq->queue_declare(1, $queue);
$mq->queue_bind(1, $queue, $exchange, '');
$mq->consume(1,$queue);

### Assuming metric format is 'some.metric.name.datacenter.host', tagged in opentsdb with datacenter= and host=

while(1) {
  my $msg = $mq->recv();
  if ($msg) {
    my $sock = IO::Socket::INET->new(PeerAddr => $opentsdb_host,
                                      PeerPort => $opentsdb_port,
                                      Proto    => 'tcp',
                                      Timeout  => 10);
    if ($sock) {
      foreach my $line (split(/\n/, $msg->{'body'})) {
        if ($line =~ /(.+)\.([^.]+)\.([^.]+)\s(\d+)\s(\d+)/) {
          print $sock "put $1 $5 $4 datacenter=$2 host=$3\n";
        }
      }
      close($sock);
    }
  } else {
    sleep(1);
  }
}
