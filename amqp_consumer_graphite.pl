#!/usr/bin/perl

use strict;
use Data::Dumper;
use Net::RabbitMQ;
use IO::Socket;

my $rabbit_host = '127.0.0.1';
my $rabbit_port = 5672;
my $graphite_host = '127.0.0.1';
my $graphite_port = 2003;

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

while(1) {
  my $msg = $mq->recv();
  if ($msg) {
    my $sock = IO::Socket::INET->new(PeerAddr => $graphite_host,
                                      PeerPort => $graphite_port,
                                      Proto    => 'tcp',
                                      Timeout  => 10);
    if ($sock) {
      print $sock $msg->{'body'};
      close($sock);
    }
  } else {
    sleep(1);
  }
}
