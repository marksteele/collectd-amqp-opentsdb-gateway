#!/usr/bin/perl

use strict;
use Data::Dumper;
use Net::RabbitMQ;
use IO::Socket;
use Getopt::Long;

my %options;
$options{'amqp_host'} = '127.0.0.1';
$options{'amqp_port'} = 5672;
$options{'amqp_user'} = 'amqpuser';
$options{'amqp_password'} = 'amqppass';
$options{'amqp_vhost'} = '/';
$options{'amqp_exchange'} = 'stats';
$options{'amqp_queue'} = 'consumerqueue';
$options{'opentsdb_host'} = '127.0.0.1';
$options{'opentsdb_port'} = 4242;
$options{'debug'} = 0;
GetOptions ("var=s" => \%options);
$options{'amqp_queue'} .= $$;

my $mq = Net::RabbitMQ->new();
$mq->connect($options{'amqp_host'} , { port => $options{'amqp_port'}, user => $options{'amqp_user'}, password => $options{'amqp_password'}, vhost => $options{'amqp_vhost'} });
$mq->channel_open(1);
$mq->queue_declare(1, $options{'amqp_queue'});
$mq->queue_bind(1, $options{'amqp_queue'}, $options{'amqp_exchange'}, '');
$mq->consume(1,$options{'amqp_queue'});

### Assuming metric format is 'some.metric.name.datacenter.host', tagged in opentsdb with datacenter= and host=

while(1) {
  my $msg = $mq->recv();
  if ($msg) {
    my $sock = IO::Socket::INET->new(PeerAddr => $options{'opentsdb_host'},
                                      PeerPort => $options{'opentsdb_port'},
                                      Proto    => 'tcp',
                                      Timeout  => 10);
    if ($sock) {
      foreach my $line (split(/\n/, $msg->{'body'})) {
        print "IN: $line\n" if $options{'debug'};
        if ($line =~ /((?:[^.]+\.)+)([^.]+)\.([^.]+)\s(\d+(\.\d+)?)\s(\d+)/) {
          my ($metric, $timestamp, $value, $datacenter, $host) = ($1, $5, $4, $2, $3);
          chop($metric);
	  print "OUT: put $metric $timestamp $value datacenter=$datacenter host=$host\n" if $options{'debug'};
	  print $sock "put $metric $timestamp $value datacenter=$datacenter host=$host\n";
        }
      }
      close($sock);
    }
  } else {
    sleep(1);
  }
}
