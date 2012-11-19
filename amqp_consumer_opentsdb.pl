#!/usr/bin/perl

use strict;
use Data::Dumper;
use Net::RabbitMQ;
use IO::Socket;
use Getopt::Long;
use JSON;

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
$options{'compress'} = 0;
$options{'debug'} = 0;
$options{'input_format'} = 'json';
GetOptions ("var=s" => \%options);

if ($options{'compress'}) {
  use Compress::Snappy;
}

my $mq = Net::RabbitMQ->new();
$mq->connect($options{'amqp_host'} , { port => $options{'amqp_port'}, user => $options{'amqp_user'}, password => $options{'amqp_password'}, vhost => $options{'amqp_vhost'} });
$mq->channel_open(1);
$mq->queue_declare(1, $options{'amqp_queue'});
$mq->queue_bind(1, $options{'amqp_queue'}, $options{'amqp_exchange'}, '#');
$mq->consume(1,$options{'amqp_queue'});

### Assuming metric format is 'some.metric.name.datacenter.host', tagged in opentsdb with datacenter= and host=

while(1) {
  my $msg = $mq->recv();
  if ($msg) {
    if ($options{'compress'}) {
      $msg->{'body'} = decompress($msg->{'body'});
    }
    my $sock = IO::Socket::INET->new(PeerAddr => $options{'opentsdb_host'},
                                      PeerPort => $options{'opentsdb_port'},
                                      Proto    => 'tcp',
                                      Timeout  => 1);
    if ($sock) {
      foreach my $line (split(/\n/, $msg->{'body'})) {
        my $metric = {};
        eval {
          $metric = decode_json($line);
        };
        if ($@) {
          print "Error parsing input data: $line\n";
          next;
        }
        my $output = sprintf("put %s %d %s", $metric->{'plugin'},$metric->{'time'},$metric->{'value'});
        delete($metric->{'plugin'});
        delete($metric->{'time'});
        delete($metric->{'value'});
        foreach my $k (keys %{$metric}) {
          my $tk = $k;
          $tk =~ s/[ =]//g;
          $metric->{$k} =~ s/[ =]//g;
          $output .= ' ' . $tk . '=' . $metric->{$k};
        } 
        $output .= "\n";
	print $sock $output;
      }
      close($sock);
    }
  } else {
    sleep(1);
  }
}
