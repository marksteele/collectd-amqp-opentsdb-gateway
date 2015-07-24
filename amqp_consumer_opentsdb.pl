#!/usr/bin/perl

## TODO: error checking around dead socket....

use strict;
use Data::Dumper;
use Net::RabbitMQ;
use IO::Socket;
use Getopt::Long;
use JSON;
$| = 1;
$0 = 'AMQP-OpenTSDB';

my @amqp_hosts;
my @amqp_exchanges;
my $amqp_queue = 'consumerqueue';
my $amqp_topic = '#';
my $amqp_queue_max_length = 0;
my @opentsdb_hosts;

my $debug = 0;

GetOptions (
  "amqp_hosts=s" => \@amqp_hosts,
  "amqp_exchanges=s" => \@amqp_exchanges,
  "amqp_queue=s" => \$amqp_queue,
  "amqp_queue_max_length=i" => \$amqp_queue_max_length,
  "amqp_topic=s" => \$amqp_topic,
  "opentsdb_hosts=s" => \@opentsdb_hosts,
  "debug=s" => \$debug
);
@amqp_hosts = split(/,/,join(',',@amqp_hosts));
@amqp_exchanges = split(/,/,join(',',@amqp_exchanges));
@opentsdb_hosts = split(/,/,join(',',@opentsdb_hosts));

while(1) {
  print "Starting up\n" if $debug;
  ## Pick AMQP/OpenTSDB hosts
  my $amqp = $amqp_hosts[rand(@amqp_hosts)];
  if ($amqp !~ /^amqp:\/\/(.+?):(.+?)@(.+?):(\d+)(.+)$/) {
    print "Unable to parse AMQP URI\n";
    exit;
  }
  my $amqp_user = $1;
  my $amqp_pass = $2;
  my $amqp_host = $3;
  my $amqp_port = $4;
  my $amqp_vhost = $5;
  print "$amqp_user : $amqp_pass @ $amqp_host : $amqp_port $amqp_vhost\n" if $debug;
  my $opentsdb = $opentsdb_hosts[rand(@opentsdb_hosts)];

  if ($opentsdb !~ /^opentsdb:\/\/(.+?):(\d+)\/?$/) {
    print "Unable to parse opentsdb URI\n";
    exit;
  }

  my $opentsdb_host = $1;
  my $opentsdb_port = $2;
  my $amqp_queue_options = ($amqp_queue_max_length > 0)
    ? {'x-max-length' => $amqp_queue_max_length}
    : {};

  print "Connecting to opentsdb: $opentsdb\n" if $debug;
  my $sock;
  $sock = IO::Socket::INET->new(PeerAddr => $opentsdb_host, PeerPort => $opentsdb_port, Proto => 'tcp', Timeout  => 1, Blocking => 0);
  $sock->sockopt(SO_KEEPALIVE);
  if (!$sock->connected()) {
    print "Unable to connect to opentsdb\n" if $debug;
    sleep(1);
    next;
  }
  print "Connected to opentsdb $opentsdb\n" if $debug;
  print "Connecting to RabbitMQ: $amqp\n" if $debug;
  my $mq;
  eval {
    $mq = Net::RabbitMQ->new();
    $mq->connect($amqp_host , { port => $amqp_port, user => $amqp_user, password => $amqp_pass, vhost => $amqp_vhost });
    $mq->channel_open(1);
    $mq->queue_declare(1, $amqp_queue, {}, $amqp_queue_options);
    foreach my $ex (@amqp_exchanges) {
      $mq->queue_bind(1, $amqp_queue, $ex, $amqp_topic);
    }
    $mq->consume(1,$amqp_queue);
    print "Connected to $amqp\nEntering main loop" if $debug;
  };

  if ($@) {
    print "Unable to connect to rabbit: $@\n";
    sleep(1);
    next;
  };

  while(1) {
    my $msg = $mq->recv();
    if ($msg) {
      print $msg->{'body'} if $debug;
      foreach my $line (split(/\n/, $msg->{'body'})) {
        my $metric = {};
        eval {
          $metric = decode_json($line);
        };
        if ($@) {
          print "Error parsing input data: $line\n" if $debug;
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
        ## Should be using select to see if the socket is ready.... Ah well....
        my $len = length($output);
        my $written = 0;
        while ($written != $len) {
          $written += syswrite($sock,$output,$len - $written, $written);
        }
      }
    } else {
      print "Nothing received, sleeping\n" if $debug;
      sleep(1);
    }
    ## Drain inbound data from socket
    sysread($sock,my $blarh,8192);
  }
}
