#!/usr/bin/perl 
# 
# 
# 
# 
# Kim Brugger (17 May 2010), contact: kim.brugger@easih.ac.uk

use strict;
use warnings;
use Data::Dumper;
use IO::Socket;

my $job_id = shift || -1;
my $port   = shift || 8888;

my $socket =  IO::Socket::INET->new( "localhost:$port");

if ( ! $socket ) {
  print "Unknown";
  exit 1;
}

print $socket "$job_id\n";
my $result = <$socket>;

if ( ! $result ) {
  print "Unknown";
  exit 1;
}

$result =~ s/\n//;;
$result =~ s/\r//;;

print "$result\n";
