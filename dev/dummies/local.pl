#!/usr/bin/perl 
# 
# 
# 
# 
# Kim Brugger (17 May 2010), contact: kim.brugger@easih.ac.uk

use strict;
use warnings;
use Data::Dumper;

use Getopt::Std;


my %opts;
getopts('S:f:F', \%opts);


my $sleep_time = shift || 10;
my $fail_rate = $opts{f} || 5;

srand;
$sleep_time = int(rand( $sleep_time ));

$sleep_time = $opts{'S'} if ($opts{'S'});

#print "Starting sleep for $sleep_time seconds\n";
sleep($sleep_time);

if ( 0 && $opts{'F'} || $fail_rate > int(rand(100))) {
  print "------- Failed -----------\n";
  exit 1;
}
#print "Done sleeping.\n";

