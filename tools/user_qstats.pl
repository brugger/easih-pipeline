#!/usr/bin/perl 
# 
# 
# 
# 
# Kim Brugger (29 Sep 2010), contact: kim.brugger@easih.ac.uk

use strict;
use warnings;
use Data::Dumper;

my %res;

use Sys::Hostname;
my $host = hostname;
use POSIX 'strftime';
my $username = scalar getpwuid $<;

while(1) {

  open( my $pipe, "qstat -u $username | ") || die "Could not open qstat-pipe:$!\n";
  while(<$pipe>) {
    chomp;
    
    next if ( ! /(\d+\.*?) /);
    
    my @fields = split(" ", $_);
    
    $res{ $fields[9] }++;
  }


  my $time = strftime('%d/%m/%y %H.%M', localtime);


  print "[$time \@$host]\n" . "-"x30 . "\n";


  printf("|| Completed     ||    %4d ||\n", $res{ C }) if ($res{ C });
  printf("|| Held          ||    %4d ||\n", $res{ H }) if ($res{ H });
  printf("|| Queued        ||    %4d ||\n", $res{ Q }) if ($res{ Q });
  printf("|| Running       ||    %4d ||\n", $res{ R }) if ($res{ R });
  printf("|| Transfering   ||    %4d ||\n", $res{ T }) if ($res{ T });
  printf("|| Waiting       ||    %4d ||\n", $res{ W }) if ($res{ W });
#  print "-"x30 . "\n\n";
  print "\n";

  sleep 20;
  %res = ();

}
