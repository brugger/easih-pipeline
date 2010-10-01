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
use Time::HiRes;

my $port = shift || 8888;
my $fail_rate = 10;
my $run_time  = 10;

my $server =  IO::Socket::INET->new( LocalPort => $port,
				     Type      => SOCK_STREAM,
				     reuse     => 1,
				     Listen    => 10) || die "Cannot create port $port: $!\n";


my %jobs = ();

my %done = ();

srand();


my $job_id = 1000;
while ( my $client = $server->accept()) {
  my $request = <$client>;

  $request =~ s/\n//;;
  $request =~ s/\r//;;

#  print Dumper( $request );

#  print Dumper( \%jobs );

  print "[ '$request' ]\n";
  print "Now: " . Time::HiRes::gettimeofday() . " vs $jobs{ $request } \n" if ( $jobs{ $request } );
  if (! $jobs{ $request } || ! $request ) {

    my $endtime = Time::HiRes::gettimeofday() + int(rand( $run_time ));

    $jobs{ $job_id} = $endtime;
    print $client "$job_id";
    $job_id++;

  }
  else {
    print $done{ $request } if ( $done{ $request });
    if ( $jobs{ $request } < Time::HiRes::gettimeofday()) {
      if ( $fail_rate > int(rand(100))) {
	$done{ $request } = "Failed";
	print $client "Failed";
      }
      else {
	$done{ $request } = "Done";
	print $client "Done";
      }
    }
    else {
      print $client "Running";
    }
    
    
  }
  
}
  


sub catch_ctrl_c {
    print "Caught a ctrl-c\n";
    close($server);
}


BEGIN {
  $SIG{INT} = \&catch_ctrl_c;
}
