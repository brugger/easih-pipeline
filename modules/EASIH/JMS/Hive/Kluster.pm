package EASIH::JMS::Hive::Kluster;

use EASIH::JMS::Hive;
use EASIH::JMS;

use strict;
use warnings;
use POSIX ":sys_wait_h";
use Time::HiRes;


our %stats;


use base qw(EASIH::JMS::Hive);

my $executer = "/home/brugger/projects/easih-flow/scripts/dummies/local.pl";

my $max_jobs = 8;
my @running_jobs;
my @waiting_jobs;


# 
# 
# 
# Kim Brugger (18 May 2010)
sub submit_job {
  my ( $cmd, $limit) = @_;
  
  $cmd = $executer;

  my $cpid = create_child( $cmd );

  $stats{ $cpid }{start} = Time::HiRes::gettimeofday;
  
  return $cpid;
}


# 
# 
# 
# Kim Brugger (18 May 2010)
sub job_status {
  my ( $job_id) = @_;

#  print "job_id == $job_id\n";

  my $kid = waitpid($job_id, WNOHANG);
  my $status = $?;

#  print "$kid == $status\n";

  return $EASIH::JMS::RUNNING  if ( $kid == 0);
  return $EASIH::JMS::FAILED   if ( $status != 0 );
  $stats{ $kid }{ end } = Time::HiRes::gettimeofday if ( $kid );
  return $EASIH::JMS::FINISHED if ( $status == 0 );
  
  return $JMS::UNKNOWN;
}


sub create_child {
  my ($command) = @_;

  my $pid;
  if ($pid = fork) {
    ;
#    print "I think I forked, ups\n";
  } 
  else {
    die "cannot fork: $!" unless defined $pid;
    system($command);
    
    exit 1 if ( $? );
    exit 0;
  }
  
  return $pid;
}



# 
# 
# 
# Kim Brugger (27 May 2010)
sub job_runtime {
  my ($job_id ) = @_;

  return 0 if ( !$stats{$job_id}{end} || ! $stats{$job_id}{start});
  my $runtime = $stats{$job_id}{end} - $stats{$job_id}{start};
  return $runtime;
}


1;
