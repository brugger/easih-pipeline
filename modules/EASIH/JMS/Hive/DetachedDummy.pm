package EASIH::JMS::Hive::DetachedDummy;

use EASIH::JMS::Hive;
use EASIH::JMS;

use strict;
use warnings;


#@ISA = qw(EASIH::JMS::Hive);


my $executer = "/home/kb468/projects/easih-flow/scripts/dummies/qstat.pl";


# 
# 
# 
# Kim Brugger (18 May 2010)
sub submit_job {
  my ( $cmd, $limit) = @_;
  
  $cmd = -10;

  open (my $qpipe, " $executer $cmd | ") || die "Could not open qsub-pipe: $!\n";
  my $job_id = <$qpipe>;
  close ( $qpipe);  
  $job_id =~ s/\n//;
  $job_id =~ s/\r//;
  
  return $job_id;
}



# 
# 
# 
# Kim Brugger (18 May 2010)
sub job_status {
  my ( $job_id) = @_;

  open (my $qpipe, " $executer $job_id | ") || die "Could not open qsub-pipe: $!\n";
  my $status = <$qpipe>;
  close ( $qpipe);  
  $status =~ s/\n//;
  $status =~ s/\r//;


  return $EASIH::JMS::FINISHED if ( $status eq "Done" );
  return $EASIH::JMS::FAILED   if ( $status eq "Failed" );
  return $EASIH::JMS::RUNNING  if ( $status eq "Running" );
    
  
  return $JMS::UNKNOWN;
}



1;
