package EASIH::JMS::Hive::DetachedDummy;

use EASIH::JMS::Hive;
use EASIH::JMS;

use strict;
use warnings;


use base(qw(EASIH::JMS::Hive));


my %stats;


my $executer = "/home/kb468/easih-pipeline/scripts/dummies/qstat.pl";
#my $executer = "/home/brugger/projects/easih-flow/scripts/dummies/qstat.pl";

# 
# 
# 
# Kim Brugger (24 Jun 2010)
sub stats {
  my ($self, $new_stats ) = @_;
  %stats = %$new_stats if ( $new_stats );
  return \%stats;
}


# 
# 
# 
# Kim Brugger (18 May 2010)
sub submit_job {
  my ($self, $cmd, $limit) = @_;
  
  $cmd = -10;

  open (my $qpipe, " $executer  | ") || die "Could not open qsub-pipe: $!\n";
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
  my ($self, $job_id) = @_;

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
