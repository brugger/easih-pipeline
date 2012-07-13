package EASIH::Pipeline::Backend::MPIexec;

use EASIH::Pipeline::Backend;
use EASIH::Pipeline;

use strict;
use warnings;
use POSIX ":sys_wait_h";
use Time::HiRes;


our %stats;

my $i = 1;

use base qw(EASIH::Pipeline::Backend);


my $max_jobs = 8;


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
  
  print "-->> $cmd\n";

  use Sys::Hostname;
  my $host = hostname;

  my $error_file  = " $host.$i";

  $i++;

#  $cmd = "mpiexec -comm none -n 1 $cmd 2> $error_file";
#  $cmd = "mpiexec -comm none -n 1 $cmd ";
  my $command = "mpiexec -comm none -n 1 $cmd ";
  my $cpid = create_child( $command );

  $stats{ $cpid }{start} = Time::HiRes::gettimeofday;
  
  return $cpid;
}


# 
# 
# 
# Kim Brugger (18 May 2010)
sub job_status {
  my ($self, $job_id) = @_;

#  print "job_id == $job_id\n";

  my $kid = waitpid($job_id, WNOHANG);
  my $status = $?;

  print "$job_id -- $kid == $status\n";

  return $EASIH::Pipeline::FAILED   if ( $status != 0 );
  return $EASIH::Pipeline::RUNNING  if ( $kid == 0);
  $stats{ $kid }{ end } = Time::HiRes::gettimeofday;
  return $EASIH::Pipeline::FINISHED if ( $status == 0 );
  
  return $EASIH::Pipeline::UNKNOWN;
}


sub create_child {
  my ( $command) = @_;

  my $pid;
  if ($pid = fork) {
    ;
#    print "I think I forked, ups\n";
  } 
  else {
    die "cannot fork: $!" unless defined $pid;
    system("$command");
    
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
  my ($self, $job_id ) = @_;

  return 0 if ( !$stats{$job_id}{end} || ! $stats{$job_id}{start});
  my $runtime = $stats{$job_id}{end} - $stats{$job_id}{start};
  return $runtime;
}


1;
