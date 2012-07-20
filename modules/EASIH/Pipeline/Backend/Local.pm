package EASIH::Pipeline::Backend::Local;

use EASIH::Pipeline::Backend;
use EASIH::Pipeline;

use strict;
use warnings;
use POSIX ":sys_wait_h";
use Time::HiRes;


our %stats;


use base qw(EASIH::Pipeline::Backend);


my $max_jobs = 8;
my @running_jobs;
my @waiting_jobs;


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

  my $cpid = create_child( $cmd );

  $stats{ $cpid }{ start } = Time::HiRes::gettimeofday;
  
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
  my $status = $? ;

  print "$job_id $kid == $status\n";

  return $EASIH::Pipeline::RUNNING  if ( $kid == 0);
  $stats{ $kid }{ end } = Time::HiRes::gettimeofday if ( $kid );
  return $EASIH::Pipeline::FINISHED if ( $status == 0 );
  return $EASIH::Pipeline::FAILED   if ( $status != 0 );
  
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

    # eval { system "$command" };
    # print "$pid --> $@\n";

    # if ( ! $@ ) {
    #   exit 0;
    # }
    # else {
    #   verbose("$@\n", 1);
    #   exit 1;
    # }

     system("$command");

     my $status = $?;	

     print "$pid ==> $? $status\n";
    
     exit 1 if ( $status );
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
