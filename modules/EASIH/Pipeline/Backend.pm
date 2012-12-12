package EASIH::Pipeline::Backend;

use EASIH::Pipeline::Backend::DetachedDummy;
use EASIH::Pipeline::Backend::Local;
use EASIH::Pipeline::Backend::Darwin;
#use EASIH::Pipeline::Backend::MPIexec;
use EASIH::Pipeline::Backend::SGE;

use strict;
use warnings;


# 
# 
# 
# Kim Brugger (18 May 2010)
sub submit_job {

  die "submit_job is not implemented for this HIVE\n";

}

# 
# 
# 
# Kim Brugger (18 May 2010)
sub job_status {

  die "pull_job is not implemented for this HIVE\n";
 
}


# 
# 
# 
# Kim Brugger (18 May 2010)
sub kill {

  die "kill is not implemented for this HIVE\n";
 
}

sub job_runtime {
#  print "job_runtime has not been implemented for the hive you are using!\n";
  return 0;

}

sub job_memory {
#  print "job_memory has not been implemented for the hive you are using!\n";
  return 0;
}



sub stats {
#  print "stats has not been implemented for the hive you are using!\n";
  return 0;
}



1;
