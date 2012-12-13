package EASIH::JMS::Backend;

use EASIH::JMS::Backend::DetachedDummy;
use EASIH::JMS::Backend::Local;
use EASIH::JMS::Backend::Darwin;
use EASIH::JMS::Backend::SGE;

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
