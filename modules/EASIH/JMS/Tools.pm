package EASIH::JMS::Tools;

use EASIH::JMS;

# 
# 
# 
# Kim Brugger (28 Apr 2010)
sub find_input {
  my ($glob_pattern) = @_;
  my @fastq_files = glob "$glob_pattern";
  EASIH::JMS::push_input( @fastq_files );
}






1;
