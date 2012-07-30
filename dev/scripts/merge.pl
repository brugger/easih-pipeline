#!/usr/bin/perl 
# 
# Map/realign/indels/snps integrated pipeline
# 
# 
# Kim Brugger (27 Jul 2010), contact: kim.brugger@easih.ac.uk

use strict;
use warnings;
use Data::Dumper;

use Getopt::Std;

use lib '/home/kb468/projects/BRC_exomes/easih-pipeline/modules';
use EASIH::Pipeline;
#use EASIH::Pipeline::Samtools;
#use EASIH::Pipeline::Picard;

my $executer = "/home/kb468/projects/BRC_exomes/easih-pipeline/dev/dummies/local.pl";


EASIH::Pipeline::add_start_step('A', 'single');
EASIH::Pipeline::add_step('A', 'B', 'multiple');
EASIH::Pipeline::add_step('B', 'E', 'multiple');
EASIH::Pipeline::add_step('A', 'C', 'single_fast');
EASIH::Pipeline::add_step('C', 'D', 'single_slow');
EASIH::Pipeline::add_merge_step('E', 'M', 'single_fast');
EASIH::Pipeline::add_merge_step('D', 'M', 'single_fast');


my %opts;
getopts('R:', \%opts);

#EASIH::Pipeline::no_store();
#EASIH::Pipeline::print_flow('fastq-split');

#EASIH::Pipeline::backend('Darwin');
EASIH::Pipeline::backend('Local');
#EASIH::Pipeline::backend('SGE');
EASIH::Pipeline::max_retry(3);

EASIH::Pipeline::run();

sub multiple {
  my ($input) = @_;

  my $cmd = "$executer";
  my $tmp_file = 'tyt';
  
  for ( my $i=0; $i< 3; $i++ ) {
    EASIH::Pipeline::submit_job("$cmd ", $tmp_file);
  }

}

sub multiple_fail {
  my ($input) = @_;

  my $cmd = "$executer";
  my $tmp_file = 'tyt';
  
  for ( my $i=0; $i< 3; $i++ ) {
    EASIH::Pipeline::submit_job("$cmd ", $tmp_file);
  }

  EASIH::Pipeline::submit_job("$cmd  ", $tmp_file);

}


sub single {
  my ($input) = @_;

  my $cmd = "$executer ";
  my $tmp_file = 'tyt';
  EASIH::Pipeline::submit_job("$cmd ", $tmp_file);
}


sub single_fast {
  my ($input) = @_;

  my $cmd = "$executer ";
  my $tmp_file = 'tyt';
  EASIH::Pipeline::submit_job("$cmd ", $tmp_file);
}

sub single_slow {
  my ($input) = @_;

  my $cmd = "$executer -S 60";
  my $tmp_file = 'tyt';
  EASIH::Pipeline::submit_job("$cmd ", $tmp_file);
}


# 
# 
# 
# Kim Brugger (09 Sep 2010)
sub single_fail {

  my ($input) = @_;

  my $cmd = "$executer ";
  my $tmp_file = 'tyt';
  EASIH::Pipeline::submit_job("$cmd -F", $tmp_file);
  
}




# 
# 
# 
# Kim Brugger (22 Apr 2010)
sub usage {
  
  print "Not the right usage, please look at the code\n";
  exit;

}
