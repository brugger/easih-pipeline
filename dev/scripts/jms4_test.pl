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

use lib '/home/kb468/easih-pipeline/modules';
use EASIH::JMS;
use EASIH::JMS::Misc;
use EASIH::JMS::Samtools;
use EASIH::JMS::Picard;

my $executer = "/home/kb468/easih-pipeline/dev/dummies/local.pl";



our %analysis = ('A'   => { function   => 'single'},
					 
		 'B'   => { function   => 'single_slow'},
		 
		 'C'   => { function   => 'single'},
		 
		 'D'   => { function   => 'multiple'},

		 'E'   => { function   => 'multiple'},
		 
		 'F'   => { function   => 'single_slow',
			    sync       => 1},
		 
		 'G'   => { function   => 'single',
			    sync       => 1},
    );
		     


our %flow = ( 'A'      => ['B','C'],
	      'B'      => ['D','F'],
	      'C'      => ['E','F'],
	      'D'      => 'G',
	      'E'      => 'G',
	      'F'      => 'G',);


my %opts;
getopts('R:', \%opts);

#EASIH::JMS::no_store();
#EASIH::JMS::print_flow('fastq-split');

#EASIH::JMS::backend('Darwin');
EASIH::JMS::backend('Local');
EASIH::JMS::max_retry(3);

if ( $opts{R} ) {
  &EASIH::JMS::reset($opts{R});
#  &EASIH::JMS::hard_reset( $opts{R} );
#  &EASIH::JMS::reset();
  &EASIH::JMS::no_store();
  EASIH::JMS::run('A');
}
else {
  EASIH::JMS::run('A');
  EASIH::JMS::store_state();
}


sub multiple {
  my ($input) = @_;

  my $cmd = "$executer";
  my $tmp_file = 'tyt';
  
  for ( my $i=0; $i< 3; $i++ ) {
    EASIH::JMS::submit_job("$cmd ", $tmp_file);
  }

}

sub multiple_fail {
  my ($input) = @_;

  my $cmd = "$executer";
  my $tmp_file = 'tyt';
  
  for ( my $i=0; $i< 3; $i++ ) {
    EASIH::JMS::submit_job("$cmd ", $tmp_file);
  }

  EASIH::JMS::submit_job("$cmd  ", $tmp_file);

}


sub single {
  my ($input) = @_;

  my $cmd = "$executer ";
  my $tmp_file = 'tyt';
  EASIH::JMS::submit_job("$cmd ", $tmp_file);
}

sub single_slow {
  my ($input) = @_;

  my $cmd = "$executer -S 60";
  my $tmp_file = 'tyt';
  EASIH::JMS::submit_job("$cmd ", $tmp_file);
}


# 
# 
# 
# Kim Brugger (09 Sep 2010)
sub single_fail {

  my ($input) = @_;

  my $cmd = "$executer ";
  my $tmp_file = 'tyt';
  EASIH::JMS::submit_job("$cmd -F", $tmp_file);
  
}




# 
# 
# 
# Kim Brugger (22 Apr 2010)
sub usage {
  
  print "Not the right usage, please look at the code\n";
  exit;

}
