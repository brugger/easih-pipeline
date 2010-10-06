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

my $executer = "/home/kb468/easih-pipeline/scripts/dummies/local.pl";



our %analysis = ('start'            => { function   => 'single',
					 hpc_param  => "-NEP-fqs -l nodes=1:ppn=1,mem=500mb,walltime=02:00:00"},
					 
		 'fastq-split'      => { function   => 'multiple',
					 hpc_param  => "-NEP-fqs -l nodes=1:ppn=1,mem=500mb,walltime=02:00:00"},
		 
		 'std-aln'          => { function   => 'single',
					 hpc_param  => "-NEP-fqs -l nodes=1:ppn=1,mem=2500mb,walltime=12:00:00"},
		 
		 'std-generate'     => { function   => 'single',
					 hpc_param  => "-NEP-fqs -l nodes=1:ppn=1,mem=2500mb,walltime=10:00:00",},

		 'std-tag_sam'      => { function   => 'single',
					 hpc_param  => "-NEP-fqs -l nodes=1:ppn=1,mem=2500mb,walltime=10:00:00",},
		 
		 'std-sam2bam'      => { function   => 'single',
					 hpc_param  => "-NEP-fqs -l nodes=1:ppn=1,mem=1000mb,walltime=10:00:00"},
		 
		 'std-merge'        => { function   => 'single',
					 hpc_param  => "-NEP-fqs -l nodes=1:ppn=1,mem=2500mb,walltime=08:00:00",
					 sync       => 1},

		 'get_mapped'       => { function   => 'multiple_fail',
					 hpc_param  => "-NEP-fqs -l nodes=1:ppn=1,mem=2500mb,walltime=08:00:00"},
		 
		 'get_unmapped'     => { function   => 'single',
					 hpc_param  => "-NEP-fqs -l nodes=1:ppn=1,mem=2500mb,walltime=08:00:00"},
		 

		 're-aln'           => { function   => 'single',
				       hpc_param  => "-NEP-fqs -l nodes=1:ppn=1,mem=2500mb,walltime=12:00:00"},
		 
		 're-generate'      => { function   => 'single',
					 hpc_param  => "-NEP-fqs -l nodes=1:ppn=1,mem=2500mb,walltime=10:00:00",},

		 're-sam2bam'       => { function   => 'single',
					 hpc_param  => "-NEP-fqs -l nodes=1:ppn=1,mem=1000mb,walltime=10:00:00"},
		 
		 'initial_merge'    => { function   => 'single',
					 hpc_param  => "-NEP-fqs -l nodes=1:ppn=1,mem=2500mb,walltime=08:00:00",
					 sync       => 1},

		 'initial_sort'         => { function   => 'single',
					 hpc_param  => "-NEP-fqs -l nodes=1:ppn=1,mem=20000mb,walltime=08:00:00"},
		 

		 'initial_index'    =>{ function   => 'single',
					hpc_param  => "-NEP-fqs -l nodes=1:ppn=1,mem=2000mb,walltime=04:00:00"},
		 



		 'get_all_mapped'   => { function   => 'multiple',
					 hpc_param  => "-NEP-fqs -l nodes=1:ppn=1,mem=2500mb,walltime=08:00:00"},
		 
		 'get_all_unmapped' => { function   => 'single',
					 hpc_param  => "-NEP-fqs -l nodes=1:ppn=1,mem=2500mb,walltime=08:00:00"},
		 
		 'identify_indel'   => { function   => 'single',
					 hpc_param  => "-NEP-fqs -l nodes=1:ppn=1,mem=2500b,walltime=02:00:00"},
		 
		 'realign_indel'    => { function   => 'single',
					 hpc_param  => "-NEP-fqs -l nodes=1:ppn=1,mem=2500b,walltime=02:00:00"},

		 'merge_realigned'     => { function   => 'single',
					 hpc_param  => "-NEP-fqs -l nodes=1:ppn=1,mem=2500mb,walltime=08:00:00",
					 sync       => 1},

		 'sort_realigned'    => { function   => 'single',
					 hpc_param  => "-NEP-fqs -l nodes=1:ppn=1,mem=20000mb,walltime=08:00:00"},
		 

		 'index_realigned'   =>{ function   => 'single',
					hpc_param  => "-NEP-fqs -l nodes=1:ppn=1,mem=2000mb,walltime=04:00:00"},
		 
		 'call_indels'      => { function   => 'multiple',
					 hpc_param  => "-NEP-fqs -l nodes=1:ppn=1,mem=2500b,walltime=12:00:00"},

		 'merge_indels'     => { function   => 'single',
					 sync       => 1},


		 'identify_snps'    => { function   => 'multiple',
					 hpc_param  => "-NEP-fqs -l nodes=1:ppn=1,mem=2500b,walltime=02:00:00"},
		 
		 'filter_snps'      => { function   => 'single',
					 hpc_param  => "-NEP-fqs -l nodes=1:ppn=1,mem=500b,walltime=01:00:00"},
		 

		 'merge_vcfs'       => { function   => 'single',
					 hpc_param  => "-NEP-fqs -l nodes=1:ppn=1,mem=500mb,walltime=02:00:00", 
					 sync       => 1},
		 
		 'cluster_snps'     => { function   => 'single',
					 hpc_param  => "-NEP-fqs -l nodes=1:ppn=1,mem=2500mb,walltime=01:00:00"},
		 
		 'rescore_snps'     => { function   => 'single',
					 hpc_param  => "-NEP-fqs -l nodes=1:ppn=1,mem=50000mb,walltime=01:00:00"},		 
		 
		 

    );
		     


our %flow = ( 'start'            => 'fastq-split',
	      'fastq-split'      => "std-aln",

	      'std-aln'          => "std-generate",
	      'std-generate'     => "std-tag_sam",
	      'std-tag_sam'      => 'std-sam2bam',
	      'std-sam2bam'      => "std-merge",
	      'std-merge'        => ["get_unmapped", "get_mapped"],

	      "get_mapped"       => 'initial_merge',
	      "get_unmapped"     => 're-aln',
	      're-aln'           => 're-generate',
	      're-generate'      => 're-sam2bam',
	      're-sam2bam'       => 'initial_merge',
	      'initial_merge'    => 'initial_sort',
	      'initial_sort'     => "initial_index",

	      "initial_index"    => ['get_all_mapped', 'get_all_unmapped'],
	      'get_all_unmapped' => 'merge_realigned',
	      'get_all_mapped'   => 'identify_indel',
	      'identify_indel'   => 'realign_indel',
	      'realign_indel'    => 'merge_realigned',
	      'merge_realigned'  => 'sort_realigned',
	      'sort_realigned'   => 'index_realigned',

	      'index_realigned'  => ['call_indels','identify_snps'],
	      'call_indels'      => 'merge_indels',

	      'identify_snps'    => 'filter_snps',
	      'filter_snps'      => 'merge_vcfs',
	      'merge_vcfs'       => 'cluster_snps',
	      'cluster_snps'     => 'rescore_snps'
	      );



my %opts;
getopts('R:', \%opts);

#EASIH::JMS::no_store();
#EASIH::JMS::print_flow('fastq-split');

#EASIH::JMS::backend('Darwin');
EASIH::JMS::backend('Local');
EASIH::JMS::max_retry(3);

if ( $opts{R} ) {
  &EASIH::JMS::restore_state($opts{R});
  getopts('1:2:nm:R:d:f:o:r:p:hls', \%opts);
  &EASIH::JMS::hard_reset();
#  &EASIH::JMS::reset();
  &EASIH::JMS::no_store();
  EASIH::JMS::run('start');
}
else {
  EASIH::JMS::run('start');
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
