#!/usr/bin/perl 
# 
# 
# 
# 
# Kim Brugger (22 Apr 2010), contact: kim.brugger@easih.ac.uk

use strict;
use warnings;
use Data::Dumper;
use File::Temp;

use Getopt::Std;

my %job_stats;


my $fq_split = '/home/kb468/bin/fastq_split.pl';
my $bwa      = '/home/kb468/bin/bwa';
my $samtools = '/home/kb468/bin/samtools';

use lib '/home/kb468/projects/easih-pipeline/modules';
use lib '/home/brugger/projects/easih-pipeline/modules';
use lib '/home/kb468/easih-pipeline/modules';
use EASIH::JMS;

our %analysis = ('fastq-split'    => { function   => 'fastq_split',
				       hpc_param  => "-NEP-fqs -l nodes=1:ppn=1,mem=500mb,walltime=00:01:00"},
		 
		 'BWA-mapping'    => { function   => 'bwa_aln',
				       hpc_param  => "-NEP-fqs -l nodes=1:ppn=1,mem=1000mb,walltime=00:01:00"},
		 
		 'BWA-samse'      => { function   => 'bwa_samse',
				       hpc_param  => "-NEP-fqs -l nodes=1:ppn=1,mem=1000mb,walltime=00:01:00"},
		 
		 'SAM2BAM'        => { function   => 'sam2bam',
				       hpc_param  => "-NEP-fqs -l nodes=1:ppn=1,mem=500mb,walltime=00:01:00"},
		 
		 'BWA-merge'      => { function   => 'bwa_merge',
				       hpc_param  => "-NEP-fqs -l nodes=1:ppn=1,mem=500mb,walltime=00:01:00",
				       sync       => 1},
		 
		 'samtools-sort'  => { function   => 'samtools_sort',
				       hpc_param  => "-NEP-fqs -l nodes=1:ppn=1,mem=1000mb,walltime=00:01:00"},
		 
		 'bam-rename'     => { function   => 'rename'},
		 
		 'samtools-index' =>{ function   => 'samtools_index',
				      hpc_param  => "-NEP-fqs -l nodes=1:ppn=1,mem=1000mb,walltime=00:01:00"},
    );
		     

our %flow = ( 'fastq-split'   => "BWA-mapping",
	      'BWA-mapping'   => "BWA-samse",
	      'BWA-samse'     => "SAM2BAM",
	      'SAM2BAM'       => "BWA-merge",
	      'BWA-merge'     => "samtools-sort",
	      'samtools-sort' => "bam-rename",
	      'bam-rename'    => "samtools-index");



my %opts;
getopts('w:i:q:o:p:n:r:', \%opts);

if ( $opts{ r} ) {
  &EASIH::JMS::restore_state($opts{r});
#  EASIH::JMS::print_HPC_usage();
#  exit;
  getopts('w:i:q:o:p:n:', \%opts);
#  EASIH::JMS::reset();
}


my $infile  = $opts{'i'} || "1_1_1.fastq" || usage();
my $outfile = $opts{'o'} || "1_1_1.fastq.bam" || usage();
my $prefix  = $opts{'p'} || " /home/kb468/Hs/hs_GRCh37" || usage();
my $split   = $opts{'n'} || 10000 || 30000000;

my $dummy = '/home/kb468/easih-pipeline/scripts/dummies/local.pl';

#push @EASIH::JMS::jobs, '1003679';
#EASIH::JMS::wait_jobs( );

EASIH::JMS::verbosity(10);
EASIH::JMS::hive('Darwin');
#EASIH::JMS::validate_flow('fastq-split');
#exit;
EASIH::JMS::run('fastq-split');
#EASIH::JMS::dry_run('fastq-split');
#EASIH::JMS::delete_tmp_files();



sub fastq_split {
  my ($input) = @_;

  for (my $i = 0; $i < 4; $i++ ) {
    my $cmd = $dummy;
    my $tmp_file = EASIH::JMS::tmp_file();
    EASIH::JMS::submit_job($cmd, $tmp_file);
  }
}

sub bwa_aln {
  my ($input) = @_;
  
  my $tmp_file = EASIH::JMS::tmp_file();
  my $cmd = "$dummy";
  EASIH::JMS::submit_job($cmd, $tmp_file);
}

sub bwa_samse {
  my ($input) = @_;

  my $tmp_file = EASIH::JMS::tmp_file();
  my $cmd = "$dummy";
  EASIH::JMS::submit_job($cmd, $tmp_file);
}

sub sam2bam {
  my ($input) = @_;

  my $tmp_file = EASIH::JMS::tmp_file();
  my $cmd = "$dummy";
  EASIH::JMS::submit_job($cmd, $tmp_file);
}

sub bwa_merge { 
  my (@inputs) = @_;

  my $tmp_file = EASIH::JMS::tmp_file();
  my $cmd = "$dummy";
  EASIH::JMS::submit_job($cmd, $tmp_file);
}

sub samtools_sort {
  my ($input) = @_;

  my $tmp_file = EASIH::JMS::tmp_file();
  my $cmd = "$dummy";
  EASIH::JMS::submit_job($cmd, $tmp_file);
}

sub rename {
  my ($input) = @_;

  my $cmd = "touch $outfile ";
  eval { system "$cmd" };
  EASIH::JMS::fail($@) if ($@);
}

sub samtools_index {
  my ($input) = @_;

  my $tmp_file = EASIH::JMS::tmp_file();
  my $cmd = "$dummy";
  EASIH::JMS::submit_job($cmd, $tmp_file);
}		     




# 
# 
# 
# Kim Brugger (22 Apr 2010)
sub usage {
  
  print "not the right usage\n";
#  exit;

}
