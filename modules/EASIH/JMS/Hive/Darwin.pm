package EASIH::JMS::Hive::Darwin;

use EASIH::JMS::Hive;
use EASIH::JMS;



@ISA = ('EASIH::JMS::Hive');



# 
# 
# 
# Kim Brugger (18 May 2010)
sub submit_job {
  my ( $cmd, $limit) = @_;


  my ($tmp_fh, $tmp_file) = File::Temp::tempfile(DIR => "./tmp" );

  open (my $qpipe, " | qsub $hpc_params -o q-logs > $tmp_file 2> /dev/null ") || die "Could not open qsub-pipe: $!\n";
  print $qpipe "cd $EASIH::cwd; $cmd";
  close( $qpipe );
  
  print "$cmd \n" if ( $verbose );
  
  my $job_id = 0;
    
  if ( -s $tmp_file ) { 
    open (my $tfile, $tmp_file) || die "Could not open '$tmp_file':$1\n";
    while(<$tfile>) {
      chomp;
      $job_id = $_;
    }
    close ($tfile);
    $job_id =~ s/(\d+?)\..*/$1/;
  }
  
  return $job_id;
}



# 
# 
# 
# Kim Brugger (18 May 2010)
sub job_status {
  my ( $job_id) = @_;

# 
# 
# 
# Kim Brugger (22 Apr 2010)
sub job_stats {
  my ($job_id)  = @_;

  my %res;
  open (my $qspipe, "qstat -f $job_id 2> /dev/null | ") || die "Could not open 'qstat-pipeline': $!\n";
  my ( $id, $value);
  while(<$qspipe>) {
    chomp;
#    print "$_ \n";
    if (/(.*?) = (.*)/ ) {
      $res{$id} = $value if ( $id && $value);
      $id    = $1;
      $value = $2;
      $id    =~ s/^\s+//;
    }
    elsif (/\t(.*)/) {
      $value .= $1;
    }
  }


  
  return $JMS::FAILED   if ( $res{exit_status} && $res{exit_status} != 0);
  return $JMS::FINISHED if ( $res{exit_status});

  return $JMS::RUNNING  if ( $res{job_state} eq "R");
  return $JMS::QUEUEING if ( $res{job_state} eq "Q");

  return $JMS::UNKNOWN;

}



1;
