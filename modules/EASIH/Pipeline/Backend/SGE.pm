package EASIH::Pipeline::Backend::SGE;

use strict;
use warnings;


use EASIH::Pipeline;
use base(qw(EASIH::Pipeline::Backend));


my %stats;

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

#  print "-->>> cd $EASIH::Pipeline::cwd; $cmd \n";


  my ($tmp_fh, $tmp_file) = File::Temp::tempfile(DIR => "./tmp" );
  $tmp_file .= ".sge";
  open (my $qpipe, " | qsub -cwd -S /bin/sh  > $tmp_file 2> /dev/null ") || die "Could not open qsub-pipe: $!\n";
  print $qpipe "cd $EASIH::Pipeline::cwd; $cmd";
  close( $qpipe );
  
#  print "$cmd \n" if ( $verbose );
  my $job_id = -100;
    
  if ( -s $tmp_file ) { 
    open (my $tfile, $tmp_file) || die "Could not open '$tmp_file':$1\n";
    while(<$tfile>) {
      chomp;
      
      $job_id = $1 if ( /Your job (\d+) \(.*/);
    }
    close ($tfile);
    $job_id =~ s/(\d+?)\..*/$1/;
  }
  
#  system "rm $tmp_file" if ( $job_id != -100 );
  
  return $job_id;
}



# 
# 
# 
# Kim Brugger (18 May 2010)
sub job_status {
  my ($self, $job_id) = @_;


#  print "$job_id\n";

  return $EASIH::Pipeline::FAILED if ( $job_id == -100);
  
  use XML::Simple;

  my $xml;
  open (my $qspipe, "qstat -f -j $job_id -xml 2> /dev/null | ") || die "Could not open 'qstat1-pipeline': $!\n";
  $xml = join("", <$qspipe>);
  close( $qspipe );

  if ( $xml && $xml !~ /unknown_jobs/) {
    my $sge_stats = XMLin( $xml );

    $stats{$job_id}{'stderr_file'} = $$sge_stats{'djob_info'}{'element'}{'JB_stderr_path_list'}{'path_list'}{'PN_path'};
    $stats{$job_id}{'stdout_file'} = $$sge_stats{'djob_info'}{'element'}{'JB_stdout_path_list'}{'path_list'}{'PN_path'};
  }

  my %res;

  open ( $qspipe, "qstat  | egrep '^ +$job_id +'  2> /dev/null | ") || die "Could not open 'qstat2-pipeline': $!\n";
  while(<$qspipe>) {
    chomp;
    (undef, undef, undef, undef, undef, my $state, undef) = split(/\s+/);
    $res{job_state} = $state;
  }
  close($qspipe);
  
  open ( $qspipe, "qacct -j $job_id  2> /dev/null | ") || die "Could not open 'qacct-pipeline': $!\n";
  while(<$qspipe>) {
    chomp;
    next if (/=======/);
    my ($id, $value) = split(/\s+/, $_, 2);
    $value =~ s/\s+\z//;
    $res{ $id } = $value;
  }
  close ($qspipe);

#  use Data::Dumper;
#  print Dumper( \%res ) if ( %res );


  if (defined $res{'exit_status'}) {
    $stats{ $job_id }{ 'runtime' } = $res{'ru_wallclock'};
    $stats{ $job_id }{ 'memory' } = $res{'maxvmem'};
    if ($stats{ $job_id }{ 'memory' } =~ /M/) {
      $stats{ $job_id }{ 'memory' } =~ s/M//;
      $stats{ $job_id }{ 'memory' } *= 1000000;
    }    
    elsif ($stats{ $job_id }{ 'memory' } =~ /K/) {
      $stats{ $job_id }{ 'memory' } =~ s/K//;
      $stats{ $job_id }{ 'memory' } *= 1000;
    }    

#    print "$job_id is finished -- ";
    if ( $res{exit_status} == 0) {      
      # Remove the darwin logfiles, as we succeeded and do not need them anymore...
      if ( $stats{ $job_id }{'stderr_file'} ) {
	system "rm -f $stats{ $job_id }{'stderr_file'}";
      }
      if ( $stats{ $job_id }{'stdout_file'} ) {
	system "rm -f $stats{ $job_id }{'stdout_file'}";
      }

#      print "successfully\n";
      return $EASIH::Pipeline::FINISHED 
    }
#    print "and failed\n";
    
    return $EASIH::Pipeline::FAILED   if ( $res{exit_status} != 0);
  }

  return $EASIH::Pipeline::RUNNING  if ( $res{job_state} && $res{job_state} eq "r");
  return $EASIH::Pipeline::QUEUEING if ( $res{job_state} && ($res{job_state} =~/q/ || $res{job_state} =~/w/));

  return $EASIH::Pipeline::UNKNOWN;
}




# 
# 
# 
# Kim Brugger (06 Jan 2011)
sub kill {
  my ($self, $job_id) = @_;
  system "qdel $job_id 2> /dev/null";
}


# 
# 
# 
# Kim Brugger (27 May 2010)
sub job_runtime {
  my ($self, $job_id ) = @_;

  return $stats{$job_id}{runtime};
}


# 
# 
# 
# Kim Brugger (27 May 2010)
sub job_memory {
  my ($self, $job_id ) = @_;

  
  my $mem_usage = $stats{$job_id}{memory };

  return undef if ( ! defined $mem_usage);

  return 0 if ( ! defined $mem_usage);

  if ( $mem_usage =~ /(\d+)kb/i) {
    $mem_usage = $1* 1000;
  }
  elsif ( $mem_usage =~ /(\d+)mb/i) {
    $mem_usage = $1* 1000000;
  }
  elsif ( $mem_usage =~ /(\d+)gb/i) {
    $mem_usage = $1* 1000000000;
  }
  $stats{$job_id}{memory} = $mem_usage;

  return $stats{$job_id}{memory};
}



1;
