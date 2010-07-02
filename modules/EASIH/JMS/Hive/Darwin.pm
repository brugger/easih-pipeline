package EASIH::JMS::Hive::Darwin;

#use EASIH::JMS::Hive;
use EASIH::JMS;

#use strict;
use warnings;
@ISA = qw(EASIH::JMS::Hive);


#my %stats;

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

#  print "-->>> cd $EASIH::JMS::cwd; $cmd \n";


  my ($tmp_fh, $tmp_file) = File::Temp::tempfile(DIR => "./tmp" );

  open (my $qpipe, " | qsub $limit > $tmp_file 2> /dev/null ") || die "Could not open qsub-pipe: $!\n";
  print $qpipe "cd $EASIH::JMS::cwd; $cmd";
  close( $qpipe );
  
#  print "$cmd \n" if ( $verbose );
  
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
  my ($self, $job_id) = @_;

  my %res;
  open (my $qspipe, "qstat -f $job_id 2> /dev/null | ") || die "Could not open 'qstat-pipeline': $!\n";
  my ( $id, $value);
  while(<$qspipe>) {
    chomp;
    s/\r//g;
    if (/(.*?) = (.*)/ ) {
      $res{$id} = $value if ( $id && defined $value);
      $id    = $1;
      $value = $2;
      $id    =~ s/^\s+//;
    }
    elsif (/\t(.*)/) {
      $value .= $1;
    }
  }
  $res{$id} = $value if ( $id && defined $value);




  if ( $res{job_state} eq "C" ) {
    my ($hour, $min, $sec) = split(":", $res{'resources_used.walltime'});

    $stats{$job_id}{runtime} = $sec + 60 * $min + 3600 * $hour;
    $stats{$job_id}{memory } = $res{'resources_used.mem'};

    return $EASIH::JMS::FINISHED if ( $res{exit_status} == 0);

    return $EASIH::JMS::FAILED   if ( $res{exit_status} != 0);
  }

  return $EASIH::JMS::RUNNING  if ( $res{job_state} && $res{job_state} eq "R");
  return $EASIH::JMS::QUEUEING if ( $res{job_state} && $res{job_state} eq "Q");

  return $EASIH::JMS::UNKNOWN;

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
