package EASIH::JMS;
# 
# JobManagementSystem for running simple pipelines on the HPC.
# 
# 
# Kim Brugger (23 Apr 2010), contact: kim.brugger@easih.ac.uk

use strict;
use warnings;
use Data::Dumper;

my $max_jobs;
my $save_state;
my $last_save_state;
my $update_job_array;
my $verbosity;

my @delete_files;
my @inputs;
my @jobs;
my $cwd      = `pwd`;
chomp($cwd);





# 
# 
# 
# Kim Brugger (23 Apr 2010)
sub cwd {
  my ($new_cwd) = @_;
  $cwd = $new_cwd;
}


# 
# 
# 
# Kim Brugger (23 Apr 2010)
sub submit_jobs {
  my ($cmds, $hpc_params) = @_;

  foreach my $cmd (@$cmds) {
    my $job_id = submit_job( $cmd, $hpc_params);
  }
}


# 
# 
# 
# Kim Brugger (22 Apr 2010)
sub submit_job {
  my ($cmd, $hpc_params) = @_;
  my ($tmp_fh, $tmp_file) = File::Temp::tempfile(DIR => "./tmp" );

  open (my $qpipe, " | qsub $hpc_params -o q-logs > $tmp_file 2> /dev/null ") || die "Could not open qsub-pipe: $!\n";
  print $qpipe "cd $cwd; $cmd";
  close( $qpipe );

  open (my $tfile, $tmp_file) || die "Could not open '$tmp_file':$1\n";
  my $job_id;
  while(<$tfile>) {
    chomp;
    $job_id = $_;
  }
  close ($tfile);
  system "rm $tmp_file";    

  $job_id =~ s/(\d+?)\..*/$1/;
  push @jobs, $job_id;
}


# 
# 
# 
# Kim Brugger (22 Apr 2010)
sub wait_jobs {
  my (@job_ids) = @_;
  
  my %s2status = ( C =>  "Completed",
                   E =>  "Exiting",
		   F =>  "Failed",
                   H =>  "Halted",
                   Q =>  "Queued",
                   R =>  "Running",
                   T =>  "Moving",
                   W =>  "Waiting",
                   S =>  "Suspend" );


  my %job_hash;
  map { $job_hash{ $_ }{ full_status } = "UNKNOWN"  } @job_ids;


  my ( $done, $running, $waiting, $queued, $failed, $other, ) = (0,0,0,0,0, 0);
  while (1) {
    
    ( $done, $running, $waiting, $queued, $failed, $other, ) = (0,0,0,0,0, 0);
    foreach my $job ( keys %job_hash ) {
      
      my ($status, $status_hash) = job_stats( $job );

#      print "STATUS $status\n";

      $done++    if ($status eq 'C');
      $running++ if ($status eq 'R');
      $queued++  if ($status eq 'Q');
      $running++ if ($status eq 'W');
      $failed++  if ($status eq 'F');
      $other++   if ($status ne 'R' && $status ne 'W' && $status ne 'Q' && $status ne 'C');

      $job_hash{ $job }{ status }      = $status;
      $job_hash{ $job }{ full_status } = $s2status{ $status };
    }

    print "Job tracking stats: D: $done, R: $running, Q: $queued, W: $waiting, F: $failed, O: $other\n";
    last if ( $done+$failed == @job_ids);

    sleep(10);

  }      

  if ( $failed ) {
    print STDERR "Failed on $failed job(s), will store current state and terminate run\n";
  }

  return;
}




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
    if (/ *(\w+) = (.*)/ ) {
      $res{$id} = $value if ( $id && $value);
      $id    = $1;
      $value = $2;
    }
    elsif (/\t(.*)/) {
      $value .= $1;
    }
  }
    
#  print Dumper( \%res );

  $res{job_state} = "F" if ( $res{exit_status} && $res{exit_status} != 0);

  return $res{job_state}, \%res;
  
}




# 
# 
# 
# Kim Brugger (22 Apr 2010)
sub tmp_file {
  my ($postfix) = @_;
  $postfix ||= "";
  my ($tmp_fh, $tmp_file) = File::Temp::tempfile(DIR => "./tmp" );

  push @delete_files, "$tmp_file$postfix";

  return "$tmp_file$postfix";
}



# 
# 
# 
# Kim Brugger (22 Apr 2010)
sub delete_tmp_file {

  system "rm @delete_files";
}


# 
# 
# 
# Kim Brugger (23 Apr 2010)
sub push_input {
  my ($input) = @_;
  push @inputs, $input;
}


# 
# 
# 
# Kim Brugger (23 Apr 2010)
sub pop_input {
  my ($input) = @_;
  return pop @inputs;
}


# 
# 
# 
# Kim Brugger (23 Apr 2010)
sub shift_input {
  return pop @inputs;
}


# 
# 
# 
# Kim Brugger (23 Apr 2010)
sub unshift_input {
  my ($input) = @_;
  unshift @inputs, $input;
}


# 
# 
# 
# Kim Brugger (23 Apr 2010)
sub reset_inputs {
  @inputs = ();
}

# 
# 
# 
# Kim Brugger (23 Apr 2010)
sub fetch_inputs {
  return @inputs;
}

# 
# 
# 
# Kim Brugger (23 Apr 2010)
sub fetch_n_reset_inputs {
  my @local_inputs = @inputs;
  reset_inputs();
  return @local_inputs;
}




# 
# 
# 
# Kim Brugger (23 Apr 2010)
sub run_flow {
  my ( $start_logic_name ) = @_;
  no strict;

  die "EASIH::JMS::run_flow not called with a logic_name\n" if (! $start_logic_name);

  my ($current_logic_name, $next_logic_name) = ($start_logic_name, $main::flow{ $start_logic_name});
  while (1) {
    
    if ( ! $main::analysis{$current_logic_name} ) {
      print "ERROR :::: No infomation on on $current_logic_name in main::analysis\n";
    }
    else {
      my $function = $main::analysis{$current_logic_name}{ function };
      
      if ( ! main->can( $function ) ){
	print "ERROR :::: $current_logic_name points to $function, but this does not exist!\n";
      }
      $function = "main::$function";
      &$function( $main::analysis{$current_logic_name}{ hpc_param }  );
    }

    print "Going from $current_logic_name --> $next_logic_name\n";
    
    if ( ! $main::flow{ $next_logic_name}) {
      print "end of flow\n";
      last;
    }
    else {
      $current_logic_name = $next_logic_name;
      $next_logic_name    = $main::flow{ $current_logic_name};
    }
  }
  
}


# 
# 
# 
# Kim Brugger (23 Apr 2010)
sub validate_flow {
  my ( $start_logic_name ) = @_;

  die "EASIH::JMS::test_flow not called with a logic_name\n" if (! $start_logic_name);

  print "Start test flow:\n";
  my ($current_logic_name, $next_logic_name) = ($start_logic_name, $main::flow{ $start_logic_name});
  while (1) {
    
    if ( ! $main::analysis{$current_logic_name} ) {
      print "ERROR :::: No infomation on on $current_logic_name in main::analysis\n";
    }
    else {
      my $function = $main::analysis{$current_logic_name}{ function };

      if ( ! main->can( $function ) ){
	print "ERROR :::: $current_logic_name points to $function, but this does not exist!\n";
      }

    }

    
    if ( ! $main::flow{ $next_logic_name}) {
      print "Going from $current_logic_name --> $next_logic_name\n";
      last;
    }
    else {
      print "Going from $current_logic_name --> $next_logic_name\n";
      $current_logic_name = $next_logic_name;
      $next_logic_name    = $main::flow{ $current_logic_name};
    }
  }
  print "end of flow\n";
  
}










