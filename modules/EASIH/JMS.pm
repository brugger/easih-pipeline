package EASIH::JMS;
# 
# JobManagementSystem frame for running (simple) pipelines on the HPC.
# 
# 
# Kim Brugger (23 Apr 2010), contact: kim.brugger@easih.ac.uk

use strict;
use warnings;
use Data::Dumper;
use Storable;
use File::Temp;

my $save_interval  =  0;
my $verbose        =  0;
my $max_retry      =  0;
my $pull_time      = 300;
my $current_logic_name;

my @delete_files;
my @_inputs;
my @prev_inputs;
my @jobs;
my $cwd      = `pwd`;
chomp($cwd);
my $dry_run  = 0;

# 
# 
# 
# Kim Brugger (23 Apr 2010)
sub verbosity {
  $verbose = shift || 0;
}


# 
# 
# 
# Kim Brugger (23 Apr 2010)
sub pull_time {
  $pull_time = shift || 60;
}


# 
# 
# 
# Kim Brugger (23 Apr 2010)
sub save_interval {
  $save_interval = shift || 0;
}


# 
# 
# 
# Kim Brugger (23 Apr 2010)
sub max_retry {
  $max_retry = shift || 0;
  $max_retry = 0 if ( $max_retry < 0);
}


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
    submit_job( $cmd, $hpc_params);
  }
}


# 
# submit some jobs and wait for them to finish.
# 
# Kim Brugger (23 Apr 2010)
sub submit_n_wait_jobs {
  my ($cmds, $hpc_params) = @_;

  foreach my $cmd (@$cmds) {
    submit_job( $cmd, $hpc_params);
  }

  wait_jobs( );
}

# 
# Submit a single job, and wait for it to finish
# 
# Kim Brugger (23 Apr 2010)
sub submit_n_wait_job {
  my ($cmd, $hpc_params) = @_;

  submit_job( $cmd, $hpc_params);
  wait_jobs( );
}


# 
# submit a single job to the HPC
# 
# Kim Brugger (22 Apr 2010)
sub submit_job {
  my ($cmd, $hpc_params) = @_;
  my ($tmp_fh, $tmp_file) = File::Temp::tempfile(DIR => "./tmp" );

  if ( $dry_run ) {
    print "echo 'cd $cwd; $cmd' |qsub $hpc_params \n";
    return;
  }

  open (my $qpipe, " | qsub $hpc_params -o q-logs > $tmp_file 2> /dev/null ") || die "Could not open qsub-pipe: $!\n";
  print $qpipe "cd $cwd; $cmd";
  close( $qpipe );

  print "$cmd \n" if ( $verbose );

  my $job_id = 'undefined';

  if ( -s $tmp_file ) { 
    open (my $tfile, $tmp_file) || die "Could not open '$tmp_file':$1\n";
    while(<$tfile>) {
      chomp;
      $job_id = $_;
    }
    close ($tfile);
    $job_id =~ s/(\d+?)\..*/$1/;
  }
  system "rm $tmp_file";    


  push @jobs, {job_id      => $job_id, 
	       full_status => 'SUBMITTED',
	       tracking    => 1,
	       command     => $cmd,
	       hpc_params  => $hpc_params,
	       logic_name  => $current_logic_name};
}



# 
# 
# 
# Kim Brugger (26 Apr 2010)
sub resubmit_job {
  my ( $job_ref ) = @_;

  my ($tmp_fh, $tmp_file) = File::Temp::tempfile(DIR => "./tmp" );

  if ( $dry_run ) {
    print "echo 'cd $cwd; $$job_ref{cmd}' |qsub $$job_ref{hpc_params} \n";
    return;
  }

  open (my $qpipe, " | qsub $$job_ref{hpc_params} -o q-logs > $tmp_file 2> /dev/null ") || die "Could not open qsub-pipe: $!\n";
  print $qpipe "cd $cwd; $$job_ref{cmd} ";
  close( $qpipe );

  print "$$job_ref{cmd} \n" if ( $verbose );

  open (my $tfile, $tmp_file) || die "Could not open '$tmp_file':$1\n";
  my $job_id;
  while(<$tfile>) {
    chomp;
    $job_id = $_;
  }
  close ($tfile);
  system "rm $tmp_file";    

  $job_id =~ s/(\d+?)\..*/$1/;

  push @jobs, {job_id      => $job_id, 
	       full_status => 'RESUBMITTED',
	       tracking    => 1};
 

}



# 
# Wait for the jobs to terminate
# 
# Kim Brugger (22 Apr 2010)
sub wait_jobs {

  return if ( $dry_run );

  my %s2status = ( C =>  "Completed",
                   E =>  "Exiting",
		   F =>  "Failed",
                   H =>  "Halted",
                   Q =>  "Queued",
                   R =>  "Running",
                   T =>  "Moving",
                   W =>  "Waiting",
                   S =>  "Suspend" );

  my ( $done, $running, $waiting, $queued, $failed, $other, ) = (0,0,0,0,0, 0);
  while (1) {
    
    ( $done, $running, $waiting, $queued, $failed, $other, ) = (0,0,0,0,0, 0);
    my $tracking_nr = 0;
    foreach my $job ( @jobs ) {
      
      # Only look at the jobs we are currently tracking
      next if ( ! $$job{tracking} );
      $tracking_nr++;
      my ($status, $status_hash) = job_stats( $$job{ job_id } );

#      print "STATUS '$status' $$job{ job_id }\n";
#      print Dumper( $status_hash);

      $$job{ status }      = $status;
      $$job{ full_status } = $s2status{ $status };
      $$job{ hpc_stats }   = $status_hash;

      # this should be done with switch, but as we are not on perl 5.10+ this is how it is done...
      if ($status eq 'C'){
	$done++;
      }
      elsif ($status eq 'R') {
	$running++;
      }
      elsif ($status eq 'Q'){
	$queued++; 
      }
      elsif ($status eq 'W'){
	$waiting++;
      }
      elsif ($status eq 'F') {
	$failed++;
	$$job{ failed }++;
	if ( $$job{ failed } < $max_retry ) {	  
	  resubmit_job( $job );
	}
      }
      else {
	$other++;
      }

    }

    use POSIX 'strftime';
    my $time = strftime('Today is %m/%d/%y: %H.%M:', localtime);


    print "[$time]: D: $done, R: $running, Q: $queued, W: $waiting, F: $failed, O: $other\n";
    last if ( $done+$failed == $tracking_nr);

    sleep( $pull_time );

  }      

  fail("Failed on $failed job(s), will store current state and terminate run\n")
      if ( $failed );

  unset_tracking();
  return;
}



# 
# 
# 
# Kim Brugger (26 Apr 2010)
sub unset_tracking {

  foreach my $job ( @jobs ) {
    $$job{tracking}  = 0;
  }


}




# 
# reset the failed states, so the pipeline can run again
# 
# Kim Brugger (26 Apr 2010)
sub reset {

  foreach my $key ( keys %main::analysis ) {
    if ($main::analysis{$key}{state} &&  $main::analysis{$key}{state} eq 'failed' ) {
      delete $main::analysis{$key}{state};
    }
  }

  # Only look at the jobs we are currently tracking
  foreach my $job ( @jobs ) {
    $$job{tracking} = 0  if ( $$job{failed} );
  }
}



# 
# reset the failed states, so the pipeline can run again
# 
# Kim Brugger (26 Apr 2010)
sub print_HPC_usage {

  my %summed = ();

  foreach my $job ( @jobs ) {
    my( $hours, $mins, $secs ) = split(":", $$job{ hpc_stats }{ 'resources_used.walltime' });
    $summed{ $$job{ logic_name } }{ walltime } += 3600*$hours + 60*$mins + $secs;
    $$job{ hpc_stats }{ 'resources_used.mem' } =~ s/kb//;
    $summed{ $$job{ logic_name } }{ max_memory } = $$job{ hpc_stats }{ 'resources_used.mem' } 
    if (! $summed{ $$job{ logic_name } }{ max_memory } || $summed{ $$job{ logic_name } }{ max_memory } < $$job{ hpc_stats }{ 'resources_used.mem' });
  }

  foreach my $key ( keys %summed ) {
    print "$key: cpus=$summed{ $key }{ walltime }, max_mem=$summed{ $key }{ max_memory } kb\n";
  }

#  print Dumper( \%summed );
}



# 
# reset the failed states, so the pipeline can run again
# 
# Kim Brugger (26 Apr 2010)
sub print_states {

  foreach my $key ( keys %main::analysis ) {
    if ( $main::analysis{$key}{state}) {
      print "$key ==> $main::analysis{$key}{state}\n";
    }
    else {
      print "$key ==> no state\n";
    }
  }
}



# 
# 
# 
# Kim Brugger (23 Apr 2010)
sub fail {
  my ( $message ) = @_;

  print STDERR "ERROR:: $message\n";
  $main::analysis{$current_logic_name}{state} = "failed";
  store_state();
  exit;
  
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
    
#  print Dumper( \%res );

  # it seems that qstat failed, revert to look at checkjob status as
  # that is kept for two days, and not only 30 min.
  if ( !  %res ) {

    # I have not implemented this yet, as the two formats differ 
    $res{job_state} = "L";


    open (my $cjpipe, "checkjob -v -v $job_id 2> /dev/null | ") || die "Could not open 'cjtat-pipeline': $!\n";
    my ( $id, $value);
    while(<$cjpipe>) {
      chomp;
    RELOOP:
#    print "$_ \n";
      if ( /^(.*?): (.*?) (\w+:.*)/ || /^(.*?): (.*?)/) {
	$id    = $1;
	$value = $2;
	$value =~ s/^\s+//;
	$res{$id} = $value if ( $id && $value);
	if ( $3) {
	  $_ = $3;
	  goto RELOOP;
	}
      }
      elsif (/\t(.*)/) {
	$value .= $1;
      }
    }
    
    
  }
    

  die Dumper( \%res );


  $res{job_state} = "F" if ( $res{exit_status} && $res{exit_status} != 0);

  return $res{job_state}, \%res;
  
}



# 
# 
# 
# Kim Brugger (22 Apr 2010)
sub tmp_file {
  my ($postfix, $keep_file) = @_;
  $postfix ||= "";
  $keep_file || 0;
  my ($tmp_fh, $tmp_file) = File::Temp::tempfile(DIR => "./tmp" );

  push @delete_files, "$tmp_file$postfix" if (! $keep_file);

  return "$tmp_file$postfix";
}



# 
# 
# 
# Kim Brugger (27 Apr 2010)
sub tag_for_deletion {
  my (@files) = @_;

  push @delete_files, @files;
  
}




# 
# 
# 
# Kim Brugger (26 Apr 2010)
sub delete_hpc_logs {
  
  my @files;
  foreach my $job ( @jobs ) {
    
    my ($host, $path) = split(":", $$job{ hpc_stats }{Error_Path});
    push @files, $path if ( -f $path);
    ($host, $path) = split(":", $$job{ hpc_stats }{Output_Path});
    push @files, $path if ( -f $path);

  }    

  system "rm @files";
}


# 
# 
# 
# Kim Brugger (22 Apr 2010)
sub delete_tmp_files {

  system "rm @delete_files";
}


# 
# 
# 
# Kim Brugger (23 Apr 2010)
sub push_input {
  my (@inputs) = @_;
  push @_inputs, @inputs;
}


# 
# 
# 
# Kim Brugger (23 Apr 2010)
sub pop_input {
  my ($input) = @_;
  return pop @_inputs;
}


# 
# 
# 
# Kim Brugger (23 Apr 2010)
sub shift_input {
  return pop @_inputs;
}


# 
# 
# 
# Kim Brugger (23 Apr 2010)
sub unshift_input {
  my ($input) = @_;
  unshift @_inputs, $input;
}


# 
# 
# 
# Kim Brugger (23 Apr 2010)
sub reset_inputs {
  @prev_inputs = @_inputs;
  @_inputs = ();
}

# 
# 
# 
# Kim Brugger (23 Apr 2010)
sub fetch_inputs {
  return @_inputs;
}

# 
# 
# 
# Kim Brugger (23 Apr 2010)
sub fetch_n_reset_inputs {
  my @local_inputs = @_inputs;
  reset_inputs();
  return @local_inputs;
}



# 
# 
# 
# Kim Brugger (23 Apr 2010)
sub dry_run {
  my ( $start_logic_name ) = @_;

  $dry_run = 1;
  run_flow( $start_logic_name );
  $dry_run = 0;
}


# 
# 
# 
# Kim Brugger (23 Apr 2010)
sub run_flow {
  my ( $start_logic_name ) = @_;
  no strict;

  die "EASIH::JMS::run_flow not called with a logic_name\n" if (! $start_logic_name);

  $current_logic_name ||= $start_logic_name;
  my $next_logic_name   = $main::flow{ $current_logic_name};
  while (1) {

    print "Running : $current_logic_name\n";
    if ( ! $main::analysis{$current_logic_name} ) {
      print "ERROR :::: No infomation on on $current_logic_name in main::analysis\n";
    }
    else {
      my ($module, $function) = function_module($main::analysis{$current_logic_name}{ function });

      $function = $module."::".$function;
      $main::analysis{$current_logic_name}{state} = "running";
      &$function( $main::analysis{$current_logic_name}{ hpc_param }  );
      $main::analysis{$current_logic_name}{state} = "done";
    }

    print "Changing from $current_logic_name --> $next_logic_name\n";
    
    if ( ! $main::flow{ $next_logic_name}) {
      print "End of flow\n";
      last;
    }
    else {
      $current_logic_name = $next_logic_name;
      $next_logic_name    = $main::flow{ $current_logic_name};
    }
  }
  
}


# Nicked the floowing two functions to Module::Loaded as they are not in perl-core for 5.8.X 
sub is_loaded (*) { 
    my $pm      = shift;
    my $file    = __PACKAGE__->_pm_to_file( $pm ) or return;


    return 1 if (exists $INC{$file} || $pm eq 'main');
    
    return 0;
}


sub _pm_to_file {
    my $pkg = shift;
    my $pm  = shift or return;
    
    my $file = join '/', split '::', $pm;
    $file .= '.pm';
    
    return $file;
}    



# 
# 
# 
# Kim Brugger (27 Apr 2010)
sub function_module {
  my ($function) = @_;
  
  my $module = 'main';
    
  ($module, $function) = ($1, $2) if ( $function =~ /(.*)::(\w+)/);
  die   "ERROR :::: $module is not loaded!!!\n" if ( ! is_loaded( $module ));

  print "ERROR :::: $current_logic_name points to $function, but this does not exist!\n" if ( ! $module->can( $function ) );

  return ($module, $function);
}


# 
# 
# 
# Kim Brugger (23 Apr 2010)
sub validate_flow {
  my ( $start_logic_name ) = @_;

  die "EASIH::JMS::test_flow not called with a logic_name\n" if (! $start_logic_name);

  print "Start test flow:\n";
  $current_logic_name ||= $start_logic_name;
  my $next_logic_name   = $main::flow{ $start_logic_name};
  while (1) {
    
    if ( ! $main::analysis{$current_logic_name} ) {
      print "ERROR :::: No infomation on on $current_logic_name in main::analysis\n";
    }
    else {
      
      my ($module, $function) = function_module($main::analysis{$current_logic_name}{ function });
    }

   
    if ( ! $main::flow{ $next_logic_name}) {
      print "No more steps in this flow...\n";
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



# 
# 
# 
# Kim Brugger (26 Apr 2010)
sub store_state {
  my ($filename ) = @_;

  if ( ! $filename ) {
    $0 =~ s/.*\///;
    $filename = "$0.freeze";
  }
  
  print "JMS :: Storing state in: '$filename'\n";

  my $blob = {delete_files       => \@delete_files,
	      inputs             => \@prev_inputs,
	      jobs               => \@jobs,
	      save_interval      => $save_interval,
	      verbose            => $verbose,
	      current_logic_name => $current_logic_name,

	      #main file variables.
	      argv               => \@main::ARGV,
	      flow               => \%main::flow,
	      analysis           => \%main::analysis};

  return Storable::store($blob, $filename);
}



# 
# 
# 
# Kim Brugger (26 Apr 2010)
sub restore_state {
  my ( $filename ) = @_;


  if ( ! $filename ) {
    $0 =~ s/.*\///;
    $filename = "$0.freeze";
  }
  
  print "JMS :: Re-storing state from: '$filename'\n";

  my $blob = Storable::retrieve( $filename);

  @delete_files       = @{$$blob{delete_files}};
  @_inputs            = @{$$blob{inputs}};
  @jobs               = @{$$blob{jobs}};

  $save_interval      = $$blob{save_interval};
  $verbose            = $$blob{verbose};
  $current_logic_name = $$blob{current_logic_name};

  @main::ARGV         = @{$$blob{argv}};
  %main::flow         = %{$$blob{flow}};
  %main::analysis     = %{$$blob{analysis}};
}


sub catch_ctrl_c {
    $main::SIG{INT} = \&catch_ctrl_c;           # See ``Writing A Signal Handler''
    fail("Caught a ctrl-c\n");
}


BEGIN {
  $SIG{INT} = \&catch_ctrl_c;
}


1;








