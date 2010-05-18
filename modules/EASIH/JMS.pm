package EASIH::JMS;
# 
# JobManagementSystem frame for running pipelines everywhere!
# 
# 
# Kim Brugger (23 Apr 2010), contact: kim.brugger@easih.ac.uk

use strict;
use warnings;
use Data::Dumper;
use Storable;
use File::Temp;
use Time::HiRes;

use EASIH::JMS::Hive;

my $last_save      = 300;
my $save_interval  =   0;
my $verbose        =   0;
my $max_retry      =   3;
my $sleep_time     =  10;
my $current_logic_name;
my $use_storing    =  0; # debugging purposes


my $hive           = "DetachedDummy";

my @delete_files;
my %jms_hash;
my @jms_ids;
my $cwd      = `pwd`;
chomp($cwd);
my $dry_run  = 0;

my $job_counter = 1;

our $FINISHED    =    1;
our $FAILED      =    2;
our $RUNNING     =    3;
our $QUEUEING    =    4;
our $RESUBMITTED =    5;
our $SUBMITTED   =    6;
our $UNKNOWN     =  100;

my %s2status = ( 1   =>  "Finished",
		 2   =>  "Failed",
		 3   =>  "Running",
		 4   =>  "Queueing",
		 5   =>  "Resubmitted",
		 6   =>  "Submitted",
		 100 =>  "Unknown");


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
sub hive {
  $hive = shift || $hive;
  return $hive;
}


# 
# 
# 
# Kim Brugger (23 Apr 2010)
sub sleep_time {
  $sleep_time = shift || 60;
}


# 
# -1 is never or only on crashes
# 
# Kim Brugger (23 Apr 2010)
sub save_interval {
  $save_interval = shift || -1;
}



# 
# Checks and see if the current state of the run should be stored
# The inverval of this happening is set with save_interval
#
# Kim Brugger (04 May 2010)
sub check_n_store_state {
  
  my $now = Time::HiRes::gettimeofday();
  store_state() if ( $now - $last_save > $save_interval );
  
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
# submit a single job to the HPC
# 
# Kim Brugger (22 Apr 2010)
sub submit_job {
  my ($cmd, $output) = @_;
  my ($tmp_fh, $tmp_file) = File::Temp::tempfile(DIR => "./tmp" );

  if ( $dry_run ) {
    print "$cmd using $hive\n";
    return;
  }


  my $jms_id = $job_counter++;
  my $instance = { status      => $SUBMITTED,
		   tracking    => 1,
		   command     => $cmd,
		   output      => $output,
		   logic_name  => $current_logic_name};

  # dummy jobs insert output that will be picked up by the next step of the process,
  # so not all jobs execute a command.
  if ( $cmd ) {

    my $submit_job = "EASIH::JMS::Hive::".$hive."::submit_job";

    no strict 'refs';
    my $job_id = &$submit_job( $cmd, $main::analysis{$current_logic_name}{ hpc_param });

    $$instance{ job_id } = $job_id;
  }    

  $jms_hash{ $jms_id }  = $instance;

  push @jms_ids, $jms_id;
}



# 
# 
# 
# Kim Brugger (26 Apr 2010)
sub resubmit_job {
  my ( $jms_id ) = @_;

  my $instance   = $jms_hash{ $jms_id };
  my $logic_name = $$instance{logic_name};

  if ( $dry_run ) {
    print "echo 'cd $cwd; $$instance{cmd}' |qsub $main::analysis{$logic_name}{ hpc_param } \n";
    return;
  }


  my $submit_job = "EASIH::JMS::Hive::".$hive."::submit_job";
  no strict 'refs';
  my $job_id = &$submit_job( $$instance{ cmd }, $main::analysis{$logic_name}{ hpc_param });
  
  $$instance{ job_id }   = $job_id;
  $$instance{ status }   = $RESUBMITTED;
  $$instance{ tracking } = 1;

}



# 
# Wait for the jobs to terminate
# 
# Kim Brugger (22 Apr 2010)
sub check_jobs {

  return if ( $dry_run );

  my ( $done, $running, $waiting, $queued, $failed, $other, ) = (0,0,0,0,0,0);
  foreach my $jms_id ( @jms_ids ) {
      
    # Only look at the jobs we are currently tracking
    next if ( ! $jms_hash{ $jms_id }{ tracking } );

    my $job_status = "EASIH::JMS::Hive::".$hive."::job_status";
    no strict 'refs';
    my $status = &$job_status( $jms_hash{ $jms_id}{ job_id } );

    $jms_hash{ $jms_id }{ status } = $status;

    # this should be done with switch, but as we are not on perl 5.10+ this is how it is done...
    if ($status  ==  $FINISHED  ) {
      $done++;
    }
    elsif ($status == $FAILED   ) {
      $failed++;
      $jms_hash{ $jms_id }{ failed }++;
      if ( $jms_hash{ $jms_id }{ failed } < $max_retry ) {
	print "resubmitting job\n";
	resubmit_job( $jms_id );
      }
      else { 
	print "Cannot resubmit job ($jms_hash{ $jms_id }{ failed } < $max_retry)\n";
      }
    }
    elsif ($status == $RUNNING  ) {
      $running++;
    }
    elsif ($status == $QUEUEING  ) {
      $queued++; 
    }
    else {
      $other++;
    }
    
  }

  use POSIX 'strftime';
  my $time = strftime('Today is %m/%d/%y: %H.%M:', localtime);
  
  print "[$time]: D: $done, R: $running, Q: $queued, W: $waiting, F: $failed, O: $other\n";

  return;
}





# 
# reset the failed states, so the pipeline can run again
# 
# Kim Brugger (26 Apr 2010)
sub reset {
  my ($reset_logic_name) = @_;


  # Only look at the jobs we are currently tracking
  foreach my $jms_id ( @jms_ids ) {
    delete $jms_hash{ $jms_id } if ($jms_hash{ $jms_id }{ logic_name } eq $reset_logic_name );
  }
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
  my ($message ) = @_;

  print STDERR "ERROR:: $message\n";
  store_state();
  exit;
  
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
# Kim Brugger (17 May 2010)
sub next_analysis {
  my ( $logic_name ) = @_;

  return $main::flow{ $logic_name} || undef;
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
  foreach my $jms_id ( @jms_ids ) {
    
    my ($host, $path) = split(":", $jms_hash{$jms_id}{ hpc_stats }{Error_Path});
    push @files, $path if ( -f $path);
    ($host, $path) = split(":", $jms_hash{$jms_id}{ hpc_stats }{Output_Path});
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
sub dry_run {
  my ( $start_logic_name ) = @_;

  $dry_run = 1;
  run( $start_logic_name );
  $dry_run = 0;
}




# 
# 
# 
# Kim Brugger (18 May 2010)
sub fetch_active_jobs {

  my @active_jobs;
  foreach my $jms_id ( @jms_ids ) {
    push @active_jobs, $jms_id if ( $jms_hash{ $jms_id }{ tracking });
  }

  return @active_jobs;
}




# 
# 
# 
# Kim Brugger (18 May 2010)
sub run {
  my (@start_logic_names) = @_;

  my ( $done, $running, $started, $no_restart) = (0,0,0, 0);
  while (1) {

    check_n_store_state();

    my @active_jobs = fetch_active_jobs();
    
    # nothing running, start from the start_logic_names
    if ( ! @active_jobs ) {
      foreach my $start_logic_name ( @start_logic_names ) {
        run_analysis( $start_logic_name );
	$running++;
      }
      @start_logic_names = ();
    }
    else {
      
      foreach my $jms_id ( @active_jobs ) {

        my $logic_name = $jms_hash{ $jms_id }{ logic_name };

        if ( $jms_hash{ $jms_id }{ status } == $FINISHED ) {
	  
	  $jms_hash{ $jms_id }{ tracking } = 0;	  
          my $next_logic_name = next_analysis( $jms_hash{ $jms_id }{ logic_name } );
          # no more steps we can take, jump the the next job;
          if ( ! $next_logic_name ) {
            $done++;
            next;
          }

          # all threads for this run has to finish before we can 
          # proceed.
          if ( $main::analysis{ $next_logic_name }{ sync } ) {
            
            my @lactive = fetch_active_jobs( $logic_name );
            my @inputs;
	    my $all_threads_done = 1;
            foreach my $ljms_id ( @lactive ) {
              if ( ! $jms_hash{ $ljms_id }{ done } ) {
		$all_threads_done = 0;
		last;
	      }
              push @inputs, $main::analysis{ $logic_name }{ output };
            }
	    
	    if ( $all_threads_done ) {
	      print " $jms_id :: $jms_hash{ $jms_id }{ logic_name }  --> $next_logic_name (synced !!!) \n";
	      run_analysis( $next_logic_name, @inputs);
	      $started++;
	    }
            
          }
          else {
	    print " $jms_id :: $jms_hash{ $jms_id }{ logic_name }  --> $next_logic_name  \n";
            run_analysis( $next_logic_name, $jms_hash{ $jms_id }{ output });
            $started++;
          }
        }
	elsif ( $jms_hash{ $jms_id }{ status } == $FAILED ) {
	  $jms_hash{ $jms_id }{ tracking } = 0;
	  $no_restart++;
	}
        else {
          $running++;
        }
      }
    }

    print "Done: $done, Running: $running, Started: $started, No-restart: $no_restart \n";
    last if ( ! $running && ! $started);

    check_n_store_state();
    sleep ( $sleep_time );
    check_jobs();
    ( $done, $running, $started) = (0,0,0);
    
  }
  

}



# 
# 
# 
# Kim Brugger (18 May 2010)
sub run_analysis {
  my ( $logic_name, @inputs) = @_;

  my $function = function_module($main::analysis{ $logic_name }{ function }, $logic_name);
	
  $current_logic_name = $logic_name;

  {
    no strict 'refs';
    &$function(@inputs);
  }
}


# Nicked the following two functions from Module::Loaded as they are
# not in perl-core for 5.8.X and I needed a special version of them
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
  my ($function, $logic_name) = @_;
  
  my $module = 'main';
    
  ($module, $function) = ($1, $2) if ( $function =~ /(.*)::(\w+)/);
  die "ERROR :::: $module is not loaded!!!\n" if ( ! is_loaded( $module ));
  die "ERROR :::: $logic_name points to $function, but this does not exist!\n" if ( ! $module->can( $function ) );

  return $module . "::" . $function;
}


# 
# 
# 
# Kim Brugger (23 Apr 2010)
sub validate_flow {
  my (@start_logic_names) = @_;

  die "EASIH::JMS::validate_flow not called with a logic_name\n" if (! @start_logic_names);

  foreach my $start_logic_name ( @start_logic_names ) {

    print "Start test flow for $start_logic_name:\n";
    $current_logic_name ||= $start_logic_name;
    my $next_logic_name   = $main::flow{ $start_logic_name};
    while (1) {
      
      if ( ! $main::analysis{$current_logic_name} ) {
	print "ERROR :::: No infomation on on $current_logic_name in main::analysis\n";
      }
      else {
	
	my $function = function_module($main::analysis{$current_logic_name}{ function });
	print "Will be running $function\n";
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

  print "End of validate_run\n";
  
}



# 
# 
# 
# Kim Brugger (26 Apr 2010)
sub store_state {
  my ($filename ) = @_;

  return if ( $dry_run);
  return if ( ! $use_storing );

  if ( ! $filename ) {
    $0 =~ s/.*\///;
    $filename = "$0.$$";
  }
  
  print "JMS :: Storing state in: '$filename'\n";

  my $blob = {delete_files       => \@delete_files,
	      jms_ids            => \@jms_ids,
	      jms_hash           => \%jms_hash,
	      save_interval      => $save_interval,
	      verbose            => $verbose,

	      #main file variables.
	      argv               => \@main::ARGV,
	      flow               => \%main::flow,
	      analysis           => \%main::analysis};

  $last_save = Time::HiRes::gettimeofday();

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
  @jms_ids            = @{$$blob{jms_ids}};
  %jms_hash           = %{$$blob{jms_hash}};

  $save_interval      = $$blob{save_interval};
  $verbose            = $$blob{verbose};


  @main::ARGV         = @{$$blob{argv}};
  %main::flow         = %{$$blob{flow}};
  %main::analysis     = %{$$blob{analysis}};
}


sub catch_ctrl_c {
    $main::SIG{INT} = \&catch_ctrl_c;
    fail("Caught a ctrl-c\n");
}


BEGIN {
  $SIG{INT} = \&catch_ctrl_c;
}


1;








