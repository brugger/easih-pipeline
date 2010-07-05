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

my $last_save      =   0;
my $save_interval  = 300;
my $verbose        =   0;
my $max_retry      =   3;
my $jobs_submitted =   0;
my $sleep_time     =   5;
my $current_logic_name;
my $use_storing    =   1; # debugging purposes
my $max_jobs       =  -1; # to control that we do not flood Darwin, or if local, block the machine. -1 is no limit
my @argv; # the argv from main is fetched at load time, and a copy kept here so we can store it later

my $no_restart     =   0; # failed jobs that cannot be restarted. 

# default dummy hive that will fail gracefully, and the class that every other hive
# should inherit from.
my $hive           = "EASIH::JMS::Hive";

my @delete_files;
my %jms_hash;
my @jms_ids;

my @retained_jobs;
my %analysis_order;

my $job_counter = 1; # This is for generating internal jms_id (JobManamentSystem_Id)

our $cwd      = `pwd`;
chomp($cwd);

my $dry_run  = 0;
my %dependencies;


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
# Kim Brugger (24 Jun 2010)
sub hive {
  $hive = shift;
  
  if ( $hive ) {
    # strip away the the expected class
    $hive =~ s/EASIH::JMS::Hive:://;
    # and append it (again);
    $hive = "EASIH::JMS::Hive::".$hive;
  }

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
# Setting the working directory, if different than the cwd
# 
# Kim Brugger (23 Apr 2010)
sub cwd {
  my ($new_cwd) = @_;
  $cwd = $new_cwd;
}



# 
# 
# 
# Kim Brugger (05 Jul 2010)
sub submit_system_job {
  my ($cmd, $output, $system) = @_;
  submit_job($cmd, $output, 1);
}


# 
# submit a single job if $system is then a single system call is doing the work!
# 
# Kim Brugger (22 Apr 2010)
sub submit_job {
  my ($cmd, $output, $system) = @_;

  my $tmp_file = EASIH::JMS::tmp_file();

  if ( $dry_run ) {
    print "$cmd using $hive\n";
    return;
  }

  if ( ! $cmd ) {
     use Carp;
     Carp::confess(" no cmd given\n");
  }

  if (@retained_jobs && $max_jobs > 0 && $max_jobs > $jobs_submitted) {
    push @retained_jobs, [ $cmd, $output, $current_logic_name];
    my $params = shift @retained_jobs;
#    print "Queued/unqueued a job ( ". @retained_jobs . " jobs retained)\n";
    ($cmd, $output, $current_logic_name)= (@$params);
#    print " PARAMS :::     ($cmd, $output, $current_logic_name) \n";
  }
  elsif ($max_jobs > 0 && $max_jobs <= $jobs_submitted ) {
    push @retained_jobs, [ $cmd, $output, $current_logic_name];
#    print "Retained a job ( ". @retained_jobs . " jobs retained)\n";
    return;
  };

  my $jms_id = $job_counter++;
  my $instance = { status      => $SUBMITTED,
		   tracking    => 1,
		   command     => $cmd,
		   output      => $output,
		   logic_name  => $current_logic_name};

#  print "$jms_id ::: " . Dumper( $instance );

  if ( $system ) {
    eval { system "$cmd" };
    $$instance{ job_id } = -1;
    if ( ! $@ ) {
      $$instance{ status   } = $FINISHED;
    }
    else {
      print "$@\n";
      $$instance{ status   } = $FAILED;
    }
  }
  else {

    my $job_id = $hive->submit_job( "cd $cwd;$cmd", $main::analysis{$current_logic_name}{ hpc_param });
    
    $$instance{ job_id } = $job_id;
  }    

  $jms_hash{ $jms_id }  = $instance;

  $jobs_submitted++;      

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
    print "echo 'cd $cwd; $$instance{command}' | qsub $main::analysis{$logic_name}{ hpc_param } \n";
    return;
  }

  my $job_id = $hive->submit_job( $$instance{ command }, $main::analysis{$logic_name}{ hpc_param });
  
  $$instance{ job_id }   = $job_id;
  $$instance{ status }   = $RESUBMITTED;
  $$instance{ tracking } = 1;
  $jobs_submitted++;      

}

# 
# 
# 
# Kim Brugger (24 Jun 2010)
sub report {

  my %res = ();

  foreach my $jms_id ( @jms_ids ) {
    my $logic_name = $jms_hash{ $jms_id }{ logic_name};
    my $status     = $jms_hash{ $jms_id }{ status }; 
    $res{ $logic_name }{ $status }++;
    $res{ $logic_name }{ failed } += ($jms_hash{ $jms_id }{ failed } || 0);


    my $job_id     = $jms_hash{ $jms_id }{ job_id }; 
   
    if ( $job_id != -1 ) {
      my $memory = int($hive->job_memory( $job_id )) || 0;
      $res{ $logic_name }{ memory } = $memory if ( !$res{ $logic_name }{ memory } || $res{ $logic_name }{ memory } < $memory);
      $res{ $logic_name }{ runtime } += int($hive->job_runtime( $job_id ));
    }


  }

  return if ( keys %res == 0);

  my $report = "Run usage statistics:\n";
  foreach my $logic_name ( sort {$analysis_order{ $a } <=> $analysis_order{ $b } } keys %res ) {
    if ( ! defined $res{ $logic_name }{ memory } ) {
      $res{ $logic_name }{ memory } = "N/A";
    }
    else {
      if ($res{ $logic_name }{ memory } > 1000000000 ) {
	$res{ $logic_name }{ memory } = sprintf("%.2fGB",$res{ $logic_name }{ memory }/1000000000);
      }
      elsif ($res{ $logic_name }{ memory } > 1000000 ) {
	$res{ $logic_name }{ memory } = sprintf("%.2fMB",$res{ $logic_name }{ memory }/1000000);
      }
      elsif ($res{ $logic_name }{ memory } > 1000 ) {
	$res{ $logic_name }{ memory } = sprintf("%.2fKB",$res{ $logic_name }{ memory }/1000);
      }
    }

    $res{ $logic_name }{ runtime } ||= 0;

    my ($hour, $min, $sec) = (0,0,0);
    $hour = int($res{ $logic_name }{ runtime }/3600);
    $res{ $logic_name }{ runtime } -= 3600*$hour; 
    $min = int($res{ $logic_name }{ runtime }/60);
    $res{ $logic_name }{ runtime } -= 60*$min;
    $sec = int($res{ $logic_name }{ runtime });
    $res{ $logic_name }{ runtime } = sprintf("%02d:%02d:%02d", $hour, $min, $sec);

    my $queue_stats;

    $queue_stats .= sprintf("%02d/%02d/",($res{ $logic_name }{ $FINISHED } || 0),($res{ $logic_name }{ $RUNNING  } || 0));
    my $sub_other = ($res{ $logic_name }{ $QUEUEING  } || 0);
    $sub_other += ($res{ $logic_name }{ $RESUBMITTED  } || 0);
    $sub_other += ($res{ $logic_name }{ $SUBMITTED  } || 0);
    $queue_stats .= sprintf("%02d/%02d",$sub_other, ($res{ $logic_name }{ failed  } || 0));


    $report .= sprintf("%-15s ||  %8s  || %10s || $queue_stats\n",$logic_name,$res{ $logic_name }{ runtime },  $res{ $logic_name }{ memory });
  }

  use POSIX 'strftime';
  my $time = strftime('%m/%d/%y %H.%M', localtime);
  print "[$time]\n";
  print "-"x30 . "\n";
  print $report;
}



# 
# Wait for the jobs to terminate
# 
# Kim Brugger (22 Apr 2010)
sub check_jobs {

  return if ( $dry_run );

  foreach my $jms_id ( @jms_ids ) {
      
    # Only look at the jobs we are currently tracking
    next if ( ! $jms_hash{ $jms_id }{ tracking } );

    if ( ! $jms_hash{ $jms_id }{ job_id } ) {
      print "'$jms_id' ==> " . Dumper( $jms_hash{ $jms_id }) . "\n";
      die;
    }
 
    my $status;
    if ( $jms_hash{ $jms_id }{ job_id } == -1 ) {
      $status = $jms_hash{ $jms_id }{ status };
    }
    else {	
      $status = $hive->job_status( $jms_hash{ $jms_id}{ job_id } );
      $jms_hash{ $jms_id }{ status } = $status;
    }

    # this should be done with switch, but as we are not on perl 5.10+ this is how it is done...
    if ($status ==  $FINISHED  ) {
      $jobs_submitted--;
    }
    elsif ($status == $FAILED ) {
      $jobs_submitted--;
      $jms_hash{ $jms_id }{ failed }++;
      if ( $jms_hash{ $jms_id }{ failed } < $max_retry ) {
	print "Failed, resubmitting job\n";
	resubmit_job( $jms_id );
      }
      else { 
	print "Cannot resubmit job ($jms_hash{ $jms_id }{ failed } < $max_retry)\n";
	$no_restart++;
      }
    }
    
  }

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
# 
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
  
  print  "mkdir tmp" if ( ! -d './tmp');
  system "mkdir tmp" if ( ! -d './tmp');

  my ($tmp_fh, $tmp_file) = File::Temp::tempfile(DIR => "./tmp" );
  close ($tmp_fh);
  system "rm $tmp_file";

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
    push @files, $path if ( $path && -f $path);
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

#  @active_jobs = sort { $a <=> $b } @active_jobs;
#  @active_jobs = sort { $analysis_order{ $jms_hash{ $a }{logic_name}} <=> $analysis_order{ $jms_hash{ $b }{logic_name}} } @active_jobs;

#  print "@active_jobs\n";

  return @active_jobs;
}



# 
# 
# 
# Kim Brugger (18 May 2010)
sub fetch_jobs {
  my ( $logic_name ) = @_;

  my @jobs;
  foreach my $jms_id ( @jms_ids ) {    
    push @jobs, $jms_id if ( $jms_hash{ $jms_id }{ logic_name } eq $logic_name );
  }

  return @jobs;
}


# 
# 
# 
# Kim Brugger (05 Jul 2010)
sub analysis_dependencies {
  my ( $logic_name ) = @_;

  while ( my $next_logic_name = next_analysis( $logic_name ) ) {
    push @{$dependencies{ $next_logic_name }}, $logic_name;
    push @{$dependencies{ $next_logic_name }}, @{$dependencies{ $logic_name }} if ($dependencies{ $logic_name });
    
    # make sure a logic_name only occurs once.
    my %saw;
    @{$dependencies{ $next_logic_name }} = grep(!$saw{$_}++, @{$dependencies{ $next_logic_name }});
    $logic_name = $next_logic_name;
  }
}



# 
# 
# 
# Kim Brugger (05 Jul 2010)
sub depends_on_active_jobs {
  my ($logic_name) = @_;

  my %dependency;
  map { $dependency{ $_ }++ } @{$dependencies{ $logic_name }};
  
  foreach my $jms_id ( @jms_ids ) {
    next if (! $jms_hash{ $jms_id }{ tracking });
    
    if ( $dependency{ $jms_hash{ $jms_id }{ logic_name }}) {
      return 1;
    }
  }
  

  return 0;
}





# 
# Main loop that does all the work.
# 
# Kim Brugger (18 May 2010)
sub run {
  my (@start_logic_names) = @_;

  while (1) {

    my ($started, $running ) = (0,0);

    my @active_jobs = fetch_active_jobs();
    
    # nothing running, start from the start_logic_names
    if ( ! @active_jobs ) {
      foreach my $start_logic_name ( @start_logic_names ) {
	analysis_dependencies( $start_logic_name );
	$analysis_order{ $start_logic_name } = 1;
        run_analysis( $start_logic_name );
	$running++;
      }
      # set this variable to null so we dont end here again. 
      # This could also be done with a flag, but for now here we are.
      @start_logic_names = ();
    }
    else {

      foreach my $jms_id ( @active_jobs ) {

	next if ( ! $jms_hash{ $jms_id }{ tracking });
        my $logic_name = $jms_hash{ $jms_id }{ logic_name };

        if ( $jms_hash{ $jms_id }{ status } == $FINISHED ) {
	  
	  $jms_hash{ $jms_id }{ tracking } = 0;	  
          my $next_logic_name = next_analysis( $logic_name );

          # no more steps we can take, jump the the next job;
          if ( ! $next_logic_name ) {
            next;
          }

	  $analysis_order{ $next_logic_name } = $analysis_order{ $logic_name } + 1 
	      if (! $analysis_order{ $next_logic_name } || 
		  $analysis_order{ $next_logic_name } <= $analysis_order{ $logic_name } + 1);



          # all threads for this run has to finish before we can 
          # proceed to the next one. If a failed job exists this will never be 
	  # possible
          if ( $main::analysis{ $next_logic_name }{ sync } ) { 

	    next if ( $no_restart );
	    # we do not go further if new jobs has been started or is running.
	    next if ( @retained_jobs > 0 );
	    
	    next if (depends_on_active_jobs( $next_logic_name));

	    my $all_threads_done = 1;
            foreach my $ljms_id ( fetch_jobs( $logic_name ) ) {
              if ( $jms_hash{ $ljms_id }{ status } != $FINISHED ) {
		$all_threads_done = 0;
		last;
	      }
	    }
	    
	    if ( $all_threads_done ) {
	      # collect inputs, and set tracking to 0
	      my @inputs;
	      foreach my $ljms_id ( fetch_jobs( $logic_name ) ) {
		$jms_hash{ $ljms_id }{ tracking } = 0;
		push @inputs, $jms_hash{ $ljms_id }{ output };
	      }

	      print " $jms_id :: $jms_hash{ $jms_id }{ logic_name }  --> $next_logic_name (synced !!!) $no_restart\n";
	      run_analysis( $next_logic_name, @inputs);
	      $started++;
	    }
            
	  }
	  # unsynced part of the pipeline, run the next job.
          else {
	    print " $jms_id :: $jms_hash{ $jms_id }{ logic_name }  --> $next_logic_name  \n";
            run_analysis( $next_logic_name, $jms_hash{ $jms_id }{ output });
	    $started++;
          }
        }
	elsif ( $jms_hash{ $jms_id }{ status } == $FAILED ) {
	  $jms_hash{ $jms_id }{ tracking } = 0;
#	  $no_restart++;
	}
        else {
          $running++;
        }
      }
    }


    while ( $max_jobs > 0 && $jobs_submitted < $max_jobs && @retained_jobs ) {
      my $params = shift @retained_jobs;
      submit_job(@$params);
      $started++;
    }


    check_n_store_state();
    report();    
    last if ( ! $running && ! $started && !@retained_jobs);

    sleep ( $sleep_time );
    check_jobs();
  }
  

#  print "Retaineded jobs: ". @retained_jobs . " (should be 0)\n";

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

  die "$logic_name does not point to a function\n" if ( ! $function );
  
  
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
	      last_save          => $last_save,
	      save_interval      => $save_interval,
	      max_retry          => $max_retry,
	      sleep_time         => $sleep_time,
	      max_jobs           => $max_jobs,
	      hive               => $hive,
	      job_counter        => $job_counter,
	      
	      stats              => $hive->stats,
	      
	      retained_jobs      => \@retained_jobs,
	      current_logic_name => $current_logic_name,
	      #main file variables.
	      argv               => \@argv,
	      flow               => \%main::flow,
	      analysis           => \%main::analysis,
	      analysis_order     => \%analysis_order,
	      dependencies       => \%dependencies};

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

  $last_save          = $$blob{last_save};
  $save_interval      = $$blob{save_interval};
  $max_retry          = $$blob{max_retry};
  $sleep_time         = $$blob{sleep_time};
  $max_jobs           = $$blob{max_jobs};
  $job_counter        = $$blob{job_counter};
	      
  @retained_jobs      = $$blob{retained_jobs};
  $current_logic_name = $$blob{current_logic_name};

  @main::ARGV         = @{$$blob{argv}};
  %main::flow         = %{$$blob{flow}};
  %main::analysis     = %{$$blob{analysis}};
  %analysis_order     = %{$$blob{analysis_order}};
  %dependencies       = %{$$blob{dependencies}};

  hive($$blob{hive});
  $hive->stats($$blob{stats});
}


sub catch_ctrl_c {
    $main::SIG{INT} = \&catch_ctrl_c;
    fail("Caught a ctrl-c\n");
}


BEGIN {
  $SIG{INT} = \&catch_ctrl_c;
  @argv = @main::ARGV;

}


1;








