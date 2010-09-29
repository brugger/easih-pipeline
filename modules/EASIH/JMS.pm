package EASIH::JMS;
# 
# JobManagementSystem framework for running pipelines everywhere!
# 
# 
# Kim Brugger (23 Apr 2010), contact: kim.brugger@easih.ac.uk

use strict;
use warnings;
use Data::Dumper;
use Storable;
use File::Temp;
use Time::HiRes;
use Carp;

use EASIH::JMS::Backend;

my $last_save      =   0;
my $save_interval  = 300;
my $verbose_level  =   0;
my $max_retry      =   3;
my $jobs_submitted =   0;
my $sleep_time     =   5;
my $current_logic_name;
my $pre_jms_id     = undef;
my $use_storing    =   1; # debugging purposes
my $max_jobs       =  -1; # to control that we do not flood Darwin, or if local, block the machine. -1 is no limit
my @argv; # the argv from main is fetched at load time, and a copy kept here so we can store it later
my $freeze_file;

my $no_restart     =   0; # failed jobs that cannot be restarted. 

# default dummy hive that will fail gracefully, and the class that every other hive
# should inherit from.
my $backend           = "EASIH::JMS::Backend";

my ($start_time, $end_time);
my @delete_files;
my %jms_hash;

my @retained_jobs;
my %analysis_order;

my $job_counter = 1; # This is for generating internal jms_id (JobManamentSystem_Id)

our $cwd      = `pwd`;
chomp($cwd);

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
  $verbose_level = shift || 0;
}



# 
# 
# 
# Kim Brugger (05 Jul 2010)
sub verbose {
  my ( $message, $level) = @_;
  $level ||= 0;
  
  return if ( $level > $verbose_level);
  $message =~ s/\Z//;
  print " ::::" . " $message\n";

}



# 
# disable the store function
# 
# Kim Brugger (27 Jul 2010)
sub no_store {
  $use_storing = 0;
}



# 
# 
# 
# Kim Brugger (23 Apr 2010)
sub fail {
  my ($message ) = @_;

  print STDERR "ERROR :: $message\n";
  store_state();
  exit;
  
}



# 
# 
# 
# Kim Brugger (24 Jun 2010)
sub backend {
  $backend = shift;
  
  if ( $backend ) {
    # strip away the the expected class
    $backend =~ s/EASIH::JMS::Backend:://;
    # and (re)append it (again);
    $backend = "EASIH::JMS::Backend::".$backend;
  }

  return $backend;
}


# 
# 
# 
# Kim Brugger (24 Jun 2010)
sub hive {
  $backend = shift;

  print "\n :::: Hive has been replaced with Backend, please change your script ::::";
  print "\n :::: This function will be removed soon.                            ::::\n\n";
  
  if ( $backend ) {
    # strip away the the expected class
    $backend =~ s/EASIH::JMS::Backend:://;
    $backend =~ s/EASIH::JMS::Hive:://;
    # a renaming of one of the backend modules...
    $backend = "Local" if ( $backend eq "Kluster");
    # and (re)append it (again);
    $backend = "EASIH::JMS::Backend::".$backend;
  }

  return $backend;
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
# 
# 
# Kim Brugger (20 Sep 2010)
sub version {


  my $libdir = $INC{ 'EASIH/JMS.pm'};

  my $VERSION   = "unknown";

  if ($libdir && $libdir =~ /.*\//) {
    $libdir =~ s/(.*\/).*/$1/;
    $VERSION = `cd $libdir; git describe 2> /dev/null`;
  }
  else {
    $VERSION = `git describe`;
  }
  $VERSION ||= "unknown";

  chomp( $VERSION );

  return $VERSION;
}



# 
# Checks and see if the current state of the run should be stored
# The inverval of this happening is set with save_interval
#
# Kim Brugger (04 May 2010)
sub check_n_store_state {

  return if ( $save_interval == -1 );
  
  my $now = Time::HiRes::gettimeofday();
  store_state() if ($now - $last_save > $save_interval );
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
  my ($cmd, $output) = @_;
  submit_job($cmd, $output, 1);
}


# 
# submit a single job if $system is then a single system call is doing the work!
# 
# Kim Brugger (22 Apr 2010)
sub submit_job {
  my ($cmd, $output, $system) = @_;

  if ( ! $cmd ) {
     Carp::confess(" no cmd given\n");
  }

  if (@retained_jobs && $max_jobs > 0 && $max_jobs > $jobs_submitted) {
    push @retained_jobs, [ $cmd, $output, $current_logic_name];
    my $params = shift @retained_jobs;
    verbose("Queued/unqueued a job ( ". @retained_jobs . " jobs retained)\n", 20);
    ($cmd, $output, $current_logic_name)= (@$params);
    verbose(" PARAMS :::     ($cmd, $output, $current_logic_name) \n", 20);
  }
  elsif ($max_jobs > 0 && $max_jobs <= $jobs_submitted ) {
    push @retained_jobs, [ $cmd, $output, $current_logic_name];
    verbose("Retained a job ( ". @retained_jobs . " jobs retained)\n", 20);
    return;
  };

  my $jms_id = $job_counter++;
  my $instance = { status      => $SUBMITTED,
		   tracking    => 1,
		   command     => $cmd,
		   output      => $output,
		   logic_name  => $current_logic_name,
		   pre_jms_id  => $pre_jms_id};


  if ( $system ) {
    eval { system "$cmd" };
    $$instance{ job_id } = -1;
    if ( ! $@ ) {
      $$instance{ status   } = $FINISHED;
    }
    else {
      verbose("$@\n", 1);
      $$instance{ status   } = $FAILED;
    }
  }
  else {

    my $job_id = $backend->submit_job( "cd $cwd;$cmd", $main::analysis{$current_logic_name}{ hpc_param });
    
    $$instance{ job_id } = $job_id;
  }    

  $jms_hash{ $jms_id }  = $instance;

  $jobs_submitted++;      

  push @{$jms_hash{ $pre_jms_id }{ post_jms_id }}, $jms_id if ( $pre_jms_id );
}



# 
# 
# 
# Kim Brugger (26 Apr 2010)
sub resubmit_job {
  my ( $jms_id ) = @_;


  my $instance   = $jms_hash{ $jms_id };
  my $logic_name = $$instance{logic_name};


  my $job_id = $backend->submit_job( $$instance{ command }, $main::analysis{$logic_name}{ hpc_param });
  
  $$instance{ job_id }   = $job_id;
  $$instance{ status }   = $RESUBMITTED;
  $$instance{ tracking } = 1;
  $jobs_submitted++;      

}


# 
# 
# 
# Kim Brugger (05 Jul 2010)
sub format_memory {
  my ( $memory ) = @_;

  if ( ! defined $memory ) {
    return "N/A";
  }
  else {
    if ($memory > 1000000000 ) {
      return sprintf("%.2fGB",$memory / 1000000000);
    }
    elsif ($memory  > 1000000 ) {
      return sprintf("%.2fMB",$memory / 1000000);
    }
    elsif ($memory  > 1000 ) {
      return sprintf("%.2fKB",$memory / 1000);
    }
  }
  
  return "N/A";
}



# 
# 
# 
# Kim Brugger (05 Jul 2010)
sub format_time {
  my ( $runtime ) = @_;
  
  return "N/A" if ( ! defined $runtime);

  my $res;
  my ($hour, $min, $sec) = (0,0,0);
  $hour = int( $runtime / 3600);
  $runtime -= 3600*$hour; 
  $min = int( $runtime / 60);
  $runtime -= 60*$min;
  $sec = int( $runtime );
  return sprintf("%02d:%02d:%02d", $hour, $min, $sec);
}



# 
# 
# 
# Kim Brugger (24 Sep 2010)
sub freeze_file {

  my $filename = $0;
  $filename =~ s/.*\///;
  
  return "$filename.$$";
}


# 
# 
# 
# Kim Brugger (05 Jul 2010)
sub get_timestamp {
  
  use Sys::Hostname;
  my $host = hostname;

  use POSIX 'strftime';
  my $time = strftime('%d/%m/%y %H.%M', localtime);


  return "[$time \@$host ".(freeze_file())."]\n" . "-"x30 . "\n";

}



# 
# 
# 
# Kim Brugger (13 Sep 2010)
sub fetch_jms_ids {
  my $active_only = shift || 0;

  my @jms_ids = sort {$a <=> $b } keys %jms_hash;

  if ( $active_only ) {
    my @active_jobs;
    foreach my $jms_id ( @jms_ids ) {
      push @active_jobs, $jms_id if ( $jms_hash{ $jms_id }{ tracking });
    }
    return @active_jobs;
  }

  return @jms_ids;
}


# 
# 
# 
# Kim Brugger (18 May 2010)
sub fetch_active_jms_ids {
  return fetch_jms_ids(1);
}



# 
# 
# 
# Kim Brugger (24 Jun 2010)
sub report {

  my %res = ();

  foreach my $jms_id ( fetch_jms_ids() ) {
    my $logic_name = $jms_hash{ $jms_id }{ logic_name};
    my $status     = $jms_hash{ $jms_id }{ status }; 
    $res{ $logic_name }{ $status }++;
    $res{ $logic_name }{ failed } += ($jms_hash{ $jms_id }{ failed } || 0);

    my $job_id     = $jms_hash{ $jms_id }{ job_id }; 
   
    if ( $job_id != -1 ) {
      my $memory = $backend->job_memory( $job_id ) || 0;
      $res{ $logic_name }{ memory } = $memory if ( !$res{ $logic_name }{ memory } || $res{ $logic_name }{ memory } < $memory);
      $res{ $logic_name }{ runtime } += $backend->job_runtime( $job_id ) || 0;
    }
  }

  return if ( keys %res == 0);

  my $report = get_timestamp(). "Run statistics:\n";
  foreach my $logic_name ( sort {$analysis_order{ $a } <=> $analysis_order{ $b } } keys %res ) {
    my $queue_stats;

    $queue_stats .= sprintf("%02d/%02d/",($res{ $logic_name }{ $FINISHED } || 0),($res{ $logic_name }{ $RUNNING  } || 0));
    my $sub_other = ($res{ $logic_name }{ $QUEUEING  } || 0);
    $sub_other += ($res{ $logic_name }{ $RESUBMITTED  } || 0);
    $sub_other += ($res{ $logic_name }{ $SUBMITTED  } || 0);
    $queue_stats .= sprintf("%02d/%02d",$sub_other, ($res{ $logic_name }{ failed  } || 0));

    $report .= sprintf("%-17s ||  %8s  || %10s || $queue_stats\n", $logic_name,
		       format_time($res{ $logic_name }{ runtime }), format_memory($res{ $logic_name }{ memory }));
  }

  return $report;
}




# 
# 
# 
# Kim Brugger (24 Jun 2010)
sub total_runtime {

  my $runtime = 0;
  
  foreach my $jms_id ( fetch_jms_ids() ) {
    my $job_id     = $jms_hash{ $jms_id }{ job_id }; 
   
    next if ( $job_id == -1 || !$job_id );

    $runtime += int($backend->job_runtime( $job_id )) || 0;
  }

  return sprintf("Total runtime: %8s\n", format_time( $runtime ));
}



# 
# 
# 
# Kim Brugger (04 Aug 2010)
sub real_runtime {
  return "" if ( ! $end_time || ! $start_time);
  return sprintf("Real runtime: %8s\n", format_time( $end_time - $start_time ));
}


# 
# 
# 
# Kim Brugger (24 Jun 2010)
sub full_report {

  my $report = get_timestamp();

  my %printed_logic_name = ();

  foreach my $jms_id ( sort { $analysis_order{ $jms_hash{ $a }{logic_name}} <=> $analysis_order{ $jms_hash{ $b }{logic_name}} } fetch_jms_ids() ) {   
    my $logic_name = $jms_hash{ $jms_id }{ logic_name};

    if ( ! $printed_logic_name{ $logic_name } ) {
      $report .= "\n| $logic_name\n";
      $report .=  "-="x10 . "-\n";
      $printed_logic_name{ $logic_name }++;
    }

    my $status     = $jms_hash{ $jms_id }{ status }; 
    my $job_id     = $jms_hash{ $jms_id }{ job_id }; 

    my %status2name = ( $FINISHED    => "Finished",
			$FAILED      => "Failed",
			$RUNNING     => "Running",
			$QUEUEING    => "Queueing",
			$RESUBMITTED => "Re-submitted",
			$SUBMITTED   => "Submitted",
			$UNKNOWN     => "Unknown" );
    
    $report .= sprintf("%3d/%-5d\t%12s\tfailures: %d\n", $jms_id, $job_id, $status2name{ $status }, $jms_hash{ $jms_id }{ failed } || 0);

    if ( $job_id != -1 ) {
      $report .= sprintf("Runtime: %s || Memory: %s\n", format_time($backend->job_runtime( $job_id )), format_memory($backend->job_memory( $job_id )));
    }
    if ( $jms_hash{ $jms_id }{ output } && ref ($jms_hash{ $jms_id }{ output }) eq 'ARRAY') {
      $report .= sprintf("cmd/output: %s --> %s\n", $jms_hash{ $jms_id }{ command }, join(",", @{$jms_hash{ $jms_id }{ output }}));
    }
    elsif ( $jms_hash{ $jms_id }{ output } && ref ($jms_hash{ $jms_id }{ output }) eq 'HASH') {
      $report .= sprintf("cmd/output: %s --> %s\n", $jms_hash{ $jms_id }{ command }, join(',',map { "$_=> $jms_hash{ $jms_id }{ output }{$_}"} keys %{$jms_hash{ $jms_id }{ output }}));
    }
    else {
      $report .= sprintf("cmd/output: %s --> %s\n", $jms_hash{ $jms_id }{ command }, ($jms_hash{ $jms_id }{ output } || ""));
    }
  }

  return $report;
}


# 
# 
# 
# Kim Brugger (05 Jul 2010)
sub mail_report {
  my ( $to, $subject, $extra) = @_;

  $subject = "$subject (Error)" if ( $no_restart );

  open(my $mail, " | mail $to -s '[easih-pipeline] $subject'") || die "Could not open mail-pipe: $!\n";

  if ( $no_restart ) {
    print $mail "ERROR :: The pipeline was unsucessful with $no_restart jobs not being able to finish\n";
    print $mail "ERROR :: This might have interrupted the pipeline flow as well\n\n";
  }

  print $mail report() . "\n\n";
  print $mail total_runtime();
  print $mail real_runtime();

  print $mail "Running directory: $cwd, Freeze file: ".(freeze_file())."\n";
  print $mail "easih-pipeline version: " . version() . "\n";

  print $mail $extra. "\n\n";
  
  print $mail full_report(). "\n\n";
  close( $mail );
}



# 
# Wait for the jobs to terminate
# 
# Kim Brugger (22 Apr 2010)
sub check_jobs {

  foreach my $jms_id ( fetch_active_jms_ids ) {


    if ( ! defined $jms_hash{ $jms_id }{ job_id } ) {
      die "'$jms_id' ==> " . Dumper( $jms_hash{ $jms_id }) . "\n";
    }
 
    my $status;
    if ( $jms_hash{ $jms_id }{ job_id } == -1 ) {
      $status = $jms_hash{ $jms_id }{ status };
    }
    else {	
      $status = $backend->job_status( $jms_hash{ $jms_id}{ job_id } );
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
	verbose("Failed, resubmitting job\n", 3);
	resubmit_job( $jms_id );
      }
      else { 
	verbose("Cannot resubmit job ($jms_hash{ $jms_id }{ failed } < $max_retry)\n", 3);
	$no_restart++;
      }
    }
    
  }

  return;
}



# 
# Hard ! resets the pipeline. If a analysis failed it is deleted and
# pushed back to the previous step in the pipeline. If that job
# resulted in multiple child processes then they and all their spawn
# is deleted.
#
# Did I mention this was a HARD reset?
#
# Kim Brugger (26 Apr 2010)
sub hard_reset {
  my ( $freezefile ) = @_;

  if ( ! $freezefile || ! -e $freezefile) {
    print "Cannot do a hard-reset without a freezefile\n";
    exit 1;
  }

  restore_state( $freezefile);

  # Update job statuses...
  check_jobs();

  # Only look at the jobs we are currently tracking
  foreach my $jms_id ( fetch_jms_ids() ) {

    next if (! $jms_hash{ $jms_id });
    next if ($jms_hash{ $jms_id }{ post_jms_id });
    # the analysis depends on a previous analysis, and can be rerun

    if ( $jms_hash{ $jms_id }{ status } == $FAILED ||  $jms_hash{ $jms_id }{ status } == $UNKNOWN) {
      my $pre_jms_id = $jms_hash{ $jms_id }{ pre_jms_id };
      my $pre_logic_name = $jms_hash{ $pre_jms_id }{ logic_name } if ( $pre_jms_id );

      if ($pre_jms_id  && @{$jms_hash{ $pre_jms_id }{ post_jms_id }} > 1 ){
	my @children = @{$jms_hash{ $pre_jms_id }{ post_jms_id }};

	while (my $child = shift @children ) {
	  push @children, @{$jms_hash{ $child }{ post_jms_id }} if ($jms_hash{ $child }{ post_jms_id });
	  delete $jms_hash{ $child };
	}

      }
      print "Resubmitted $pre_jms_id after hard reset downstream (due to $jms_id)\n";
     resubmit_job( $pre_jms_id );
    }
    elsif (! $jms_hash{ $jms_id }{ post_jms_id }) {
      print "Tracking $jms_id\n";
      $jms_hash{ $jms_id }{ tracking } = 1;
      next;
    }

  }
}

# 
# reset the failed states, so the pipeline can run again
# 
# Kim Brugger (26 Apr 2010)
sub reset {
  my ( $freezefile ) = @_;

  if ( ! $freezefile || ! -e $freezefile) {
    print "Cannot do a reset without a freezefile\n";
    exit 1;
  }

  restore_state( $freezefile);

  # Update job statuses...
  check_jobs();

  # Only look at the jobs we are currently tracking
  foreach my $jms_id ( fetch_jms_ids() ) {

    next if ($jms_hash{ $jms_id }{ post_jms_id });
    # the analysis depends on a previous analysis, and can be rerun

    if ( $jms_hash{ $jms_id }{ status } == $FAILED ||  $jms_hash{ $jms_id }{ status } == $UNKNOWN) {
      verbose("Resubmitted $jms_id\n", 10);
      resubmit_job( $jms_id );
    }
    elsif (! $jms_hash{ $jms_id }{ post_jms_id }) {
      verbose( "Tracking $jms_id\n", 10);
      $jms_hash{ $jms_id }{ tracking } = 1;
      next;
    }

  }
}



# 
# 
# 
# Kim Brugger (22 Apr 2010)
sub tmp_file {
  my ($postfix, $keep_file) = @_;
  $postfix ||= "";
  $keep_file || 0;
  
  system "mkdir tmp" if ( ! -d './tmp');

  my ($tmp_fh, $tmp_file) = File::Temp::tempfile(DIR => "./tmp" );
  close ($tmp_fh);
  system "rm -f $tmp_file";

  push @delete_files, "$tmp_file$postfix" if (! $keep_file);

  return "$tmp_file$postfix";
}



# 
# 
# 
# Kim Brugger (17 May 2010)
sub next_analysis {
  my ( $logic_name ) = @_;
  
  my @res;

  my $next = $main::flow{ $logic_name} || undef;
  if ( ref ( $next ) eq "ARRAY" ) {
    @res = @$next;
  }
  elsif ( defined $next ) {
    push @res, $next;
  }
  
  return @res;
}




# 
# 
# 
# Kim Brugger (22 Apr 2010)
sub delete_tmp_files {

  system "rm -f @delete_files";
}





# 
# 
# 
# Kim Brugger (18 May 2010)
sub fetch_jobs {
  my ( @logic_names ) = @_;

#  print " --> @logic_names \n";

  my @jobs;
  foreach my $jms_id ( fetch_jms_ids() ) {    
    push @jobs, $jms_id if ( grep(/$jms_hash{ $jms_id }{ logic_name }/, @logic_names) );
  }

  return @jobs;
}


# 
# Set the pre analysis dependencies for each analysis.
# 
# Kim Brugger (05 Jul 2010)
sub set_analysis_dependencies {
  my ( $logic_name ) = @_;


  my @logic_names = next_analysis( $logic_name );

  foreach my $next_logic_name ( @logic_names) {
    push @{$dependencies{ $next_logic_name }}, $logic_name;
  }

  while ( $logic_name = shift @logic_names  ) {

    my @next_logic_names = next_analysis( $logic_name );
    next if ( ! @next_logic_names );
    push @logic_names, @next_logic_names if (@next_logic_names);
    foreach my $next_logic_name ( @next_logic_names) {
      push @{$dependencies{ $next_logic_name }}, $logic_name;
      push @{$dependencies{ $next_logic_name }}, @{$dependencies{ $logic_name }} if ($dependencies{ $logic_name });
    
    # make sure a logic_name only occurs once.
      my %saw;
      @{$dependencies{ $next_logic_name }} = grep(!$saw{$_}++, @{$dependencies{ $next_logic_name }});
    }
  }

}


# 
# Traverse the flow hash and stores the analysis order
# 
# Kim Brugger (09 Sep 2010)
sub set_analysis_order {
  my ( $logic_name ) = @_;
  
  $analysis_order{ $logic_name } = 1;
  my @logic_names = ( $logic_name );

  while ( $logic_name = shift @logic_names  ) {

    my @next_logic_names = next_analysis( $logic_name );
    

    foreach my $next_logic_name ( @next_logic_names ) {
      
      $analysis_order{ $next_logic_name } = $analysis_order{ $logic_name } + 1 
	  if (! $analysis_order{ $next_logic_name } || 
	      $analysis_order{ $next_logic_name } <= $analysis_order{ $logic_name } + 1);
    }
    push @logic_names, next_analysis( $logic_name );
  }


#  foreach my $key ( sort {$analysis_order{$a} <=> $analysis_order{$b}} keys %analysis_order ) {
#    printf("%03d --> $key\n", $analysis_order{ $key });
#  }
}




# 
# for print_flow, so we can fake dependencies...
# 
# Kim Brugger (05 Jul 2010)
sub waiting_for_analysis {
  my ($logic_name, @done_analyses) = @_;

  return 0 if ( ! $dependencies{ $logic_name });

  my %done;
  map { $done{ $_ }++ } @done_analyses;
  foreach my $dependency ( @{$dependencies{ $logic_name }} ) {
    if ( ! $done{ $dependency} ) {
      verbose("$logic_name is waiting for $dependency\n", 10);
      return 1;
    }
  }
  

  return 0;
}


# 
# 
# 
# Kim Brugger (05 Jul 2010)
sub depends_on_active_jobs {
  my ($logic_name) = @_;

  return 0 if ( ! $dependencies{ $logic_name });

  my %dependency;
  map { $dependency{ $_ }++ } @{$dependencies{ $logic_name }};
  
  foreach my $jms_id ( fetch_jms_ids() ) {
    next if (! $jms_hash{ $jms_id }{ tracking });
    
#    print "$jms_id --> $dependency{ $jms_hash{ $jms_id }{ logic_name }}\n";

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

  # Just to make sure that the script is setup as it should be
  # the overhead of doing this is close to null, and it saves 
  # time if things does not crash.
  validate_flow(@start_logic_names);


  $start_time = Time::HiRes::gettimeofday();

  foreach my $start_logic_name ( @start_logic_names ) {
    set_analysis_dependencies( $start_logic_name );
    set_analysis_order( $start_logic_name );
  }

  while (1) {

    my ($started, $running ) = (0,0);

    my @active_jobs = fetch_active_jms_ids();
       

#    print Dumper( \@active_jobs );
    
    # nothing running, start from the start_logic_names
    if ( ! @active_jobs ) {
      foreach my $start_logic_name ( @start_logic_names ) {
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
          my @next_logic_names = next_analysis( $logic_name );

          # no more steps we can take, jump the the next job;
          if ( ! @next_logic_names ) {
            next;
          }

	  foreach my $next_logic_name ( @next_logic_names ) {

	    # all threads for this run has to finish before we can 
	    # proceed to the next one. If a failed job exists this will never be 
	    # possible
	    if ( $main::analysis{ $next_logic_name }{ sync } ) { 
	      

	      $DB::single = 1 ;;

	      next if ( $no_restart );
	      # we do not go further if new jobs has been started or is running.
	      next if ( @retained_jobs > 0 );

	      next if (depends_on_active_jobs( $next_logic_name));
	      
	      my @depends_on;
	      foreach my $analysis ( keys %main::flow ) {
		push @depends_on, $analysis if ( $main::flow{ $analysis } eq $next_logic_name );
	      }
	      
	      my @depend_jobs = fetch_jobs( @depends_on );
	      
	      my $all_threads_done = 1;
	      foreach my $ljms_id ( @depend_jobs ) {
		if ( $jms_hash{ $ljms_id }{ status } != $FINISHED ) {
		  $all_threads_done = 0;
		  last;
		}
	      }
	      
	      if ( $all_threads_done ) {
		# collect inputs, and set their tracking to 0
		my @inputs;
		foreach my $ljms_id ( @depend_jobs ) {
		  $jms_hash{ $ljms_id }{ tracking } = 0;
		  push @inputs, $jms_hash{ $ljms_id }{ output };
		}
		
		verbose(" $jms_id :: $jms_hash{ $jms_id }{ logic_name }  --> $next_logic_name (synced !!!) $no_restart\n", 2);
		run_analysis( $next_logic_name, $jms_id, @inputs);
		$started++;
	      }
	      
	    }
	    # unsynced part of the pipeline, run the next job.
	    else {
	      verbose(" $jms_id :: $jms_hash{ $jms_id }{ logic_name }  --> $next_logic_name  \n", 2);
	      run_analysis( $next_logic_name, $jms_id, $jms_hash{ $jms_id }{ output });
	      $started++;
	    }
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
    print report();
    last if ( ! $running && ! $started && !@retained_jobs);

    sleep ( $sleep_time );
    check_jobs();
  }
  print total_runtime();
  print real_runtime();

  if ( $no_restart ) {
    print "The pipeline was unsucessful with $no_restart job(s) not being able to finish\n";
  }
  

  verbose( "Retaineded jobs: ". @retained_jobs . " (should be 0)\n", 5);
  $end_time = Time::HiRes::gettimeofday();
  store_state();

  return( $no_restart );
}



# 
# 
# 
# Kim Brugger (18 May 2010)
sub run_analysis {
  my ( $logic_name, $pre_id, @inputs) = @_;

  my $function = function_module($main::analysis{ $logic_name }{ function }, $logic_name);
	
  $current_logic_name = $logic_name;
  $pre_jms_id         = $pre_id || undef;

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

  if ( ! $function ) {
    store_state();
    use Carp;
    
    Carp::confess "$logic_name does not point to a function\n";
  }

  
  
  my $module = 'main';
    
  ($module, $function) = ($1, $2) if ( $function =~ /(.*)::(\w+)/);
  die "ERROR :::: $module is not loaded!!!\n" if ( ! is_loaded( $module ));
  die "ERROR :::: $logic_name points to $module\:\:$function, but this function does not exist!\n" if ( ! $module->can( $function ) );

  return $module . "::" . $function;
}


# 
# 
# 
# Kim Brugger (23 Apr 2010)
sub print_flow {
  my (@start_logic_names) = @_;

  die "EASIH::JMS::print_flow not called with a logic_name\n" if (! @start_logic_names);

  my @analyses;

  foreach my $start_logic_name ( @start_logic_names ) {
    analysis_dependencies( $start_logic_name );
  }

  my @logic_names = @start_logic_names;

  print "Starting with: @logic_names \n";
  print "--------------------------------------------------\n";

  
  while ( $current_logic_name = shift @logic_names ) {

    print "$current_logic_name queue: [@logic_names] \n";

    push @analyses, $current_logic_name;

    if ( ! $main::analysis{$current_logic_name} ) {
      die "ERROR :::: No information on $current_logic_name in main::analysis\n";
    }
    else {
      my $function = function_module($main::analysis{$current_logic_name}{ function }, $current_logic_name);
      print "$current_logic_name ==>  $function\n";
    }
      
    my @next_logic_names = next_analysis( $current_logic_name );

    if ( @next_logic_names ) {
      
      foreach my $next_logic_name ( @next_logic_names ) {
      
	if ($main::analysis{$next_logic_name}{ sync } ) {
	  print "$current_logic_name --> $next_logic_name (Synced!!)\n";
	}
	else {
	  print "$current_logic_name --> $next_logic_name\n";
	}

	if ( waiting_for_analysis($next_logic_name, @analyses)) {
#	  push @logic_names, $next_logic_name;
	}
	else {
	  push @logic_names, $next_logic_name;
	}
      }
    }
    print "--------------------------------------------------\n";
#    print "end of flow\n";
  }

  
}



# 
# 
# 
# Kim Brugger (23 Apr 2010)
sub validate_flow {
  my (@start_logic_names) = @_;

  die "EASIH::JMS::validate_flow not called with a logic_name\n" if (! @start_logic_names);

  my @analyses;

  foreach my $start_logic_name ( @start_logic_names ) {
    set_analysis_dependencies( $start_logic_name );
  }

  my @logic_names = @start_logic_names;

  while ( $current_logic_name = shift @logic_names ) {

    push @analyses, $current_logic_name;

    if ( ! $main::analysis{$current_logic_name} ) {
      die "ERROR :::: No information on $current_logic_name in main::analysis\n";
    }
    else {
      my $function = function_module($main::analysis{$current_logic_name}{ function }, $current_logic_name);
    }
      
    my @next_logic_names = next_analysis( $current_logic_name );

    if ( @next_logic_names ) {
      
      foreach my $next_logic_name ( @next_logic_names ) {
      
	if ($main::analysis{$next_logic_name}{ sync } ) {
	}
	else {
	}

	if ( waiting_for_analysis($next_logic_name, @analyses)) {
#	  push @logic_names, $next_logic_name;
	}
	else {
	  push @logic_names, $next_logic_name;
	}
      }
    }
#    print "end of flow\n";
  }

  verbose("\nEnd of validate_run\n", 1);
  
}



# 
# 
# 
# Kim Brugger (26 Apr 2010)
sub store_state {
  my ($filename ) = @_;

  return if ( ! $use_storing );

  $filename = freeze_file();
  
  verbose("JMS :: Storing state in: '$filename'\n", 2);

  my $blob = {delete_files       => \@delete_files,
	      jms_hash           => \%jms_hash,
	      save_interval      => $save_interval,
	      last_save          => $last_save,
	      save_interval      => $save_interval,
	      max_retry          => $max_retry,
	      sleep_time         => $sleep_time,
	      max_jobs           => $max_jobs,
	      backend            => $backend,
	      job_counter        => $job_counter,
	      start_time         => $start_time,
	      end_time           => $end_time,
	      
	      stats              => $backend->stats,
	      
	      retained_jobs      => \@retained_jobs,
	      current_logic_name => $current_logic_name,

	      #main file variables.
	      argv               => \@argv,
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

  
  verbose("JMS :: Re-storing state from: '$filename'\n", 2);

  my $blob = Storable::retrieve( $filename);

  @delete_files       = @{$$blob{delete_files}};
  %jms_hash           = %{$$blob{jms_hash}};

  $save_interval      = $$blob{save_interval};

  $last_save          = $$blob{last_save};
  $save_interval      = $$blob{save_interval};
  $max_retry          = $$blob{max_retry};
  $sleep_time         = $$blob{sleep_time};
  $max_jobs           = $$blob{max_jobs};
  $job_counter        = $$blob{job_counter};
  
  $start_time         = $$blob{start_time};
  $end_time           = $$blob{end_time};
	      
  @retained_jobs      = @{$$blob{retained_jobs}};
  $current_logic_name = $$blob{current_logic_name};

  @main::ARGV         = @{$$blob{argv}};
  %analysis_order     = %{$$blob{analysis_order}};
  %dependencies       = %{$$blob{dependencies}} if ($$blob{dependencies});

#  hive($$blob{hive}) if ( $$blob{hive} );
  backend($$blob{backend}) if ( $$blob{backend});
  $backend->stats($$blob{stats});
  
  # Overwrite the argv array with the values just loaded...
  @argv = @main::ARGV;

}


sub catch_ctrl_c {
    $main::SIG{INT } = \&catch_ctrl_c;
    $main::SIG{KILL} = \&catch_ctrl_c;
    $main::SIG{HUP } = \&catch_ctrl_c;
    fail("Caught a ctrl-c\n");
    store_state();
}


BEGIN {
  $SIG{INT} = \&catch_ctrl_c;
  $SIG{KILL} = \&catch_ctrl_c;
  $SIG{HUP} = \&catch_ctrl_c;
  @argv = @main::ARGV;
}


1;




