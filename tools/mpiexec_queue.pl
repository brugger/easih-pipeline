#!/usr/bin/perl -w
# 
# 
# 
# 
# 
#
# Kim Brugger (30 Nov 2010), contact: kim.brugger@easih.ac.uk

use strict;
use POSIX ':sys_wait_h';
use Getopt::Std;
use File::Temp;
use Sys::Hostname;

my %opts;
getopts("c:", \%opts);

my $cpus = nr_of_cpus();
my $MAX_NODES = $opts{c} || $cpus;
my $infile = shift || die "USAGE $0 -c[pus to use, (runs on $cpus cpus by default)] COMMAND-INFILE \n";

# Store the user specifed values in more readable named variables.
my $INFILE    = $infile;

my @cpids = ();
my %processes = ();
my $host = hostname;
my $pid  = $$;
chomp($pid);

my $running_counter = 0;

my $done   = 0; # to track the number of files handled
my $failed = 0;
my $total  = 0;
my $running_nodes  = 0;
my $overhead_nodes = 0;

my (@commands, @completed);

open (my $in, $infile) || die "Could not open '$infile': $!\n";
my $i = 0;
while( <$in>) {
  next if (/^\s*\z/); # empty line
  next if (/^\#/);    # comment...
  chomp;

  if ( /^(\-{0,1}\d+)\t(.*)/) {
    if ( $1 == 0 ) {
      push @completed, $_;
      next;
    }
    else {
      $_ = $2;
    }
  }

  my $error_file = " $host.$pid.$i";
  push @commands, [$_, $error_file] ;
  $i++;

  $total++;
}  
close $in;

open ( *STDERR, ">$infile.returns") || die "Could not open '$infile': $!\n";
foreach my $c ( @completed ) {
  print STDERR "$c\n";
}


while ($done < $total ) {

 FREE_NODE:
#  print STDERR "$total vs $done, ". @commands ." left :: $running_nodes + $overhead_nodes\n";
  last if ( $done == $total);
  last if ( $failed && $running_nodes == 0);
#  @cpids = grep(!/-10/, @cpids);

  if ( @commands && ($running_nodes + $overhead_nodes) <  $MAX_NODES && ! $failed) {    
    $_ = shift @commands;
    my ($c, $error_file) = @$_;
    system "rm -f $error_file" if ( -e $error_file);
    
    my $command = "mpiexec -comm none -n 1 $c 2> $error_file";
    my $cpid = create_child($command, $error_file);
#    print STDERR "$$cpid -> $command \n";
    $processes{$$cpid} = $_;
    $running_nodes++;
    push @cpids, [$cpid, $c, $error_file];
  }
  else {
    # loop through the nodes to see when one becomes available ...
    while ($running_nodes) {
      for (my $i = 0; $i <@cpids; $i++) {
	next if ($cpids[$i][0] == -10);

	my $cpid = $cpids[$i][0];
	if (!waitpid($$cpid, WNOHANG)) {
#	  print STDERR "Waiting for ($$cpid)\n";
	}
	elsif ($$cpid != -10) {
	  my $return_value = $? >> 8;
#	  print STDERR "$$cpid -> $? $return_value\n";
	  $running_nodes--;
	  if ( $return_value) {
	    my $error = "";
	    open (my $e, $cpids[$i][2]) || die "Could not open '$cpids[$i][2]': $!\n";
	    $error = join("\n", <$e>);
	    close $e;
	    
#	    print STDERR "$cpids[$i][2] --> $error \n";
	    
	    if ( $error =~  /no processors left in overall allocation/) {
	      $overhead_nodes += $MAX_NODES;
	      unshift @commands, $processes{$$cpid};
	    }
	    else {
#	      print STDERR $error;
	      $done++;
              $failed++;
	      print STDERR "$return_value\t$cpids[$i][1]\n";
	    }
	  }
	  else {
	    system "rm -f $cpids[$i][2]";
	    print STDERR "$return_value\t$cpids[$i][1]\n";
	    $done++;
	    $overhead_nodes = 0 if ( $overhead_nodes > 0);
	  }

	  $cpids[$i] = [-10];
	}
      }
      sleep 4;
      last if ($running_nodes < $MAX_NODES);
    }
    last if ( $done == $total);
    goto FREE_NODE;
  }

}

foreach my $c ( @commands ) {
  my ($c, $error_file) = @$c;
  print STDERR "-1\t$c\n";
}


#print STDERR "Done ... \n";

exit $failed;

while ($done < $total) {
  for (my $i = 0; $i <@cpids; $i++) {
    next if ($cpids[$i] == -10);
    
    my $cpid = $cpids[$i];
    if (!waitpid($$cpid, WNOHANG)) {
      ;
    }
    elsif ($$cpid != -10) {
      $done++;
      $cpids[$i] = -10;
    }
  }
  sleep 4;
}


sub create_child {
  my ($command, $error_file) = @_;

#  print STDERR "submitting $command\n";
  my $return_value = 0;  
  my $pid;
  if ($pid = fork) {
    ;
  } 
  else {
    die "cannot fork: $!" unless defined $pid;

    sleep(int(rand(30)+10));

    # if the process crashes, run it again, with a limit of 2 times...
    system($command);
    
    $return_value = $? >> 8;
#    print STDERR "$command ::  $return_value\n";
    
    if ( $return_value == 1 ) {
      $overhead_nodes++;
    }
    
    exit $return_value if ( $return_value );
    exit 0;
  }

  $return_value = $? >> 8;
#  print STDERR "PID == $pid \n";
  
  return \$pid;
}


# 
# 
# 
# Kim Brugger (13 Jan 2011)
sub nr_of_cpus {

  my $cpus = `cat /proc/cpuinfo | egrep ^proc | wc -l`;
  chomp $cpus;
  return $cpus;
}


