package EASIH::Pipeline::Misc;
#
# Misc functions that does not fit anywhere else, but multiple scripts rely on. 
# 
# 
# 
# Kim Brugger (13 Jul 2010), contact: kim.brugger@easih.ac.uk

use strict;
use warnings;
use Data::Dumper;

use EASIH::Pipeline;



# 
# 
# 
# Kim Brugger (13 Jul 2010)
sub find_program {
  my ($program) = @_;


  my @paths = ("/home/easih/bin/",
#	       "/home/cjp64/bin/",
#	       "/home/cjp64/git/easih-toolbox/scripts/",
	       "/home/kb468/bin/",
	       "/home/kb468/easih-toolbox/scripts/",
	       "/usr/local/bin");
  
  foreach my $path ( @paths ) {
    
    return "$path/$program" if ( -e "$path/$program" );
  }

  my $location = `which $program`;
  chomp( $location);
  
  return $location if ( $location );

  return undef;
}



# 
# 
# 
# Kim Brugger (08 Nov 2010)
sub bwa_version {
  my ($bwa ) = @_;

  $bwa = find_program('bwa') if ( ! $bwa );
  return "Cannot find bwa\n" if ( ! $bwa );

  my $version = `$bwa  2>&1 | head -n 3 | tail -n1`;
  chomp( $version );
  return $version;
}




1;
