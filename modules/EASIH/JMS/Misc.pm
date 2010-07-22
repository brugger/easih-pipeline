package EASIH::JMS::Misc;
#
# Misc functions that does not fit anywhere else, but multiple scripts rely on. 
# 
# 
# 
# Kim Brugger (13 Jul 2010), contact: kim.brugger@easih.ac.uk

use strict;
use warnings;
use Data::Dumper;

use EASIH::JMS;



# 
# 
# 
# Kim Brugger (13 Jul 2010)
sub find_program {
  my ($program) = @_;


  my @paths = ("/home/easih/bin/",
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




1;
