#!/usr/bin/perl 
# 
# Takes a JMs freeze and changes it into data::dumper format for viewing and editing.
# 
# 
# Kim Brugger (06 May 2010), contact: kim.brugger@easih.ac.uk

use strict;
use warnings;
use Data::Dumper;
use Storable;

my $dumpfile   = shift || die "no dump file provided\n";
my $freezefile = shift || die "no freeze file provided\n";

my $blob;
open (my $in, $dumpfile) || die "Could not open '$dumpfile': $1\n";
while (<$in>) {
  $blob .= $_;
}
$blob =~ s/^\$VAR\d+\s*=\s*//;

print ( $blob );
$blob = eval {$blob};
print Dumper( $blob );
#Storable::store($blob, $freezefile);
