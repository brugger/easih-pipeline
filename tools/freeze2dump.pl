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

my $filename = shift || die "no freeze file provided\n";

my $blob = Storable::retrieve( $filename);

print Dumper( $blob );
