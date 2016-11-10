#!/usr/bin/perl -w

use strict;
use Getopt::Long;

my ($device1, $device2, $test1, $test2, $verbose, $help);
my $status;

Getopt::Long::Configure('bundling');
GetOptions(
	"device1=s"	=> \$device1,
	"device2=s"	=> \$device2,
	"test1=s"	=> \$test1,
	"test2=s"	=> \$test2,
	"v"	=> \$verbose,	"verbose"	=> \$verbose,
	"h"	=> \$help,	"help"		=> \$help,
	);
$help = 1 unless ($device1);
$help = 1 unless ($device2);
$help = 1 unless ($test1);
$help = 1 unless ($test2);

if ($help) {
  print "Valid options:
	--device1=<DeviceName>	# REQUIRED
	--device2=<DeviceName>	# REQUIRED
	--test1=<TestName>	# REQUIRED
	--test2<TestName>	# REQUIRED
	-v|--verbose
	-h|--help

";
  exit 3;
}

my $ReturnedData;
$ReturnedData=`/usr/local/bin/1wire-query.pl --device="$device1" --test="$test1" --state || /usr/local/bin/1wire-query.pl --device="$device2" --test="$test2" --state`;
chomp($ReturnedData);
if ($? == -1) {
  print "ERROR: check failed to run: $!\n";
  exit 3;
} else {
  $? = ($? >> 8);
  print "$ReturnedData\n";
  exit $?;
}
