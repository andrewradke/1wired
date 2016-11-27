#!/usr/bin/perl -w

use strict;
use Getopt::Long;

my ($device1, $device2, $test1, $test2, $test, $range, $warn, $critical, $percent, $verbose, $help);
my ($warntext, $criticaltext);
my $status;
my ($difference, $absdifference);
my ($device1data, $device2data);

Getopt::Long::Configure('bundling');
GetOptions(
	"device1=s"	=> \$device1,
	"device2=s"	=> \$device2,
	"test1=s"	=> \$test1,
	"test2=s"	=> \$test2,
	"T=s"	=> \$test,	"test=s"	=> \$test,
	"R=s"	=> \$range,	"range=s"	=> \$range,
	"w=s"	=> \$warn,	"warn=s"	=> \$warn,
	"c=s"	=> \$critical,	"critical=s"	=> \$critical,
	"p"	=> \$percent,	"percent"	=> \$percent,
	"v"	=> \$verbose,	"verbose"	=> \$verbose,
	"h"	=> \$help,	"help"		=> \$help,
	);
$help = 1 unless ($device1);
$help = 1 unless ($device2);
$help = 1 unless ( ($test) || ($test1 && $test2) );
$help = 1 if ( ($test) && ($test1 || $test2) );
unless ($range) {
  $help = 1 unless (($warn && $critical));
}
$warntext     = $warn;
$criticaltext = $critical;
$warntext     = "+-$warn"     if ($warn =~ m/^\d+(.\d+|)$/);
$criticaltext = "+-$critical" if ($critical =~ m/^\d+(.\d+|)$/);

if ($help) {
  print "Valid options:
	--device1=<DeviceName>	# REQUIRED
	--device2=<DeviceName>	# REQUIRED
	-T|--test=<TestName>	# REQUIRED unless test1 AND test2 supplied
	--test1=<TestName>	# test on device1
	--test2=<TestName>	# test on device2
	-R|--range=<range>	# REQUIRED unless warn AND critical supplied
	-w|--warn=<range>	# Use NA if no warn value to be used
	-c|--critical=<range>	# Use NA if no critical value to be used
	-p|--percent		# Compare percentages instead of raw values
	-v|--verbose
	-h|--help

";
  exit 3;
}

if ($test) {
  $device1data=`/usr/local/bin/1wire-query.pl --device="$device1" --test="$test"`;
  $device2data=`/usr/local/bin/1wire-query.pl --device="$device2" --test="$test"`;
} else {
  $device1data=`/usr/local/bin/1wire-query.pl --device="$device1" --test="$test1"`;
  $device2data=`/usr/local/bin/1wire-query.pl --device="$device2" --test="$test2"`;
}
chomp($device1data);
chomp($device2data);
unless (($device1data =~ m/^-?\d+(\.\d+|)$/) && ($device2data =~ m/^-?\d+(\.\d+|)$/)) {
  print "Unrecognised values returned by query: $device1: $device1data ; $device2: $device2data\n";
  exit 3;
}

if ($percent) {
  $difference = ( ( 1 - $device2data / $device1data ) * 100 );
  $absdifference = $difference;
} else {
  $difference = $device1data - $device2data;
  $absdifference = $difference;
}
$difference *= -1 if ( $difference < 0 );

if ($range) {
  $status = CheckRange($range);
  print "range:\t$range\t$status\n";
} else {
  if (lc($critical) eq 'na') {
    $status = 'OK';
  } else {
    $status = CheckRange($critical);
  }
  if ($status ne 'OK') {
    if ($status eq 'UNKNOWN') {
      print "Unrecognised range: $critical\n";
      exit 3;
    } else {
      print "CRITICAL: " . restrict_num_decimal_digits($difference, 2) , ($percent) ? "%" : "" , " ($device1:$test1 $device1data" , ($percent) ? "," : " -" , " $device2:$test2 $device2data > $criticaltext", ($percent) ? "%" : "" , ")\n";
      exit 2;
    }
  } else {
    if (lc($warn) eq 'na') {
      $status = 'OK';
    } else {
      $status = CheckRange($warn);
    }
    if ($status ne 'OK') {
      if ($status eq 'UNKNOWN') {
        print "Unrecognised range: $warn\n";
        exit 3;
      } else {
        print "WARNING: " . restrict_num_decimal_digits($difference, 2) , ($percent) ? "%" : "" , " ($device1:$test1 $device1data" , ($percent) ? "," : " -" , " $device2:$test2 $device2data > $warntext", ($percent) ? "%" : "" , ")\n";
        exit 1;
      }
    } else {
      if (lc($warn) eq 'na') {
        print "OK: " . restrict_num_decimal_digits($difference, 2) , ($percent) ? "%" : "" , " ($device1:$test1 $device1data" , ($percent) ? "," : " -" , " $device2:$test2 $device2data < $criticaltext", ($percent) ? "%" : "" , ")\n";
      } else {
        print "OK: " . restrict_num_decimal_digits($difference, 2) , ($percent) ? "%" : "" , " ($device1:$test1 $device1data" , ($percent) ? "," : " -" , " $device2:$test2 $device2data < $warntext", ($percent) ? "%" : "" , ")\n";
      }
      exit 0;
    }
  }
}



sub CheckRange {
  my $range = shift;
  if ($range =~ s/^(\+-|)(\d+(\.\d+|))$/$2/) {
    if ($difference >= $range) {
      return "BAD";
    } else {
      return "OK";
    }
  } elsif ($range =~ s/^>(\d+(\.\d+|))$/$1/) {
    if (($absdifference) > $range) {
      return "BAD";
    } else {
      return "OK";
    }
  } elsif ($range =~ s/^<(\d+(\.\d+|))$/$1/) {
    if (($absdifference) < $range) {
      return "BAD";
    } else {
      return "OK";
    }
  } else {
    return "UNKNOWN";
  }
}

sub restrict_num_decimal_digits {
  my $num=shift;
  my $digs_to_cut=shift;
  if ($num=~/\d+\.(\d){$digs_to_cut,}/) {
    $num=sprintf("%.".($digs_to_cut)."f", $num);
  }
  return $num;
}
