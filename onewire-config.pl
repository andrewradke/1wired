#!/usr/bin/perl -w

use strict;
use Getopt::Long;

my ($opt_c, $opt_v, $opt_h);

Getopt::Long::Configure('bundling');
GetOptions(
	"c=s"	=> \$opt_c, "config=s"	=> \$opt_c,
	"v"	=> \$opt_v, "verbose"	=> \$opt_v,
	"h"	=> \$opt_h, "help"	=> \$opt_h,
	);

my $ConfigFile = '/etc/onewired/onewired.conf';

my $DeviceFile = '/etc/onewired/devices';
my $StateFile = '/etc/onewired/state';

if ($opt_h) {
  print "Valid options:
	-c|--config=<ConfigFile>	# Default: $ConfigFile
	-v|--verbose
	-h|--help

";
  exit;
}

$ConfigFile = $opt_c if ($opt_c);

my $StateData;
my ($StateGood, $StateWarning, $StateError);

my ($option, $value);
open(CONFIG,  "<$ConfigFile") or die "Can't open config file ($ConfigFile): $!";
while (<CONFIG>) {
  chomp;
  s/\w*#.*//;
  next if (m/^\s*$/);
  if (m/^([A-Za-z0-9]+)\s*=\s*(.+)\s*$/) {
    $option = $1;
    $value = $2;
    if ($option eq 'DeviceFile') {
      $DeviceFile = $value;
    } elsif ($option eq 'StateFile') {
      $StateFile = $value;
    }
  }
}
close(CONFIG);
$option = undef;
$value = undef;

my ($name, $address, $test, $state, %StateData);

open(STATE,  "<$StateFile") or die "Can't open config file ($StateFile): $!";
while (<STATE>) {
  chomp;
  s/\w*#.*//;
  next if (m/^\s*$/);
  if (m/^([A-Za-z0-9-_]+)\s+([A-Za-z]+)\s+(.*)\s*$/) {
    $name = $1;
    $test = $2;
    $StateData = $3;
    if ($StateData =~ m/^([0-9<>.,-]+);\s*([0-9<>.,-]*);\s*([0-9<>.,-]*)\s*$/) {
      $StateGood = $1;
      $StateWarning = $2;
      $StateError = $3;
    } elsif ($StateData =~ m/^((?:,?\d+min[0-9<>.-]+)*);\s*((?:,?\d+min[0-9<>.-]+)*);\s*((?:,?\d+min[0-9<>.-]+)*)\s*$/i) {
      $StateGood = $1;
      $StateWarning = $2;
      $StateError = $3;
    } else {
      print "Couldn't parse state data: $StateData\n";
      $StateGood = '';
      $StateWarning = '';
      $StateError = '';
    }
    $StateData{$name}{$test}{OK} = $StateGood;
    $StateData{$name}{$test}{WARN} = $StateWarning;
    $StateData{$name}{$test}{CRIT} = $StateError;
  }
}
close(STATE);

print '<form name="config" method="post" action="#">'."\n";

print '<table class="NagiosConfig">
<thead>
<tr><td rowspan="2">Sensor</td><td rowspan="2">Test</td><td colspan="3" align="center">State</td></tr>
<tr><td>OK</td><td>Warning</td><td>Critical</td></tr>
</thead>'."\n";

my ($testnum,$statenum);
foreach $name (sort(keys(%StateData))) {
  $testnum = 0;
  print "<tr><td>$name</td> ";
  foreach $test (sort(keys(%{$StateData{$name}}))) {
    $testnum++;
    print "</tr>\n<tr><td></td>" if ($testnum > 1);
    print "<td><img style=\"float:right;\" src=\"/nagios/images/local/$test.png\" height=21 width=21>$test</td> ";
    foreach $state ('OK', 'WARN', 'CRIT') {
      $statenum = 0;
      print "<td><input type=\"text\" name=\"$name:$test:$state\" id=\"$name:$test:$state\" value=\"";
      foreach (split(/,/, $StateData{$name}{$test}{$state})) {
        $statenum++;
        print ", " if ($statenum > 1);
        s/^(\d+min)/$1 /i;
        print "$_";
      }
      print "\"></td>\n";
    }
  }
  print "</tr>\n";
}
print "</table>\n";
print "</form>\n";
