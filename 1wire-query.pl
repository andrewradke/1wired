#!/usr/bin/perl -w

use strict;
use IO::Socket::INET;
use Getopt::Long;

my ($device, $command, $test, $statecheck, $ConfigFile, $timeout, $verbose, $MinuteMax, $help);

$ConfigFile = '/etc/1wired/1wired.conf';
$timeout = 10;

Getopt::Long::Configure('bundling');
GetOptions(
	"D=s"	=> \$device,		"device=s"	=> \$device,
	"C=s"	=> \$command,		"command=s"	=> \$command,
	"T=s"	=> \$test,		"test=s"	=> \$test,
	"s"	=> \$statecheck,	"state"		=> \$statecheck,
	"m"	=> \$MinuteMax,		"minute"	=> \$MinuteMax,
	"c=s"	=> \$ConfigFile,	"config=s"	=> \$ConfigFile,
	"t=s"	=> \$timeout,		"timeout=s"	=> \$timeout,
	"v"	=> \$verbose,		"verbose"	=> \$verbose,
	"h"	=> \$help,		"help"		=> \$help,
	);

my $DeviceFile = '/etc/1wired/devices';
my $StateFile = '/etc/1wired/state';
my $UseRRDs = 0;
my $RRDsDir = "/var/1wired";
my $hostname = 'localhost';
my $ListenPort = 2345;

if ($help) {
  print "Valid options:
	-D|--device=<DeviceName>	# REQUIRED unless -C used
	-C|--command=<command>		# All other options except -t ignored
	-T|--test=<TestName>		# Only used with -s|--state
	-s|--state			# Check state
	-m|--minute			# Report 1 minute maximum value
	-c|--config=<ConfigFile>	# Default: $ConfigFile
	-t|--timeout=<number>		# Default: $timeout
	-v|--verbose
	-h|--help

";
  exit;
}

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
    } elsif ($option eq 'UseRRDs') {
      $UseRRDs = $value;
      $UseRRDs = 0 if ($UseRRDs =~ m/^(no|false)$/i);
    } elsif ($option eq 'RRDsDir') {
      $RRDsDir = $value;
    } elsif ($option eq 'ListenPort') {
      if ($value =~ m/^\d+$/) {
        if ($ListenPort <= 65535 && $ListenPort > 1024) {
          $ListenPort = $value;
        } else {          die "Port defined in config file ($ListenPort) is not within the range 1025-65535. Exiting.\n";
        }
      } else {
        $ListenPort = $value;
        #die "Port defined in config file ($ListenPort) is not a number. Exiting.\n";           ### If this isn't a UNIX system then this should fail
      }
    }
  }
}
close(CONFIG);
$option = undef;
$value = undef;

if ($UseRRDs) {
  use RRDs;
  unless ( (-d $RRDsDir) && (-r $RRDsDir) ) {
    print "ERROR: Can't read from RRD dir '$RRDsDir'. Disabling using RRDs.\n";
    $UseRRDs = 0;
  }
}

if ($statecheck) {
  $test = "temperature" unless ($test);
  open(STATE,  "<$StateFile") or die "Can't open config file ($StateFile): $!";
  while (<STATE>) {
    chomp;
    s/\w*#.*//;
    next if (m/^\s*$/);
    if (m/^([A-Za-z0-9-_]+)\s+([A-Za-z]+)\s+(.*)\s*$/) {
      if (lc($device) eq lc($1) && lc($test) eq lc($2)) {
        $StateData = $3;
        if ($StateData =~ m/^([0-9<>=.,-]+);\s*([0-9<>=.,-]*);\s*([0-9<>=.,-]*)\s*$/) {
          $StateGood = $1;
          $StateWarning = $2;
          $StateError = $3;
        } elsif ($StateData =~ m/^((?:,?\d+min[0-9<>=.-]+)*);\s*((?:,?\d+min[0-9<>=.-]+)*);\s*((?:,?\d+min[0-9<>=.-]+)*)\s*$/i) {
          $StateGood = $1;
          $StateWarning = $2;
          $StateError = $3;
        } else {
          print "Couldn't parse state data: $StateData\n";
          $StateGood = '';
          $StateWarning = '';
          $StateError = '';
        }
        last;
      }
    }
  }
  close(STATE);
}

# Just in case of problems, let's not hang
$SIG{'ALRM'} = sub {
        print ("No response from 1wired server (alarm)\n");
        exit 3;
};
alarm($timeout);

my $query = 'temperature';
if (defined($ARGV[1])) {
  $query = lc($ARGV[1]);
}

unless ($command) {
  die "No device name specified!\n" unless ($device);
}

my $return;

my $retry = 0;

my $socket;
if ($ListenPort =~ m/^\d+$/) {
  my $retry = 0;
  $socket = IO::Socket::INET->new (
		PeerAddr => $hostname,
		PeerPort => $ListenPort,
		Proto    => 'tcp',
		);
  while (! $socket) {
    $retry++;
    if ($retry > 5) {
      print "Couldn't connect to monitoring daemon on $hostname:$ListenPort: $!\n";
      exit 3;
    }
    sleep ($retry * 5);
    $socket = IO::Socket::INET->new (
		PeerAddr => $hostname,
		PeerPort => $ListenPort,
		Proto    => 'tcp',
		);
  }
  unless ($socket) {
    print "Couldn't connect to monitoring daemon on $hostname:$ListenPort: $!\n";
    exit 3;
  }
} else {
  my $retry = 0;
  $socket = IO::Socket::UNIX->new (
		Peer    => $ListenPort,
		Type    => SOCK_STREAM,
		Timeout => 5,
		);
  while (! $socket) {
    $retry++;
    if ($retry > 5) {
      print "Couldn't connect to monitoring daemon on $hostname:$ListenPort: $!\n";
      exit 3;
    }
    sleep ($retry * 5);
    $socket = IO::Socket::UNIX->new (
		Peer    => $ListenPort,
		Type    => SOCK_STREAM,
		Timeout => 5,
		);
  }
  unless ($socket) {
    print "Couldn't connect to monitoring daemon on $ListenPort: $!\n";
    exit 3;
  }
}

$socket->recv($return,255);
chomp($return);
unless ($return eq '220 1wired') {
  print "Unexpected reply from monitoring daemon:\n$return\n";
  exit 2;
}


if ($command) {
  ### If a command has been given send it, print the results and exit
  $socket->send("$command\n");
  sleep 0.1;
  while (<$socket>) { print; }
  exit;
}


$socket->send("value $device\n");
sleep 0.1;
$socket->recv($return,1024);
chomp($return);

my ($name, $address, $type, $temperature, $voltage, $minute, $FiveMinute, $updated, $age, $iminute);
my %data;
my ($range, $max);
my $normal = '';

($name) = $return =~ m/name: ([^\n]*)/;
if ($name) {
  foreach (split(/\n/, $return)) {
    m/^([^:]+): ([^\n]*)$/;
    $data{$1} = $2;
  }
  ($address)     = $return =~ m/address: ([^\n]*)/;
  ($type)        = $return =~ m/type: ([^\n]*)/;
  ($temperature) = $return =~ m/temperature: ([^\n]*)/;
  ($voltage)     = $return =~ m/$type: ([^\n]*)/;
  ($minute)      = $return =~ m/1MinuteMax: ([^\n]*)/;
  ($FiveMinute)  = $return =~ m/5MinuteMax: ([^\n]*)/;
  ($updated)     = $return =~ m/updated: ([^\n]*)/;
  ($age)         = $return =~ m/age: ([^\n]*)/;
  ($iminute)     = $return =~ m/Current1MinuteMax: ([^\n]*)/;
  $voltage       = 0 unless ($voltage);
  $minute        = 0 unless ($minute);
  $FiveMinute    = 0 unless ($FiveMinute);
  $iminute       = 0 unless ($iminute);
  $temperature   = restrict_num_decimal_digits($temperature,2);
  $voltage       = restrict_num_decimal_digits($voltage,2);
  $minute        = restrict_num_decimal_digits($minute,2);
  $FiveMinute    = restrict_num_decimal_digits($FiveMinute,2);
  $data{temperature} = $temperature;
  $data{$type}   = $minute;

  my ($start, $step, $data);		### RRD data

  if ($age > 30) {
    print "$test data stale (" . age_text($age) . " old)\n";
    exit 3;
  }

  if ($statecheck) {
    if ($test eq 'dryness') {
      if ($iminute < 200) {
        print "$device reports as dry ($iminute < 200).\n";
        exit 0;
      } else {
        print "$device reports as possibly wet ($iminute >= 200).\n";
        exit 2;
      }
    }
    if ($data{$test} eq 'NA') {
      print "No state data found for $device:$test\n";
      exit 3;
    }
    if ($StateData) {
      $normal = "(NORMAL: $StateGood)";
      foreach (split(/,/, $StateGood)) {
        if (s/^(\d+)min//i) {
          if (! $UseRRDs) {
            print "Invalid state data for $device:$test specified without RRD configured.\n";
            exit 3;
          }
          $range = $1;
# Don't bother with the RRDs if
#	the check is for > x and current value is > x
#	the check is for > x and the range is >= 5 min and the 5 min max is > x
#	the range is exactly 5 minutes
          if ((m/^>(.*)/) && ($return = checkrange($data{$test},$_))) {
            #print "current $test is $data{$test} ($_);  $normal\n";
            print "current $test is $data{$test};  $normal\n";
            exit 0;
          } elsif (($range >= 5) && (m/^>(.*)/) && ($return = checkrange($FiveMinute,$_))) {
            #print "$range minute(s) maximum $test: $FiveMinute ($_);  $normal\n";
            print "$range minute(s) maximum $test: $FiveMinute;  $normal\n";
            exit 0;
#          } elsif ($range == 5) {
#            if (checkrange($FiveMinute,$_)) {
#              #print "$range minute(s) maximum $test: $FiveMinute ($_);  $normal\n";
#              print "$range minute(s) maximum $test: $FiveMinute;  $normal\n";
#              exit 0;
#            }
          } else {
            ($start, $step, $data) = check_rrds($name) if (! defined($data));
            $max = restrict_num_decimal_digits(MaxInRange($start, $step, $data, $range),1);
            $max = $data{$test} if ($data{$test} > $max);
            if ($return = checkrange($max,$_)) {
              #print "$range minute(s) maximum $test: $max ($_)\n";
              print "$range minute(s) maximum $test: $max\n";
              exit 0;
            }
          }
        } else {
          if ($return = checkrange($data{$test},$_)) {
            #print "$test is $data{$test} ($_)\n";
            print "$test is $data{$test} ($StateGood)\n";
            exit 0;
          }
        }
      }
      foreach (split(/,/, $StateWarning)) {
        if (s/^(\d+)min//i) {
          if (! $UseRRDs) {
            print "Invalid state data for $device:$test specified without RRD configured.\n";
            exit 3;
          }
          $range = $1;
# Don't bother with the RRDs if
#	the check is for > x and current value is > x
#	the check is for > x and the range is >= 5 min and the 5 min max is > x
#	the range is exactly 5 minutes
          if ((m/^>(.*)/) && ($return = checkrange($data{$test},$_))) {
            #print "current $test is $data{$test} ($_);  $normal\n";
            print "current $test is $data{$test};  $normal\n";
            exit 1;
          } elsif (($range >= 5) && (m/^>(.*)/) && ($return = checkrange($FiveMinute,$_))) {
            #print "$range minute(s) maximum $test: $FiveMinute ($_);  $normal\n";
            print "$range minute(s) maximum $test: $FiveMinute;  $normal\n";
            exit 1;
#          } elsif ($range == 5) {
#            if (checkrange($FiveMinute,$_)) {
#              #print "$range minute(s) maximum $test: $FiveMinute ($_);  $normal\n";
#              print "$range minute(s) maximum $test: $FiveMinute;  $normal\n";
#              exit 1;
#            }
          } else {
            ($start, $step, $data) = check_rrds($name) if (! defined($data));
            $max = restrict_num_decimal_digits(MaxInRange($start, $step, $data, $range),1);
            $max = $data{$test} if ($data{$test} > $max);
            if ($return = checkrange($max,$_)) {
              #print "$range minute(s) maximum $test: $max ($_);  $normal\n";
              print "$range minute(s) maximum $test: $max;  $normal\n";
              exit 1;
            }
          }
        } else {
          if ($return = checkrange($data{$test},$_)) {
            #print "$test is $data{$test} ($_);  $normal\n";
            print "$test is $data{$test};  $normal\n";
            exit 1;
          }
        }
      }
      foreach (split(/,/, $StateError)) {
        if (s/^(\d+)min//i) {
          if (! $UseRRDs) {
            print "Invalid state data for $device:$test specified without RRD configured.\n";
            exit 3;
          }
          $range = $1;
# Don't bother with the RRDs if
#	the check is for > x and current value is > x
#	the check is for > x and the range is >= 5 min and the 5 min max is > x
#	the range is exactly 5 minutes
          if ((m/^>(.*)/) && ($return = checkrange($data{$test},$_))) {
            #print "current $test is $data{$test} ($_);  $normal\n";
            print "current $test is $data{$test};  $normal\n";
            exit 2;
          } elsif (($range >= 5) && (m/^>(.*)/) && ($return = checkrange($FiveMinute,$_))) {
            #print "$range minute(s) maximum $test: $FiveMinute ($_);  $normal\n";
            print "$range minute(s) maximum $test: $FiveMinute;  $normal\n";
            exit 2;
#          } elsif ($range == 5) {
#            if (checkrange($FiveMinute,$_)) {
#              #print "$range minute(s) maximum $test: $FiveMinute ($_);  $normal\n";
#              print "$range minute(s) maximum $test: $FiveMinute;  $normal\n";
#              exit 2;
#            }
          } else {
            ($start, $step, $data) = check_rrds($name) if (! defined($data));
            $max = restrict_num_decimal_digits(MaxInRange($start, $step, $data, $range),1);
            $max = $data{$test} if ($data{$test} > $max);
            if ($return = checkrange($max,$_)) {
              #print "$range minute(s) maximum $test: $max ($_);  $normal\n";
              print "$range minute(s) maximum $test: $max;  $normal\n";
              exit 2;
            }
          }
        } else {
          if ($return = checkrange($data{$test},$_)) {
            #print "$test is $data{$test} ($_);  $normal\n";
            print "$test is $data{$test};  $normal\n";
            exit 2;
          }
        }
      }
      print "Value ($data{$test}) not within any ranges specified ($StateData).\n";
      exit 2;
    } else {
      print "No state data found for $device:$test\n";
      exit 3;
    }
  } elsif ($test) {
    if (defined($data{$test})) {
      print "$data{$test}\n";
    } else {
      print "$test is not a valid test for $device\n";
    }
  } else {
    if (($type eq 'temperature') || ($type eq 'tsense')) {
      print "$temperature\n";
      print "0\n";
    } else {
      print "$temperature\n";
      if ($MinuteMax) {
        print "$minute\n";
      } else {
        print "$voltage\n";
      }
    }
  }
} else {
  if ($statecheck) {
    print "No sensor found named \"$device\"\n";
    exit 3;
  } else {
    print "No sensor found named \"$device\"\n";
  }
}

sub checkrange {
  # ARG1 = current value
  # ARG2 = test range
  my $value = shift;
  $_ = shift;
  #print STDERR "$_\n";
  if (m/^([^><]+?)-(.*)$/) {
    if ($value >= $1 && $value <= $2) {
      return 1;
    } else {
      return 0;
    }
  } elsif (m/^<=(.*)$/) {
    if ($value <= $1) {
      return 1;
    } else {
      return 0;
    }
  } elsif (m/^<(.*)$/) {
    if ($value < $1) {
      return 1;
    } else {
      return 0;
    }
  } elsif (m/^>=(.*)$/) {
    if ($value >= $1) {
      return 1;
    } else {
      return 0;
    }
  } elsif (m/^>(.*)$/) {
    if ($value > $1) {
      return 1;
    } else {
      return 0;
    }
  } elsif ($value = $1) {
    return 1;
  } else {
    print "Couldn't evaluate $_";
    return 0;
  }
}


sub restrict_num_decimal_digits {
  my $num=shift;
  my $digs_to_cut=shift;
  if ( ( defined($num) ) && ( $num=~/\d+\.(\d){$digs_to_cut,}/ ) ) {
    $num=sprintf("%.".($digs_to_cut)."f", $num);
  }
  return $num;
}

sub check_rrds {
  my $name = shift;
  my $resolution = 60;			# interval you want the values to have
  my $cur_time = time();

  # The following forces the end time to be a multiple of $resolution
  my $end_time = int($cur_time/$resolution) * $resolution;
  my $start_time = $end_time - 43200;	# We want the previous 12 hours data

  my $rrdfile = "$RRDsDir/" . lc($name) . ".rrd";

  return () if (! -r $rrdfile);

  my ($start, $step, $names, $data) =
	RRDs::fetch($rrdfile, "MAX", "-r", "$resolution", "-s", "$start_time", "-e", "$end_time");
  my $error=RRDs::error;
  return () if $error;

  return ($start, $step, $data);
}

sub MaxInRange {
  # ($start, $step, $data, $range);
  my $start = shift;
  my $step = shift;
  my $data = shift;
  my $range = shift;

  my $row;
  my $max = 0;
  my $timestamp = $start;
  my $cur_time = time();
  my $start_time = $cur_time - ($range * 60);

  foreach $row (@$data) {
    $timestamp += $step;
    next if ($timestamp < $start_time);
    last if ($timestamp > $cur_time);
    next if (! defined(@$row[1]));
    if (@$row[1] > $max) {
      $max = @$row[1];
    }
    #$timestamp += $step;
  }

  return $max;
}

sub age_text {
	my $time = shift;
	return '' unless ($time);
	my $duration = '';
	$duration = $time % 60 . "s" if (($time % 60) && ($time < 60));
	$time = ($time - ($time % 60)) / 60;
	$duration = $time % 60 . "m $duration" if ($time % 60);
	$time = ($time - ($time % 60)) / 60;
	$duration = $time % 24 . "h $duration" if ($time % 60);
	$time = ($time - ($time % 24)) / 24;
	$duration = $time . "d $duration" if ($time % 60);
	$duration =~ s/ $//;
	return $duration;
}
