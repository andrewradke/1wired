#!/usr/bin/perl -w

use strict;

use Config;
$Config{useithreads} or die "Recompile Perl with threads to run this program.";

use threads;
use threads::shared;

use IO::Select;
use IO::Socket::INET;
use Device::SerialPort;

use Proc::Daemon;

### Begin processing config file
my $ConfigFile = '/etc/1wired/1wired.conf';
$ConfigFile = shift if ($ARGV[0]);

my $LogFile = '/var/log/1wired/1wired.log';
my $DeviceFile = '/etc/1wired/devices';
my $PidFile = '';
my $ListenPort = 2345;
my @LinkHubs;
my @LinkTHs;
my $SleepTime = 0;
my $RunAsDaemon = 1;
my $SlowDown = 0;
my $MinutePeriod = 70;
my $FiveMinutePeriod = 310;
my $LogLevel = 5;
my $UseRRDs = 0;
my $RRDsDir = '/var/1wired';
my $AutoSearch = 1;

my ($option, $value);
open(CONFIG,  "<$ConfigFile") or die "Can't open config file ($ConfigFile): $!";
while (<CONFIG>) {
  chomp;
  s/\w*#.*//;
  next if (m/^\s*$/);
  if (m/^([A-Za-z0-9]+)\s*=\s*(.+)$/) {
    $option = $1;
    $value = $2;
    $value =~ s/\s*$//;
    if ($option eq 'LogFile') {
      $LogFile = $value;
    } elsif ($option eq 'DeviceFile') {
      $DeviceFile = $value;
    } elsif ($option eq 'PidFile') {
      $PidFile = $value;
    } elsif ($option eq 'LinkTHs') {
      @LinkTHs = split(/,\s*/, $value);
    } elsif ($option eq 'LinkHubs') {
      @LinkHubs = split(/,\s*/, $value);
    } elsif ($option eq 'ListenPort') {
      if ($value =~ m/^\d+$/) {
        if ($ListenPort <= 65535 && $ListenPort > 1024) {
          $ListenPort = $value;
        } else {
          die "Port defined in config file ($ListenPort) is not within the range 1025-65535. Exiting.\n";
        }
      } else {
        $ListenPort = $value;
        #die "Port defined in config file ($ListenPort) is not a number. Exiting.\n";		### If this isn't a UNIX system then this should fail
      }
    } elsif ($option eq 'LogLevel') {
      if ($value =~ m/^\d+$/) {
        if ($LogLevel <= 5 && $LogLevel >= 0) {
          $LogLevel = $value;
        } else {
          print STDERR "LogLevel defined in config file ($value) is not within the range 0-5. Using default ($LogLevel).\n";
        }
      } else {
        print STDERR "LogLevel defined in config file ($value) is not a number. Using default ($LogLevel).\n";
      }
    } elsif ($option eq 'SlowDown') {
      if ($value =~ m/^\d+$/) {
        $SlowDown = $value;
      } else {
        print STDERR "SlowDown value defined in config file ($value) is not a number. Using default ($SlowDown).\n";
      }
    } elsif ($option eq 'AutoSearch') {
      if ($value =~ m/^(1|0|true|false|yes|no)$/i) {
        $AutoSearch = 0 if ($value =~ m/^(0|false|no)$/i);
      } else {
        print STDERR "AutoSearch value defined in config file ($value) is not valid. Using default ($AutoSearch).\n";
      }
    } elsif ($option eq 'SleepTime') {
      if ($value =~ m/^\d+(\.\d+|)$/) {
        if (($value <= 2) && ($value >= 0)) {
          $SleepTime = $value;
        } else {
          print STDERR "SleepTime defined in config file ($value) is not within the range 0-2. Using default ($SleepTime).\n";
        }
      } else {
        print STDERR "SleepTime defined in config file ($value) is not a number. Using default ($SleepTime).\n";
      }
    } elsif ($option eq 'UseRRDs') {
      if ($value =~ m/^(1|0|true|false|yes|no)$/i) {
        $UseRRDs = 1 if ($value =~ m/^(1|true|yes)$/i);
      } else {
        print STDERR "UseRRDs value defined in config file ($value) is not valid. Using default ($UseRRDs).\n";
      }
    } elsif ($option eq 'RRDsDir') {
      $RRDsDir = $value;
    } elsif ($option eq 'RunAsDaemon') {
      if ($value =~ m/^(1|0|true|false|yes|no)$/i) {
        $RunAsDaemon = 0 if ($value =~ m/^(0|false|no)$/i);
      } else {
        print STDERR "RunAsDaemon value defined in config file ($value) is not valid. Using default ($RunAsDaemon).\n";
      }
    } elsif ($option eq 'StateFile') {
      # Option not used by 1wired but possibly by other programs
    } else {
      print STDERR "Unknown option in config file: \"$option\"\n";
    }
  } else {
    print STDERR "Unrecognised line in config file: \"$_\"\n";
  }
}
close(CONFIG);
$option = undef;
$value = undef;
### End processing config file

if ($UseRRDs) {
  use RRDs;
  die "Can't write to RRD dir ($RRDsDir)" unless (-w $RRDsDir);
}

### Begin deamon setup
Proc::Daemon::Init if ($RunAsDaemon);
### End deamon setup

### Begin setting up logging
print STDERR "LogLevel has been set to 0. NO LOGGING WILL OCCUR!\n" if (! $LogLevel);
if ($LogLevel && $RunAsDaemon) {
  open(LOG,  ">>$LogFile") or die "Can't open log file ($LogFile): $!";
  my $oldfh = select LOG;
  $|=1;
  select($oldfh);
  $oldfh = undef;
}
### End setting up logging


### Begin defining logmsg sub
$0 =~ s/.*\///;		# strip path from script name
my $script = $0;
sub logmsg {
  my $level = shift;
  if ($level <= $LogLevel) {
    if ($RunAsDaemon) {
      print LOG scalar localtime, " $0\[$$\]: @_\n";
    } else {
      print scalar localtime, " $0\[$$\]: @_\n";
    }
  }
}
### End defining logmsg sub

### define variables shared by all threads
my %data : shared;
my %LinkDevData : shared;
my %deviceDB : shared;
my %addresses : shared;
my %agedata : shared;

### define non-shared variables
my $tmp;
my @tmp;


### Begin parsing device file
ParseDeviceFile();
### End parsing device file


### Beginning of monitoring threads
my %threads;
foreach my $LinkDev (@LinkHubs) {
  $LinkDevData{$LinkDev} = &share( {} );
  $addresses{$LinkDev} = &share( [] );
  $threads{$LinkDev} = threads->new(\&monitor_linkhub, $LinkDev);
}
foreach my $LinkDev (@LinkTHs) {
  $LinkDevData{$LinkDev} = &share( {} );
  $addresses{$LinkDev} = &share( [] );
  $threads{$LinkDev} = threads->new(\&monitor_linkth, $LinkDev);
}
#$threads{agedata} = threads->new(\&monitor_agedata);
### End of monitoring thread


### Beginning of RRD recording
if ($UseRRDs) {
  my $RRDthread = threads->new(\&RecordRRDs);
}
### End of RRD recording

$SIG{'HUP'} = sub {
  logmsg(1, "Re-reading device file.");
  ParseDeviceFile();
  foreach my $LinkDev (@LinkHubs) {
    $LinkDevData{$LinkDev}{SearchNow} = 1;
  }
};


### Beginning of listener
### Nothing after this will run since this is a continual loop
my $server_sock;
if ($ListenPort =~ m/^\d+$/) {
  logmsg 1, "Creating listener on port $ListenPort";
  $server_sock = new IO::Socket::INET (
					LocalPort => $ListenPort,
					Proto    => 'tcp',
					Listen   => 5,
					);
  die "Cannot create socket on port $ListenPort: $!" unless $server_sock;
} else {
  logmsg 1, "Creating listener on socket $ListenPort";
  unlink "$ListenPort";
  $server_sock = IO::Socket::UNIX->new(
					Local   => "$ListenPort",
					Type   => SOCK_STREAM,
					Listen => 5,
					);
  die "Cannot create socket on $ListenPort: $!" unless $server_sock;
}

logmsg 1, "Listening on socket $ListenPort";

if ($PidFile) {
  if (open(PID,  ">$PidFile")) {
    print PID $$;
    close PID;
  } else {
    logmsg 1, "Can't open pid file ($PidFile): $!";
  }
}

my $listener;

my $client;
while (1) {	# This is needed to restart the listening loop after a sig hup
  while ($client = $server_sock->accept()) {
    logmsg 5, "Connection on socket $ListenPort";
    $listener = threads->new(\&report, $client);
    $listener->detach;
    close ($client);			# This socket is handled by the new thread
    $client=undef;
  }
}
logmsg 1, "Closing socket $ListenPort";
close ($server_sock);
if (! ($ListenPort =~ m/^\d+$/)) {
  logmsg(1, "Removing socket on $ListenPort");
  unlink "$ListenPort";
}
### End of listener


### clean up all threads
foreach my $thread (threads->list) {
  if ($thread->tid && !threads::equal($thread, threads->self)) {
    #$thread->exit();
    $thread->join;
  }
}

##################
### END OF PROGRAM
##################

sub report {
  my $socket = shift;

  my $select = IO::Select->new($socket);
  my $command;

  $socket->send("220 1wired\n");
  if ($select->can_read(1)) {
    $socket->recv($command,128);
  }
  if ($command) {
    $command =~ s/[\r\n]//g;
    $command = lc($command);
    if ($command eq 'list') {
      $socket->send(list());
    } elsif ($command eq 'listdb') {
      $socket->send(listdb());
    } elsif ($command eq 'value all') {
      $socket->send(value_all());
    } elsif ($command =~ m/^value (.*)/) {
      $socket->send(value($1));
    } elsif ($command eq 'help') {
      $socket->send(helpmsg());
    } else {
      $socket->send("UNKNOWN command: $command\n");
      $socket->send(helpmsg());
    }
  }
  close ($socket);
  $socket=undef;
  $select=undef;
  # If the thread isn't detached then the following will cause a memory leak.
  # If threads->exit isn't used then perl will not remove it from memory and
  # that will also cause a memory leak.
  # If this causes a Perl error then you may need to install libthreads-perl
  # or upgrade to a version of perl > 5.8.8
  threads->exit();
}

sub monitor_linkhub {
  our $LinkDev = shift;
  logmsg 1, "Monitoring LinkHub $LinkDev";
  our $LinkType = 'LinkHubE';
  if ($LinkDev =~ m/^\/dev\//) {
    $LinkType = 'LinkSerial'
  };

  my @addresses;
  my $count = 0;

  my $returned;
  our $socket;
  our $select;

  my ($temperature, $voltage);
  my $address;
  my ($type, $name);

  my $DoSearch = 1;
  my $LastDev;

  while(1) {
    $count++;
    $agedata{$LinkDev} = time();

    $socket = LinkConnect($LinkDev) if (! $socket);
    if (! $socket) {			# Failed to connect
      sleep 10;				# Wait for 10 seconds before retrying
      next;
    }

    $select = IO::Select->new($socket) if ($LinkType eq 'LinkHubE');

### Begin search for devices on LinkHub
    if ( ($LinkDevData{$LinkDev}{SearchNow}) || (($DoSearch) || (($AutoSearch) && ($count == 1))) ) {
      $LinkDevData{$LinkDev}{ds1820} = 0;
      $LastDev = 0;
      logmsg 3, "Searching for devices on $LinkDev";
      @addresses = ();

      $returned = LinkData("\n");		# Clear any initial state
      next if (! defined($socket));
      $returned = LinkData("f\n");		# request first device ID
      next if (! defined($socket));

      $returned =~ s/[-+,]//gs;
      if ($returned eq 'N') {			# no devices found so we'll start again and keep trying until something is found
        logmsg 4, "No devices found on $LinkDev, closing socket.";
        next;
      }

      # We have found at least one device so we can turn of the need to do more searches as required
      $DoSearch = 0 if (! $AutoSearch);
      $LinkDevData{$LinkDev}{SearchNow} = 0;

      if ($returned =~ m/^.$/) {		# Error searching but we'll keep searching in case there are more devices
        logmsg 1, "First device search on $LinkDev returned '$returned'";
      } elsif ($returned =~ s/(..)(..)(..)(..)(..)(..)(..)(..)/$8$7$6$5$4$3$2$1/) {
        if (! CRC($returned)) {
          logmsg 1, "CRC FAILED for device ID $returned";
        } else {
          if (! defined($data{$returned})) {
            $data{$returned} = &share( {} );
          }
          if ( (defined($data{$returned}{name})) && ($data{$returned}{name} eq 'ignore') ) {
            logmsg 1, "Ignoring $returned on $LinkDev.";
          } else {
            push (@addresses, $returned);
          }
          $data{$returned}{linkdev} = $LinkDev;
          $data{$returned}{type} = 'voltage' if (! defined($data{$returned}{type}));
          $data{$returned}{name} = '' if (! defined($data{$returned}{name}));
          if ( ($returned =~ m/^28/) && ($data{$returned}{type} ne 'tsense') ) {
            # DS18B20
            logmsg 3, "Setting device $returned type to 'tsense'";
            $data{$returned}{type} = 'tsense';
          }
          if ( ($returned =~ m/^10/) && ($data{$returned}{type} ne 'ds1820') ) {
            # DS1820 or DS18S20
            logmsg 3, "Setting device $returned type to 'ds1820'";
            $data{$returned}{type} = 'ds1820';
            $LinkDevData{$LinkDev}{ds1820} = 1;
          }
          logmsg 4, "Found $returned ($data{$returned}{name}) on $LinkDev";
        }
      } else {
        logmsg 1, "Bad data returned on search of $LinkDev: $returned";
      }
      while (!($LastDev)) {
        $returned = LinkData("n\n");			# request next device ID
        next if (! defined($socket));
        if ($returned =~ m/^.$/) {		# Error searching so we'll just move on in case there are more devices
          logmsg 1, "Device search on $LinkDev returned '$returned'";
          next;
        }
        $LastDev = 1 if (!($returned =~ m/^\+,/));	# This is the last device
        $returned =~ s/[-+,]//gs;
        if ($returned =~ s/(..)(..)(..)(..)(..)(..)(..)(..)/$8$7$6$5$4$3$2$1/) {
          if (! CRC($returned)) {
            logmsg 1, "CRC FAILED for device ID $returned";
            next;
          }
          next if ($8 eq '01');				# ignore LinkHubEs
          if (! defined($data{$returned})) {
            $data{$returned} = &share( {} );
          }
          if ( (defined($data{$returned}{name})) && ($data{$returned}{name} eq 'ignore') ) {
            logmsg 1, "Ignoring $returned on $LinkDev.";
          } else {
            push (@addresses, $returned);
          }
          $data{$returned}{linkdev} = $LinkDev;
          $data{$returned}{type} = 'voltage' if (! defined($data{$returned}{type}));
          $data{$returned}{name} = '' if (! defined($data{$returned}{name}));
          if ( ($returned =~ m/^28/) && ($data{$returned}{type} ne 'tsense') ) {
            logmsg 3, "Setting device $returned type to 'tsense'";
            $data{$returned}{type} = 'tsense';
          }
          if ( ($returned =~ m/^10/) && ($data{$returned}{type} ne 'ds1820') ) {
            logmsg 3, "Setting device $returned type to 'ds1820'";
            $data{$returned}{type} = 'ds1820';
            $LinkDevData{$LinkDev}{ds1820} = 1;
          }
          logmsg 4, "Found $returned ($data{$returned}{name}) on $LinkDev";
        } else {
          logmsg 1, "Bad data returned on search of $LinkDev: $returned";
        }
      }
      logmsg 5, "Found last device on $LinkDev.";

      (@{$addresses{$LinkDev}}) = @addresses;
      $LinkDevData{$LinkDev}{SearchTime} = time();
    }
### End search for devices on LinkHub

### Begin addressing ALL devices
    $returned = LinkData("\n");			# Clear any initial state
    next if (! defined($socket));
    $returned = LinkData("r\n");		# issue a 1-wire reset
    next if (! defined($socket));
    logmsg 1, "Reset on $LinkDev returned '$returned'" if ($returned ne 'P');
    $returned = LinkData("pCC44\n");		# byte mode in pull-up mode, skip rom (address all devices), convert T
    next if (! defined($socket));
    sleep 0.1;					# wait 100ms for temperature conversion
    $returned = LinkData("\n");			# Clear any initial state
    next if (! defined($socket));
    $returned = LinkData("r\n");		# issue a 1-wire reset
    next if (! defined($socket));
    logmsg 1, "Reset on $LinkDev returned '$returned'" if ($returned ne 'P');
    $returned = LinkData("bCCB4\n");		# byte mode, skip rom (address all devices), convert V
    next if (! defined($socket));
    sleep 0.01;					# wait 10ms for voltage conversion
### End addressing ALL devices

### Begin query of devices on LinkHub
    foreach $address (@addresses) {
      last if (! defined($socket));
      next if ( (! defined($data{$address})) || (! defined($data{$address}{linkdev})) );
      if ($data{$address}{linkdev} eq $LinkDev) {
        if (! $data{$address}{type}) {
          $data{$address}{type} = 'voltage';
        }
        $type = $data{$address}{type};
        if (! $data{$address}{name}) {
          $data{$address}{name} = 'UNKNOWN';
        }
        $name = $data{$address}{name};

        logmsg 5, "querying $name ($address) as $type";
        $name = $address if ($name eq 'UNKNOWN');

        $returned = query_device($socket,$select,$address,$LinkDev);
        my $retry = 0;
        while ($returned eq 'ERROR') {
          $retry++;
          if ($retry > 5) {
            logmsg 1, "Couldn't get valid data for $LinkDev:$name";
            last;
          }
          logmsg 3, "Didn't get valid data for $LinkDev:$name, retrying... (attempt $retry)";
          $returned = query_device($socket,$select,$address,$LinkDev);
        }

        if ($returned ne 'ERROR') {
          if (! CRC($returned) ) {
            logmsg 1, "CRC error for $LinkDev:$name: $returned";
            next;
          }
          $temperature = $returned;
          $voltage = $returned;
          $temperature =~ s/^(....).*$/$1/;
          $voltage =~ s/^....(....).*$/$1/;

          #e.g.  a return value of 5701 represents 0x0157, or 343 in decimal.
          if ( $data{$address}{type} eq 'ds1820') {
            $temperature =~ m/^(..)(..)$/;
            $temperature = hex $1;
            $temperature = $temperature - 256 if ( $2 eq 'FF' );
            $temperature = $temperature/2;
          } elsif ( $data{$address}{type} eq 'tsense') {
            $temperature =~ s/^(..)(..)$/$2$1/;
            $temperature = hex $temperature;
            $temperature = $temperature/16;
            $temperature = restrict_num_decimal_digits($temperature,1);
          } else {
            $temperature =~ s/^(..)(..)$/$2$1/;
            $temperature = hex $temperature;
            $temperature = $temperature>>3;
            $temperature = $temperature*0.03125;
            $temperature = restrict_num_decimal_digits($temperature,1);
          }

          if (! defined($data{$address}{temperature})) {
            if ( $temperature == 85 ) {
              ### If the temperature is 85C it is probably a default value and should be ignored
              logmsg 2, "(query) Initial temperature ($temperature) for $address is probably not valid (85C is a default): discarding readings.";
              next;
            } else {
              $data{$address}{temperature} = $temperature;
            }
          } elsif ( ($temperature > ($data{$address}{temperature} + 10)) || ($temperature < ($data{$address}{temperature} - 10)) ) {
            ### If the temperature is more than 10 above or below the previous recorded value it is not correct and the voltage will also be wrong
            logmsg 2, "(query) Spurious temperature ($temperature) for $address: keeping previous data ($data{$address}{temperature})";
            next;
          }
          $data{$address}{temperature} = $temperature;

          $voltage =~ s/^(..)(..)$/$2$1/;
          $voltage = hex $voltage;
          $voltage = 0 if ($voltage == 1023);	# 1023 inidicates a short or 0V
          $voltage = $voltage*0.01;             # each unit is 10mV
          $data{$address}{raw} = $voltage;
          logmsg 5, "Raw voltage on $name ($LinkDev:$address) is $voltage" unless ( ($type eq 'temperature') || ($type eq 'tsense') || ($type eq 'ds1820') );
          if ($type eq 'current') {
            # convert voltage to current
            # At 23 degrees C, it is linear from .2 VDC (1 amp.) to 3.78 VDC (20 amps.).
            # The zero current reading is .09 VDC.
            if ($voltage == 0.09) {
              $voltage = 0;
            } else {
              $voltage = ( ($voltage-0.2) / ( (3.78-0.2) / 19) ) + 1;
            }
          }
          if ($type eq 'humidity') {
            # convert voltage to humidity
            # as per the formula from the Honeywell 3610-001 data sheet
            $voltage = ($voltage - 0.958)/0.0307;
          }
          if ($type eq 'light') {
            # if the reading is over 5V then it is actually < 0V and indicates darkness
            $voltage = 0 if ($voltage > 5);
            # double the voltage reading for light to give a range from 0-10
            $voltage = $voltage * 2;
          }
          if ($type eq 'depth15') {
            # 266.67 mV/psi; 0.5V ~= 0psi
            #$voltage = ($voltage - 0.5) * 3.75;
            $voltage = ($voltage - 0.43) * 3.891;
            # 1.417psi/metre
            $voltage = $voltage / 1.417;
          }
          if ($type eq 'pressure150') {
            $voltage = undef if ($voltage > 5);
            # 26.67 mV/psi would be 150psi over 4V; therefore * 37.5
            $voltage = ($voltage - 0.5) * 37.5;
          }
          if ($type eq 'pressure') {
            # 25.7 mV/psi; 0.43V ~= 0psi
            $voltage = ($voltage - 0.43) * 38.91;
          }
          if ( ($type eq 'temperature') || ($type eq 'tsense') || ($type eq 'ds1820') ) {
            $voltage = $temperature;
          }
          $voltage = restrict_num_decimal_digits($voltage,2);
          $data{$address}{$type} = $voltage;
          if (! defined($data{$address}{minute})) {
            $data{$address}{minute} = $voltage;
            $data{$address}{time} = time();
          } elsif ( ($voltage > $data{$address}{minute}) || ((time() - $data{$address}{time}) >= $MinutePeriod ) ) {
            $data{$address}{minute} = $voltage;
            $data{$address}{time} = time();
          }
          if (! defined($data{$address}{FiveMinute})) {
            $data{$address}{FiveMinute} = $voltage;
            $data{$address}{FiveTime} = time();
          } elsif ( ($voltage > $data{$address}{FiveMinute}) || ((time() - $data{$address}{FiveTime}) >= $FiveMinutePeriod ) ) {
            $data{$address}{FiveMinute} = $voltage;
            $data{$address}{FiveTime} = time();
          }
          $data{$address}{age} = time();
        }
      }
    }
    logmsg 5, "Finished querying devices on $LinkDev.";
### End query of devices on LinkHub

    $count = 0 if ($count > 1);
    #logmsg 5, "Finished loop for $LinkDev, closing socket.";
    #$socket->close;
    #$socket = undef;
    sleep $SlowDown if ($SlowDown);	# ***** This is only to slow down the rate of queries
  }
}

sub monitor_linkth {
  my $LinkDev = shift;
  logmsg 1, "Monitoring LinkTH $LinkDev";
  our $LinkType = 'LinkSerial';

  my @addresses;

  my $returned;
  my $socket;

  my ($temperature, $voltage);
  my $address;
  my ($type, $name);

  $socket = LinkConnect($LinkDev) if (! $socket);
  next if (! $socket);				# Failed to connect

  $socket->write("\n");				# Clear any initial state
  sleep $SleepTime;
  ($tmp,$returned) = $socket->read(255);	# Discard any initial unrequested data

  while (1) {
    $socket->write("D");			# Request ALL LinkTH data
    sleep $SleepTime;
    ($tmp,$returned) = $socket->read(1023);	# Get reply
    my $retry = 0;
    while ($tmp == 0 ) {			# No data in reply if $tmp == 0
      $retry++;
      if ($retry > 10) {
        logmsg 1, "Didn't get any data from $LinkDev after 10 retries. Starting again.";
        last;
      }
      logmsg 3, "Didn't get any data from $LinkDev, retrying... (attempt $retry)";
      sleep ($SleepTime + ($retry * 0.1));
      ($tmp,$returned) = $socket->read(1023);	# Get reply
    }
    next if ($retry > 10);			# Too many retries, start again.
    next if (! ($returned =~ m/EOD/));		# If there is no EOD then we haven't got all the devices, start again.

    $agedata{$LinkDev} = time();
    @addresses = ();

    foreach (split(/\r?\n/, $returned)) {
      if ($_ eq 'EOD') {
        $tmp = 'EOD';				# Record that we have reached EOD
        last;
      }
      @tmp = split(/,/, $_);			# $address $type,$temperatureC,$temperatureF,$???,$voltage[,$timestamp]
      if ($tmp[0] =~ s/ ([0-9A-F]{2})$//) {
        $address = $tmp[0];
        $type = $1;
        $temperature = $tmp[1];
        $tmp[4] = 0 if (! $tmp[4]);
        $voltage = $tmp[4];

        push (@addresses, $address);
        if (! defined($data{$address})) {
          $data{$address} = &share( {} );
        }
        $data{$address}{linkdev} = $LinkDev;
        $data{$address}{type} = 'voltage' if (! defined($data{$address}{type}));
        $data{$address}{name} = '' if (! defined($data{$address}{name}));
        if ( ($address =~ m/^28/) && ($data{$address}{type} ne 'tsense') ) {
          logmsg 3, "Setting device $address type to 'tsense'";
          $data{$address}{type} = 'tsense';
        }
        logmsg 4, "Found $address ($data{$address}{name}) on $LinkDev";

        $name = $data{$address}{name};
        #$type = $data{$address}{type};

        $voltage = 0 if ($voltage == 1023);		# 1023 inidicates a short or 0V
        $voltage = $voltage*0.01;			# each unit is 10mV
        $data{$address}{raw} = $voltage;
        logmsg 5, "Raw voltage on $name ($LinkDev:$address) is $voltage" unless ( ($type eq 'temperature') || ($type eq 'tsense') || ($type eq 'ds1820') );
        if ($data{$address}{type} eq 'current') {
          # convert voltage to current
          # At 23 degrees C, it is linear from .2 VDC (1 amp.) to 3.78 VDC (20 amps.).
          # The zero current reading is .09 VDC.
          if ($voltage == 0.09) {
            $voltage = 0;
          } else {
            $voltage = ( ($voltage-0.2) / ( (3.78-0.2) / 19) ) + 1;
          }
        }
        if ($data{$address}{type} eq 'humidity') {
          if ($type ne '19') {
            logmsg (3,"$name ($LinkDev:$address) is configured for humidity but type reported as $type");
            # convert voltage to humidity
            # as per the formula from the Honeywell 3610-001 data sheet
            $voltage = ($voltage - 0.958)/0.0307;
          }
        }
        if ($data{$address}{type} eq 'light') {
          if ($type ne '1B') {
            logmsg (3,"$name ($LinkDev:$address) is configured for light but type reported as $type");
          }
          # if the reading is over 5V then it is actually < 0V and indicates darkness
          $voltage = 0 if ($voltage > 5);
          # double the voltage reading for light to give a range from 0-10
          $voltage = $voltage * 2;
        }
        if ($data{$address}{type} eq 'depth15') {
          # 266.67 mV/psi; 0.5V ~= 0psi
          #$voltage = ($voltage - 0.5) * 3.75;
          $voltage = ($voltage - 0.43) * 3.891;
          # 1.417psi/metre
          $voltage = $voltage / 1.417;
        }
        if ($type eq 'pressure150') {
          # 26.67 mV/psi would be 150psi over 4V; therefore * 37.5
          $voltage = ($voltage - 0.5) * 37.5;
        }
        if ($data{$address}{type} eq 'pressure') {
          # 26.67 mV/psi would be 150psi over 4V; therefore * 37.5
          # 25.7 mV/psi; 0.43V ~= 0psi
          $voltage = ($voltage - 0.43) * 38.91;
        }
        if ( ($data{$address}{type} eq 'temperature') || ($data{$address}{type} eq 'tsense') || ($data{$address}{type} eq 'ds1820') ) {
          $voltage = $temperature;
        }
        $temperature = restrict_num_decimal_digits($temperature,1);

        if (! defined($data{$address}{temperature})) {
          if ( $temperature == 85 ) {
            ### If the temperature is 85C it is probably a default value and should be ignored
            logmsg 2, "(query) Initial temperature ($temperature) for $address is probably not valid (85C is a default): discarding readings.";
            next;
          } else {
            $data{$address}{temperature} = $temperature;
          }
        } elsif ( ($temperature > ($data{$address}{temperature} + 10)) || ($temperature < ($data{$address}{temperature} - 10)) ) {
          ### If the temperature is more than 10 above or below the previous recorded value it is not correct and the voltage will also be wrong
          logmsg 2, "(query) Spurious temperature ($temperature) for $address: keeping previous data ($data{$address}{temperature})";
          next;
        }
        $data{$address}{temperature} = $temperature;

        $voltage = restrict_num_decimal_digits($voltage,2);
        $data{$address}{$data{$address}{type}} = $voltage;
        if (! defined($data{$address}{minute})) {
          $data{$address}{minute} = $voltage;
          $data{$address}{time} = time();
        } elsif ( ($voltage > $data{$address}{minute}) || ((time() - $data{$address}{time}) >= $MinutePeriod ) ) {
          $data{$address}{minute} = $voltage;
          $data{$address}{time} = time();
        }
        if (! defined($data{$address}{FiveMinute})) {
          $data{$address}{FiveMinute} = $voltage;
          $data{$address}{FiveTime} = time();
        } elsif ( ($voltage > $data{$address}{FiveMinute}) || ((time() - $data{$address}{FiveTime}) >= $FiveMinutePeriod ) ) {
          $data{$address}{FiveMinute} = $voltage;
          $data{$address}{FiveTime} = time();
        }
        $data{$address}{age} = time();
      }
    }
    (@{$addresses{$LinkDev}}) = @addresses;
    $LinkDevData{$LinkDev}{SearchTime} = time();
  }
  #$socket->close;
  #$socket = undef;
}

sub query_device {
  my $socket  = shift;
  my $select  = shift;
  my $address = shift;
  my $LinkDev = shift;

  my $returned;

  eval {
    if ( ( $data{$address}{type} eq 'tsense') || ( $data{$address}{type} eq 'ds1820') ) {

      if ( $data{$address}{type} eq 'ds1820') {
        # Original ds1820 needs the bus pulled higher for longer for parasitic power
        # It can also loose the data after a Skip ROM so we address them inidividually here
        $returned = LinkData("\n");			# Clear any initial state
        return 'ERROR' if (! defined($socket));
        $returned = LinkData("r\n");			# issue a 1-wire reset
        return 'ERROR' if (! defined($socket));
        logmsg 1, "Reset on $LinkDev returned '$returned'" if ($returned ne 'P');
        $returned = LinkData("p55${address}44\n");	# byte mode in pull-up mode, ROM 0x55, address, convert T
        return 'ERROR' if (! defined($socket));
        sleep 1;					# Give it time to convert T
      }

      $returned = LinkData("\n");			# Clear any initial state
      return 'ERROR' if (! defined($socket));
      $returned = LinkData("r\n");			# issue a 1-wire reset
      return 'ERROR' if (! defined($socket));
      logmsg 1, "Reset on $LinkDev returned '$returned'" if ($returned ne 'P');
  
      # BEFFFFFFFFFFFFFFFFFF
      # BExxyyiijjkkllmmnnoo
      # xx: LSB for temperature
      # yy: MSB for temperature
      # ii TH Register or User Byte 1
      # jj TL Register or User Byte 2
      # kk Reserved (FFh)
      # ll Reserved (FFh)
      # mm COUNT REMAIN (0Ch)
      # nn COUNT PER °C (10h)
      # oo CRC

      $returned = LinkData("b55${address}BEFFFFFFFFFFFFFFFFFF\n");	# byte mode, ROM 0x55, address, read command BE, 9 bytes FF
      return 'ERROR' if (! defined($socket));
      if ( (length($returned) != 38) || (! $returned =~ m/^55${address}BE[A-Z0-9]{18}$/) ) {
        logmsg 3, "ERROR: Sent b55${address}BEFFFFFFFFFFFFFFFFFF command; got: $returned";
        return 'ERROR';
      }
      if ( $returned =~ m/^55${address}BEF{18}$/ ) {
        logmsg 4, "ERROR: Sent b55${address}BEFFFFFFFFFFFFFFFFFF command; got: $returned";
        return 'ERROR';
      }
      if ($returned =~ s/^55${address}BE//) {
        return $returned;
      } else {
        logmsg 2, "ERROR: returned data not valid for $address: $returned";
        return 'ERROR';
      }
    } else {
      $returned = LinkData("\n");			# Clear any initial state
      return 'ERROR' if (! defined($socket));
      $returned = LinkData("r\n");			# issue a 1-wire reset
      return 'ERROR' if (! defined($socket));
      logmsg 1, "Reset on $LinkDev returned '$returned'" if ($returned ne 'P');
      $returned = LinkData("b55${address}B800\n");	# byte mode, ROM 0x55, address, Recall Memory page 00 to scratch pad
      return 'ERROR' if (! defined($socket));
      if ($returned ne "55${address}B800") {
        logmsg 3, "ERROR: Sent b55${address}B800 command; got: $returned";
        return 'ERROR';
      }
  
      $returned = LinkData("\n");			# Clear any initial state
      return 'ERROR' if (! defined($socket));
      $returned = LinkData("r\n");			# issue a 1-wire reset
      return 'ERROR' if (! defined($socket));
      logmsg 1, "Reset on $LinkDev returned '$returned'" if ($returned ne 'P');
  
      # BE00FFFFFFFFFFFFFFFFFF
      # BE00xxyyzzaabbccddeeff
      # xx: status register
      # yy: LSB for temperature
      # zz: MSB for temperature
      # aa: LSB for voltage
      # bb: MSB for voltage
      # ff: CRC

      $returned = LinkData("b55${address}BE00FFFFFFFFFFFFFFFFFF\n");	# byte mode, ROM 0x55, address, read scratch pad for memory page 00
      return 'ERROR' if (! defined($socket));
      if ( (length($returned) != 40) || (! $returned =~ m/^55${address}BE0000[A-Z0-9]{18}$/) ) {
        logmsg 3, "ERROR: Sent b55${address}BE00FFFFFFFFFFFFFFFFFF command; got: $returned";
        return 'ERROR';
      }
      if ( $returned =~ m/^55${address}BE00F{18}$/ ) {
        logmsg 4, "ERROR: Sent b55${address}BE00FFFFFFFFFFFFFFFFFF command; got: $returned";
        return 'ERROR';
      }
      if ($returned =~ s/^55${address}BE00[0-9A-F]{2}//) {
        return $returned;
      } else {
        logmsg 2, "ERROR: returned data not valid for $address: $returned";
        return 'ERROR';
      }
    }
  } or do {
    logmsg 2, "ERROR: getting data from $LinkDev (Last data: $returned)";
    return 'ERROR';
  };
}

sub restrict_num_decimal_digits {
  my $num=shift;
  my $digs_to_cut=shift;
  if ($num=~/\d+\.(\d){$digs_to_cut,}/) {
    $num=sprintf("%.".($digs_to_cut)."f", $num);
  }
  return $num;
}

sub list {
  my $output = '';
  my @addresses;
  foreach my $LinkDev (keys(%addresses)) {
    (@addresses) = (@addresses, @{$addresses{$LinkDev}});
  }

  foreach (@addresses) {
    s/[\r\n\0]//g;		# Remove any CR, LF or NULL characters first
    s/^[?!,]*//;		# THEN remove appropriate any leading characters
    if ((defined($deviceDB{$_})) && (defined($deviceDB{$_}{name})) ) {
      $output .= "Device found:\t $deviceDB{$_}{name} ($_)\n";
    } else {
      $output .= "UNKNOWN device:\t $_\n";
    }
  }
  foreach $tmp (keys(%deviceDB)) {
    if (! grep $_ eq $tmp, @addresses) {
      $output .= "NOT RESPONDING:\t $deviceDB{$tmp}{name} ($tmp)\n";
    }
  }
  return $output;
}

sub listdb {
  my $output = '';
  foreach (keys(%deviceDB)) {
    $output .= "$_\t$deviceDB{$_}{name}\t$deviceDB{$_}{type}\n";
  }
  return $output;
}

sub value_all {
  my $output = '';
  my %OutputData;
  my @addresses;
  my $LinkDev;
  foreach $LinkDev (keys(%addresses)) {
    (@addresses) = (@addresses, @{$addresses{$LinkDev}});
  }

  my ($address, $name, $temperature, $type, $voltage, $age, $linkdev);
  foreach $address (@addresses) {
    eval {
      $name        = $data{$address}{name};
      $temperature = $data{$address}{temperature};
      $type        = $data{$address}{type};
      $voltage     = $data{$address}{$type};
      $linkdev     = $data{$address}{linkdev};
      if (defined($data{$address}{age})) {
        $age       = time - $data{$address}{age};
      } else {
        $age       = '0';
      }
      $temperature = 'NA' unless defined($temperature);
      $voltage     = 'NA' unless defined($voltage);
      $name        = "* $address" if ($name eq 'UNKNOWN');
      $type        =~ s/^pressure[0-9]+$/pressure/;
      $type        =~ s/^depth[0-9]+$/depth/;
      if ( ($type eq 'temperature') || ($type eq 'tsense') || ($type eq 'ds1820') ) {
        $OutputData{$name} = sprintf "%-18s - temperature: %5s                      (age: %3d s)\t%s\n", $name, $temperature, $age, $linkdev;
      } else {
        $OutputData{$name} = sprintf "%-18s - temperature: %5s - %10s: %5s  (age: %3d s)\t%s\n", $name, $temperature, $type, $voltage, $age, $linkdev;
      }
    }
  }
  foreach $name (sort(keys(%OutputData))) {
    $output .= $OutputData{$name};
  }

  $output .= "-------------------------------------------------------------------------------\n";
  foreach $LinkDev (keys(%addresses)) {
    if (defined($LinkDevData{$LinkDev}{SearchTime})) {
      $age       = time - $LinkDevData{$LinkDev}{SearchTime};
    } else {
      $age       = '0';
    }
    $output .= "$LinkDev last searched $age seconds ago\n";
  }
  return $output;
}

sub value {
  my $output = '';
  my $search = shift;

  my @addresses;
  foreach my $LinkDev (keys(%addresses)) {
    (@addresses) = (@addresses, @{$addresses{$LinkDev}});
  }

  my ($address, $name, $temperature, $type, $voltage, $minute, $FiveMinute, $time, $FiveTime, $age, $raw, $linkdev);
  foreach $address (@addresses) {
    $name          = $data{$address}{name};
    if (($search eq lc($name)) || ($search eq lc($address))) {
      $temperature = $data{$address}{temperature};
      $type        = $data{$address}{type};
      $voltage     = $data{$address}{$type};
      $linkdev     = $data{$address}{linkdev};
      $minute      = $data{$address}{minute};
      $FiveMinute  = $data{$address}{FiveMinute};
      if ($data{$address}{time}) {
        $time      = time - $data{$address}{time};
      } else {
        $time      = 'NA';
      }
      if ($data{$address}{FiveTime}) {
        $FiveTime  = time - $data{$address}{FiveTime};
      } else {
        $FiveTime  = 'NA';
      }
      if ($data{$address}{age}) {
        $age       = time - $data{$address}{age};
      } else {
        $age       = 'NA';
      }
      $raw         = $data{$address}{raw};
      $temperature = 'NA' unless defined($temperature);
      $voltage     = 'NA' unless defined($voltage);
      $minute      = 'NA' unless defined($minute);
      $FiveMinute  = 'NA' unless defined($FiveMinute);
      $raw         = 'NA' unless defined($raw);
      $type        =~ s/^pressure[0-9]+$/pressure/;
      $type        =~ s/^depth[0-9]+$/depth/;
      if ( ($type eq 'temperature') || ($type eq 'tsense') || ($type eq 'ds1820') ) {
        $output .= "name: $name\naddress: $address\ntype: $type\ntemperature: $temperature\nlinkdev: $linkdev\n";
      } else {
        $output .= "name: $name\naddress: $address\ntype: $type\ntemperature: $temperature\n$type: $voltage\n1MinuteMax: $minute\n5MinuteMax: $FiveMinute\nupdated: $time\nage: $age\nRawVoltage: $raw\nlinkdev: $linkdev\n";
      }
      return $output;
    }
  }
  foreach $address (keys(%deviceDB)) {
    $name = $deviceDB{$address}{name};
    if ($search eq lc($name)) {
      $output .= "Sensor $name not responding\n";
      return $output;
    }
  }
  $output .= "Sensor $search not found\n";
  return $output;
}

sub helpmsg {
  my $output = '';
  $output .= << "EOF"
Valid commands:
list         : lists all devices including unkown and those not responding.
listdb       : lists all devices in the database along with their type.
value all    : returns the current values from all sensors.
value <name> : return all data for sensor <name>.
help         : this message.
EOF
;
  return $output;
}


sub RecordRRDs {
  my @addresses;
  my ($address, $name, $temperature, $minute, $age);
  my ($rrdfile, $rrderror, $updatetime);
  while(1) {
    @addresses = ();
    foreach my $LinkDev (keys(%addresses)) {
      (@addresses) = (@addresses, @{$addresses{$LinkDev}});
    }

    logmsg (3, "Updating RRDs");

    $updatetime = int(time()/60) * 60;		# Round off to previous minute mark

    foreach $address (@addresses) {
      $name        = $data{$address}{name};
      next if ($name eq 'UNKNOWN');
      $temperature = $data{$address}{temperature};
      $minute      = $data{$address}{minute};
      $temperature = 'U' unless defined($temperature);
      $minute      = 'U' unless defined($minute);
      if (defined($data{$address}{age})) {
        $age       = time - $data{$address}{age};
        if ($age > 10) {
          $temperature = 'U';
          $minute      = 'U';
        }
      } else {
        $temperature = 'U';
        $minute      = 'U';
      }
      $rrdfile = "$RRDsDir/" . lc($name) . ".rrd";

      if (-w $rrdfile) {
        RRDs::update ($rrdfile, "$updatetime:$temperature:$minute");
        $rrderror=RRDs::error;
        logmsg (1, "ERROR while updating RRD file for $name: $rrderror") if $rrderror;
      } else {
        # Create RRD file
        logmsg (1, "Creating $rrdfile");
        RRDs::create ($rrdfile, "--step=60", "DS:ds0:GAUGE:300:U:300", "DS:ds1:GAUGE:300:U:300",
	"RRA:MIN:0.5:1:4000", "RRA:MIN:0.5:30:800", "RRA:MIN:0.5:120:800", "RRA:MIN:0.5:1440:800",
	"RRA:MAX:0.5:1:4000", "RRA:MAX:0.5:30:800", "RRA:MAX:0.5:120:800", "RRA:MAX:0.5:1440:800",
	"RRA:AVERAGE:0.5:1:4000", "RRA:AVERAGE:0.5:30:800", "RRA:AVERAGE:0.5:120:800", "RRA:AVERAGE:0.5:1440:800"
	);
        $rrderror=RRDs::error;
        logmsg (1, "ERROR while creating/updating RRD file for $name: $rrderror") if $rrderror;
      }
    }
    logmsg (3, "Finished updating RRDs");
    sleep 60;
  }
}

sub monitor_agedata {
  my $age;
  sleep 1;		# give the other threads a second to get started
  while(1) {
    foreach my $LinkDev (@LinkHubs,@LinkTHs) {
      $age = (time() - $agedata{$LinkDev});
      logmsg (1,"Age data for $LinkDev: $age seconds");
      if ($age > 5) {
        logmsg (1,"Age data (${age}s) for $LinkDev indicates it is stale.");
        $threads{$LinkDev}->kill('HUP');
      }
    }
    sleep 1;
  }
}


sub ParseDeviceFile {
  my $errors = 0;
  open(DEVICES,  "<$DeviceFile") or die "Can't open devices file ($DeviceFile): $!";
  while (<DEVICES>) {
    s/\w*#.*//;
    next if (m/^\s*$/);
    if (m/^([0-9A-Fa-f]{16})\s+([A-Za-z0-9-_]+)\s+([A-Za-z0-9]+)\s*$/) {
      if (! defined($deviceDB{$1})) {
        $deviceDB{$1} = &share( {} );
      }
      $deviceDB{$1}{name} = $2;
      $deviceDB{$1}{type} = $3;
    } else {
      logmsg 1, "Unrecognised line in devices file:\n$_";
      $errors = 1;
    }
  }
  close(DEVICES);
  logmsg 1, "Couldn't parse devices file ($DeviceFile)." if ($errors);
  if (! keys(%deviceDB)) {
    logmsg 1, "Warning: No devices defined in $DeviceFile\n";
  }
  %data = %deviceDB;
}

sub LinkConnect {
  my $LinkDev = shift;
  my $socket;
  my $retry = 0;
  logmsg 4, "Connecting to $main::LinkType $LinkDev";

  $socket = undef;
  while (! $socket) {
    if ($main::LinkType eq 'LinkHubE') {
      $socket = IO::Socket::INET->new (
		PeerAddr => $LinkDev,
		PeerPort => '10001',
		Proto    => 'tcp',
		Timeout  => 5,
		#Blocking => 0,
		);
    } elsif ($main::LinkType eq 'LinkSerial') {
      $socket=Device::SerialPort->new($LinkDev);
      if ($socket) {
        $socket->baudrate(9600);
        $socket->databits(8);
        $socket->parity('none');
        $socket->stopbits(1);
        $socket->read_char_time(0);                   # don't wait for each character
        $socket->read_const_time(100);                # 100 millisecond per unfulfilled "read" call
      }
    }
    last if ($socket);
    $retry++;
    if ($retry > 5) {
      logmsg 2, "Couldn't connect to $LinkDev after 5 retries: $!";
      last;
    }

    logmsg 4, "Couldn't connect to $LinkDev $!, retrying... (attempt $retry)";
    sleep 1;
  }
  logmsg 1, "Connected to $LinkDev" if ($socket);
  return $socket;
}

sub LinkData {
  my $send = shift;
  my $returned;
  my $tmp;
  if ($main::LinkType eq 'LinkHubE') {
    if (defined($main::socket)) {
      $main::socket->send($send);
      sleep $SleepTime;
      if ($main::select->can_read(1)) {
        $main::socket->recv($returned,128);
      } else {
        logmsg 1, "Couldn't read from $main::LinkDev. Closing connection.";
        $main::socket->close;
        $main::socket = undef;
      }
    }
  } elsif ($main::LinkType eq 'LinkSerial') {
    if (defined($main::socket)) {
      $main::socket->write($send);
      sleep $SleepTime;
      ($tmp,$returned) = $main::socket->read(1023);
      if (!defined($returned)) {
        logmsg 1, "Couldn't read from $main::LinkDev. Closing connection.";
        $main::socket->close;
        $main::socket = undef;
      }
    }
  }
  if (defined($returned)) {
    $returned =~ s/[?\r\n\0]*$//;
    $returned =~ s/^\?*//;
  } else {
    $returned = '';
  }
  return $returned;
}

sub CRC {
  my $data = shift;

  my @CRClookup = (
	0, 94, 188, 226, 97, 63, 221, 131, 194, 156, 126, 32, 163, 253, 31, 65,
	157, 195, 33, 127, 252, 162, 64, 30, 95, 1, 227, 189, 62, 96, 130, 220,
	35, 125, 159, 193, 66, 28, 254, 160, 225, 191, 93, 3, 128, 222, 60, 98,
	190, 224, 2, 92, 223, 129, 99, 61, 124, 34, 192, 158, 29, 67, 161, 255,
	70, 24, 250, 164, 39, 121, 155, 197, 132, 218, 56, 102, 229, 187, 89, 7,
	219, 133, 103, 57, 186, 228, 6, 88, 25, 71, 165, 251, 120, 38, 196, 154,
	101, 59, 217, 135, 4, 90, 184, 230, 167, 249, 27, 69, 198, 152, 122, 36,
	248, 166, 68, 26, 153, 199, 37, 123, 58, 100, 134, 216, 91, 5, 231, 185,
	140, 210, 48, 110, 237, 179, 81, 15, 78, 16, 242, 172, 47, 113, 147, 205,
	17, 79, 173, 243, 112, 46, 204, 146, 211, 141, 111, 49, 178, 236, 14, 80,
	175, 241, 19, 77, 206, 144, 114, 44, 109, 51, 209, 143, 12, 82, 176, 238,
	50, 108, 142, 208, 83, 13, 239, 177, 240, 174, 76, 18, 145, 207, 45, 115,
	202, 148, 118, 40, 171, 245, 23, 73, 8, 86, 180, 234, 105, 55, 213, 139,
	87, 9, 235, 181, 54, 104, 138, 212, 149, 203, 41, 119, 244, 170, 72, 22,
	233, 183, 85, 11, 136, 214, 52, 106, 43, 117, 151, 201, 74, 20, 246, 168,
	116, 42, 200, 150, 21, 75, 169, 247, 182, 232, 10, 84, 215, 137, 107, 53
);

  my $size;
  my $count;
  my @bytes;

  my @chars = split(//, $data);
  $size = @chars;
  for ($count = 0; $count < $size; $count += 2) {
    push (@bytes, $chars[$count] . $chars[$count+1]);
  }

  $size = @bytes;

  my $crc = 0;
  my $I;
  for ($count = 0; $count < $size; $count++) {
    $I = $crc ^ hex $bytes[$count];
    $crc = $CRClookup[$I];
  }
  return 0 if ($crc);	# if $crc <> 0 then CRC failed
  return 1;		# if $crc == 0 then CRC passed
}
