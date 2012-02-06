#!/usr/bin/perl -w

use strict;

use Config;
$Config{useithreads} or die "Recompile Perl with threads to run this program.";

use threads;
use threads::shared;

use IO::Select;
use IO::Socket::INET;
use Device::SerialPort;
#use Digest::CRC qw(crc16 crc8 crc16_hex crc8_hex);

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
my $ReSearchOnError = 1;
my $IgnoreCRCErrors = 0;
my $UpdateMSType = 0;

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
    } elsif ($option eq 'UpdateMSType') {
      if ($value =~ m/^(1|0|true|false|yes|no)$/i) {
        $UpdateMSType = 1 if ($value =~ m/^(1|true|yes)$/i);
      } else {
        print STDERR "UpdateMSType value defined in config file ($value) is not valid. Using default ($UpdateMSType).\n";
      }
    } elsif ($option eq 'IgnoreCRCErrors') {
      if ($value =~ m/^(1|0|true|false|yes|no)$/i) {
        $IgnoreCRCErrors = 1 if ($value =~ m/^(1|true|yes)$/i);
      } else {
        print STDERR "IgnoreCRCErrors value defined in config file ($value) is not valid. Using default ($IgnoreCRCErrors).\n";
      }
    } elsif ($option eq 'ReSearchOnError') {
      if ($value =~ m/^(1|0|true|false|yes|no)$/i) {
        $ReSearchOnError = 0 if ($value =~ m/^(0|false|no)$/i);
      } else {
        print STDERR "ReSearchOnError value defined in config file ($value) is not valid. Using default ($ReSearchOnError).\n";
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

my %mstype = (
        '00' => 'temperature',
        '19' => 'humidity',
        '1A' => 'voltage',
        '1B' => 'light',
        '1C' => 'current',
        '20' => 'pressure',
        '21' => 'pressure150',
        '22' => 'depth15',
    );

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
  my $tid = threads->tid();
  my $level = shift;
  if ($level <= $LogLevel) {
    if ($RunAsDaemon) {
      print LOG scalar localtime, " $0\[$$\]: ($tid) @_\n";
    } else {
      print scalar localtime, " $0\[$$\]: ($tid) @_\n";
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
  logmsg 2, "Search for new devices will be done on the next pass.";
};


### Beginning of listener
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

    ### Without the following the report thread will occasionally receive the signal too and it will eventually result in a segfault
    local $SIG{'HUP'} = 'IGNORE';	# we do not want the thread that processes queries to act on a SIGHUP

    logmsg 5, "Connection on socket $ListenPort";
    ### Starting a new thread causes a massive memory leak apparently due to the socket
    ### Also the overhead of thread creation significantly increases load on the system
    ### In testing I have not been able to generate a case where it couldn't answer queries
    ### with only one thread. A thread pool could be used but probably doesn't warrant the complexity
    #$listener = threads->new(\&report, $client);
    report($client);
    close ($client) if (defined($client));	# This socket is handled by the new thread
    $client=undef;

    ### Clean up any threads that have completed.
    foreach my $thread (threads->list(threads::joinable)) {
      if ($thread->tid && !threads::equal($thread, threads->self)) {
        $thread->join;
        $thread = undef;
      }
    }

  }
}
logmsg 1, "Closing socket $ListenPort";
close ($server_sock);
if (! ($ListenPort =~ m/^\d+$/)) {
  logmsg(1, "Removing socket on $ListenPort");
  unlink "$ListenPort";
}
### Nothing after this will run since this is a continual loop
### End of listener


### clean up all threads
foreach my $thread (threads->list) {
  if ($thread->tid && !threads::equal($thread, threads->self)) {
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
    if (lc($command) eq 'list') {
      $socket->send(list());
    } elsif (lc($command) eq 'listdb') {
      $socket->send(listdb());
    } elsif (lc($command) eq 'value all') {
      $socket->send(value_all());
    } elsif ($command =~ m/^value (.*)/i) {
      $socket->send(value($1));
    } elsif (lc($command) eq 'refresh') {
      $socket->send(refresh());
    } elsif (lc($command) eq 'reload') {
      $socket->send(reload());
    } elsif ($command =~ m/^search(.*)/i) {
      $socket->send(search($1));
    } elsif (lc($command) eq 'help') {
      $socket->send(helpmsg());
    } else {
      $socket->send("UNKNOWN command: $command\n");
      $socket->send(helpmsg());
    }
  }
  close ($socket) if (defined($socket));
  $socket=undef;
  $select=undef;
  return(0);
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

  my ($temperature, $voltage, $icurrent);
  my $address;
  my ($type, $name);

  my $DoSearch = 1;
  my $LastDev;

  while(1) {
    $count++;
    $agedata{$LinkDev} = time();

    $socket = LinkConnect($LinkDev) if ( (! defined($socket)) or (! $socket) );
    if ( (! defined($socket)) or (! $socket) ) {	# Failed to connect
      sleep 10;						# Wait for 10 seconds before retrying
      next;
    }

    $select = IO::Select->new($socket) if ($LinkType eq 'LinkHubE');

    $returned = LinkData("\n");				# Discard any existing data
    # Do not check this data as we are not interested in it and after initial run should be empty and CheckData would return false

### Begin search for devices on LinkHub
    if ( ($LinkDevData{$LinkDev}{SearchNow}) || (($DoSearch) || (($AutoSearch) && ($count == 1))) ) {
      $LinkDevData{$LinkDev}{ds1820} = 0;
      $LastDev = 0;
      logmsg 3, "Searching for devices on $LinkDev";
      @addresses = ();

      unless (Reset()) {
        logmsg 1, "Initial reset failed during search on $LinkDev. Closing connection and sleeping 1 second before retrying.";
        close ($socket) if (defined($socket));
        $socket = undef;
        sleep 1;
        next;
      }

      $returned = LinkData("f\n");		# request first device ID
      if (! CheckData($returned)) {		# error or no data returned so we'll start again and keep trying
        logmsg 1, "Error or no data returned on search of $LinkDev. Sleeping 1 second before retrying.";
        sleep 1;
        next;
      }

      unless ( defined($LinkDevData{$LinkDev}{channels}) ) {
        if ( $returned =~ m/,[1-5]$/ ) {
          $LinkDevData{$LinkDev}{channels} = 1;
          logmsg 1, "Channel reporting supported on $LinkDev.";
        } else {
          logmsg 1, "Checking for channel reporting support on $LinkDev";
          $returned = LinkData('\$');		# Toggles channel reporting
          next if ( ( $returned ne '' ) && (! CheckData($returned) ) );
          $returned = LinkData("f\n");		# request first device ID again
          next if (! CheckData($returned));

          if ($returned =~ m/,[1-5]$/) {
            $LinkDevData{$LinkDev}{channels} = 1;
            logmsg 1, "Channel reporting enabled on $LinkDev.";
          } else {
            $LinkDevData{$LinkDev}{channels} = 0;
            logmsg 1, "Channel reporting NOT supported on $LinkDev.";
          }
        }
      }

      $returned =~ s/^[-+,]*//gs;
      if ($returned eq 'N') {			# no devices found so we'll start again and keep trying until something is found
        logmsg 4, "No devices found on $LinkDev. Sleeping 1 second before retrying.";
        sleep 1;
        next;
      }

      # We have found at least one device so we can turn off the need to do more searches
      $DoSearch = 0 unless ($AutoSearch);
      $LinkDevData{$LinkDev}{SearchNow} = 0;	# Default to not needing a new search unless an error is found
      my $channel = '';
      if ($LinkDevData{$LinkDev}{channels}) {
        $returned =~ s/,([1-5])$//;
        $channel = $1;
      }
      if ($returned =~ m/^.$/) {		# Error but we'll keep searching in case there are more devices
        logmsg 1, "First device search on $LinkDev returned '$returned'";
        $LinkDevData{$LinkDev}{SearchNow} = 1 if ($ReSearchOnError);
      } elsif ($returned eq '0000000000000000') {
        logmsg 1, "First device search on $LinkDev returned '$returned'";
        $LinkDevData{$LinkDev}{SearchNow} = 1 if ($ReSearchOnError);
      } elsif ($returned =~ s/^(..)(..)(..)(..)(..)(..)(..)(..)$/$8$7$6$5$4$3$2$1/) {
        if (! CRC($returned)) {
          logmsg 1, "CRC FAILED on $LinkDev for device ID $returned";
          $LinkDevData{$LinkDev}{SearchNow} = 1 if ($ReSearchOnError);
        } else {
          #next if ( $returned =~ m/^01/);
          if (! defined($data{$returned})) {
            $data{$returned} = &share( {} );
          }
          if ( (defined($data{$returned}{name})) && ($data{$returned}{name} eq 'ignore') ) {
            logmsg 1, "Ignoring $returned on $LinkDev.";
          } else {
            if (grep( /^$returned$/,@addresses ) ) {
              logmsg 1, "$LinkDev:$returned already found.";
            } else {
              push (@addresses, $returned);
            }
          }
          $data{$returned}{linkdev} = $LinkDev;
          $data{$returned}{channel} = $channel;
          $data{$returned}{name} = $returned if (! defined($data{$returned}{name}));
          $data{$returned}{type} = 'unknown' if (! defined($data{$returned}{type}));
          if ( $returned =~ m/^01/) {
            # DS2401
            $data{$returned}{type} = 'ds2401' if ($data{$returned}{type} eq 'unknown');
            $returned =~ m/^..(.{12})..$/;                # 48bit serial number
            $data{$returned}{ds2401} = $1;
          }
          if ( $returned =~ m/^26/) {
            # DS2438
            $data{$returned}{type} = 'query' if ($data{$returned}{type} eq 'unknown');
          }
          if ( ($returned =~ m/^1D/) && (! ( ($data{$returned}{type} eq 'ds2423') or ($data{$returned}{type} eq 'rain') ) ) ) {
            # DS2423
            logmsg 3, "Setting device $returned type to 'ds2423'";
            $data{$returned}{type} = 'ds2423';
          }
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
        $LinkDevData{$LinkDev}{SearchNow} = 1 if ($ReSearchOnError);
      }
      while (!($LastDev)) {
        $returned = LinkData("n\n");			# request next device ID
        next if (! CheckData($returned));
        if ($returned =~ m/^.$/) {			# Error searching so we'll just move on in case there are more devices
          logmsg 1, "Device search on $LinkDev returned '$returned'";
          $LinkDevData{$LinkDev}{SearchNow} = 1 if ($ReSearchOnError);
          next;
        }
        $LastDev = 1 if (!($returned =~ m/^\+,/));	# This is the last device
        $returned =~ s/^[-+,]*//gs;
        my $channel = '';
        if ($LinkDevData{$LinkDev}{channels}) {
          $returned =~ s/,([1-5])$//;
          $channel = $1;
        }
        if ($returned =~ s/^(..)(..)(..)(..)(..)(..)(..)(..)$/$8$7$6$5$4$3$2$1/) {
          if ($returned eq '0000000000000000') {
            logmsg 1, "Device search on $LinkDev returned '$returned'";
            $LinkDevData{$LinkDev}{SearchNow} = 1 if ($ReSearchOnError);
            next;
          }
          if (! CRC($returned)) {
            logmsg 1, "CRC FAILED on $LinkDev for device ID $returned";
            $LinkDevData{$LinkDev}{SearchNow} = 1 if ($ReSearchOnError);
            next;
          }
          #next if ( $returned =~ m/^01/);
          if (! defined($data{$returned})) {
            $data{$returned} = &share( {} );
          }
          if ( (defined($data{$returned}{name})) && ($data{$returned}{name} eq 'ignore') ) {
            logmsg 1, "Ignoring $returned on $LinkDev.";
          } else {
            if (grep( /^$returned$/,@addresses ) ) {
              logmsg 1, "$LinkDev:$returned already found.";
              next;
            }
            push (@addresses, $returned);
          }
          $data{$returned}{linkdev} = $LinkDev;
          $data{$returned}{channel} = $channel;
          $data{$returned}{name} = $returned if (! defined($data{$returned}{name}));
          $data{$returned}{type} = 'unknown' if (! defined($data{$returned}{type}));
          if ( $returned =~ m/^01/) {
            # DS2401
            $data{$returned}{type} = 'ds2401' if ($data{$returned}{type} eq 'unknown');
            $returned =~ m/^..(.{12})..$/;                # 48bit serial number
            $data{$returned}{ds2401} = $1;
          }
          if ( $returned =~ m/^26/) {
            # DS2438
            $data{$returned}{type} = 'query' if ($data{$returned}{type} eq 'unknown');
          }
          if ( ($returned =~ m/^1D/) && (! ( ($data{$returned}{type} eq 'ds2423') or ($data{$returned}{type} eq 'rain') ) ) ) {
            # DS2423
            logmsg 3, "Setting device $returned type to 'ds2423'";
            $data{$returned}{type} = 'ds2423';
          }
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
          $LinkDevData{$LinkDev}{SearchNow} = 1 if ($ReSearchOnError);
        }
      }
      logmsg 1, "An error during the search on $LinkDev produced an error. Another search has been requested." if ($LinkDevData{$LinkDev}{SearchNow});
      logmsg 5, "Found last device on $LinkDev.";

      (@{$addresses{$LinkDev}}) = @addresses;
      $LinkDevData{$LinkDev}{SearchTime} = time();
    }
### End search for devices on LinkHub

### Begin addressing ALL devices
    next if (! Reset());

    ### BEGIN setting all 2438's to read input voltage rather than supply voltage
    $returned = LinkData("bCC4E0071\n");	# byte mode, skip rom (address all devices), write scratch 4E, register 00, value 71
    next if (! CheckData($returned));
    sleep 0.01;					# wait 10ms
    next if (! Reset());
    $returned = LinkData("bCCBE00FFFFFFFFFFFFFFFFFF\n");	# byte mode, skip rom (address all devices), read scratch BE, register 00
    next if (! CheckData($returned));
    sleep 0.01;					# wait 10ms
    next if (! Reset());
    $returned = LinkData("bCC4800\n");		# byte mode, skip rom (address all devices), copy scratch 48, register 00
    next if (! CheckData($returned));
    sleep 0.01;					# wait 10ms
    next if (! Reset());
    ### END setting all 2438's to read input voltage rather than supply voltage

    $returned = LinkData("pCC44\n");		# byte mode in pull-up mode, skip rom (address all devices), convert T
    next if (! CheckData($returned));
    sleep 0.1;					# wait 100ms for temperature conversion
    next if (! Reset());
    $returned = LinkData("bCCB4\n");		# byte mode, skip rom (address all devices), convert V
    next if (! CheckData($returned));
    sleep 0.01;					# wait 10ms for voltage conversion
### End addressing ALL devices

### Begin query of devices on LinkHub
    foreach $address (@addresses) {
      last if (! defined($socket));
      next if ( (! defined($data{$address})) || (! defined($data{$address}{linkdev})) );
      next if ($data{$address}{type} eq 'ds2401');
      if ($data{$address}{linkdev} eq $LinkDev) {
        $data{$address}{name} = $address if (! defined($data{$address}{name}));
        $name = $data{$address}{name};

        # If this is a Multi Sensor then query it for it's type and update it if neccessary
        if ( ($address =~ m/^26/) && (! defined($data{$address}{mstype})) ) {
          QueryMSType($address);
          if ($data{$address}{type} ne $data{$address}{mstype}) {
            logmsg 1, "$name type mismatch: config: $data{$address}{type}; sensor: $data{$address}{mstype}";
            ChangeMSType($address) if ($UpdateMSType);
          }
        }

        if (! $data{$address}{type}) {
          logmsg 2, "$name is of an unknown type.";
          $data{$address}{type} = 'unknown';
        }
        $type = $data{$address}{type};

        logmsg 5, "querying $name ($address) as $type";

        $returned = query_device($socket,$select,$address,$LinkDev);

        my $retry = 0;
        while ($returned eq 'ERROR') {
          $retry++;
          if ($retry > 5) {
            logmsg 1, "Didn't get valid data from $LinkDev:$name";
            last;
          }
          logmsg 3, "Didn't get valid data for $LinkDev:$name, retrying... (attempt $retry)";
          $returned = query_device($socket,$select,$address,$LinkDev);
        }

        if ($returned ne 'ERROR') {
          $temperature = $returned;
          $voltage = $returned;
          $icurrent = $returned;
          if ( ($data{$address}{type} eq 'ds2423') or ($data{$address}{type} eq 'rain') ) {
            $voltage =~ s/^.{64}(..)(..)(..)(..)0{8}.{4}$/$4$3$2$1/;	# 32bytes{64}, InputA{8}, 32x0bits, CRC16
            $voltage = hex $voltage;
            $data{$address}{temperature} = '';
            if ( ($data{$address}{type} eq 'rain') and (defined($data{$address}{rain})) and ($voltage > ($data{$address}{rain} + 10)) and ((time - $data{$address}{age}) < 60) ) {
              ### If the counter is more than 10 above the previous recorded value it is probably not correct
              ### If the current data is more than 60s old record it anyway
              ### The counter can go backwards if it wraps so only check for large increases
              logmsg 1, "(query) Spurious reading ($voltage) for $name: keeping previous data ($data{$address}{rain})";
              next;
            }
          } else {
            $temperature =~ s/^(....).*$/$1/;
            $voltage =~ s/^....(....).*$/$1/;
            $icurrent =~ s/^........(....).*$/$1/;

            $icurrent =~ s/^(..)(..)$/$2$1/;
            $icurrent = hex $icurrent;
            #$icurrent -= 65535 if ($icurrent > 1023);			# This gives + or - current
            $icurrent = 65535 - $icurrent if ($icurrent > 1023);	# This gives only + current
            $data{$address}{icurrent} = $icurrent;
            if (! defined($data{$address}{iminute})) {
              $data{$address}{iminute} = $icurrent;
              $data{$address}{itime} = time();
            } elsif ( ($icurrent > $data{$address}{iminute}) || ((time() - $data{$address}{itime}) >= $MinutePeriod ) ) {
              $data{$address}{iminute} = $icurrent;
              $data{$address}{itime} = time();
            }

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
              $temperature -= 4096 if ($temperature > 4000);
            } else {
              $temperature =~ s/^(..)(..)$/$2$1/;
              $temperature = hex $temperature;
              $temperature = $temperature>>3;
              $temperature = $temperature*0.03125;
            }
            $temperature = restrict_num_decimal_digits($temperature,1);
  
            if (! defined($data{$address}{temperature})) {
              if ( $temperature == 85 ) {
                ### If the temperature is 85C it is probably a default value and should be ignored
                logmsg 1, "(query) Initial temperature ($temperature) for $name is probably not valid (85C is a default): discarding readings.";
                next;
              } else {
                $data{$address}{temperature} = $temperature;
              }
            } elsif ( ($temperature > ($data{$address}{temperature} + 10)) || ($temperature < ($data{$address}{temperature} - 10)) and ((time - $data{$address}{age}) < 60) ) {
              ### If the temperature is more than 10 above or below the previous recorded value it is not correct and the voltage will also be wrong
              ### If the current data is more than 60s old record it anyway
              logmsg 1, "(query) Spurious temperature ($temperature) for $name: keeping previous data ($data{$address}{temperature})";
              next;
            }
            $data{$address}{temperature} = $temperature;

            $voltage =~ s/^(..)(..)$/$2$1/;
            $voltage = hex $voltage;
            $voltage = 0 if ($voltage == 1023);	# 1023 inidicates a short or 0V
            $voltage = $voltage*0.01;		# each unit is 10mV
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
              #$voltage = ($voltage - 0.958)/0.0307;
              $voltage = ( ($voltage / 5) - 0.16) / 0.0062;
            }
            if ($type eq 'light') {
              # if the reading is over 5V then it is actually < 0V and indicates darkness
              $voltage = 0 if ($voltage > 5);
              # double the voltage reading for light to give a range from 0-10
              $voltage = $voltage * 2;
            }
            if ($type eq 'depth15') {
              next if ($voltage > 5);
              # 266.67 mV/psi; 0.5V ~= 0psi
              $voltage = ($voltage - 0.5) * 3.75;
              # 1.417psi/metre
              $voltage = $voltage / 1.417;
            }
            if ($type eq 'pressure150') {
              next if ($voltage > 5);
              # 26.67 mV/psi; 0.5V ~= 0psi
              $voltage = ($voltage - 0.5) * 37.5;
            }
            if ($type eq 'pressure') {
              # 25.7 mV/psi; 0.43V ~= 0psi
              $voltage = ($voltage - 0.43) * 38.91;
            }
            if ( ($type eq 'temperature') || ($type eq 'tsense') || ($type eq 'ds1820') ) {
              $voltage = $temperature;
            }
            if ($type eq 'depth15') {
              $voltage = restrict_num_decimal_digits($voltage,2);
            } else {
              $voltage = restrict_num_decimal_digits($voltage,1);
            }
          }
          $data{$address}{$type} = $voltage;
          if (! defined($data{$address}{minute})) {
            $data{$address}{minute} = $voltage;
            $data{$address}{time} = time();
          } elsif ( ($voltage > $data{$address}{minute}) || ((time() - $data{$address}{time}) >= $MinutePeriod ) ) {
            $data{$address}{minute} = $voltage;
            $data{$address}{time} = time();
          }
          if (! defined($data{$address}{FiveMinute})) {
            $data{$address}{FiveMinute} = $data{$address}{minute};
            $data{$address}{FiveTime} = time();
          } elsif ( ($data{$address}{minute} > $data{$address}{FiveMinute}) || ((time() - $data{$address}{FiveTime}) >= $FiveMinutePeriod ) ) {
            $data{$address}{FiveMinute} = $data{$address}{minute};
            $data{$address}{FiveTime} = time();
          }
          $data{$address}{age} = time();
        }
      }
    }
    logmsg 5, "Finished querying devices on $LinkDev.";
### End query of devices on LinkHub

    $count = 0 if ($count > 1);
    if ($SlowDown) {		# This will slow down the rate of queries and close the socket allowing other connections to the 1wire master
      logmsg 5, "Finished loop for $LinkDev, closing socket.";
      close ($socket) if (defined($socket));
      $socket = undef;
      sleep $SlowDown;
    }
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

  while (1) {
    sleep 1;
    $socket = LinkConnect($LinkDev) if ( (! defined($socket)) or (! $socket) );
    if ( (! defined($socket)) or (! $socket) ) {	# Failed to connect
      logmsg 3, "Failed to connect to $LinkDev. Sleeping for 10 seconds before retrying";
      sleep 10;						# Wait for 10 seconds before retrying
      next;
    }

    $socket->write("D");				# Request ALL LinkTH data
    sleep $SleepTime;
    ($tmp,$returned) = $socket->read(1023);		# Get reply
    my $retry = 0;
    while ($tmp == 0 ) {				# No data in reply if $tmp == 0
      $retry++;
      if ($retry > 10) {
        logmsg 1, "Didn't get any data from $LinkDev after 10 retries. Starting again.";
        last;
      }
      logmsg 3, "Didn't get any data from $LinkDev, retrying... (attempt $retry)";
      sleep ($SleepTime + ($retry * 0.1));
      ($tmp,$returned) = $socket->read(1023);		# Get reply
    }
    next if ($retry > 10);				# Too many retries, start again.

    if ($returned =~ m/01 - No sensor present/) {
      logmsg 4, "No devices found on $LinkDev. Sleeping 1 second before retrying.";
      sleep 1;
      next;
    }

    next if (! ($returned =~ m/EOD/));			# If there is no EOD then we haven't got all the devices, start again.

    $agedata{$LinkDev} = time();
    @addresses = ();

    foreach (split(/\r?\n/, $returned)) {
      if ($_ eq 'EOD') {
        $tmp = 'EOD';					# Record that we have reached EOD
        last;
      }
      @tmp = split(/,/, $_);				# $address $type,$temperatureC,$temperatureF,$value,???[,$timestamp]
      if ($tmp[0] =~ s/ ([0-9A-F]{2})$//) {
        $address = $tmp[0];
        $type = $1;
        $temperature = $tmp[1];
        $tmp[3] = 0 if (! $tmp[3]);
        $voltage = $tmp[3];
        $voltage = 0 unless ($voltage =~ m/^\d+$/);

        push (@addresses, $address);
        if (! defined($data{$address})) {
          $data{$address} = &share( {} );
        }
        $data{$address}{linkdev} = $LinkDev;
        if (! defined($data{$address}{type})) {
          if ( defined($mstype{$type}) ) {
            $data{$address}{type} = $mstype{$type};
          } else {
            $data{$address}{type} = 'unknown';
          }
        }

        $data{$address}{name} = $address if (! defined($data{$address}{name}));
        $name = $data{$address}{name};

        if ( ($address =~ m/^28/) && ($data{$address}{type} ne 'tsense') ) {
          logmsg 3, "Setting device $name type to 'tsense'";
          $data{$address}{type} = 'tsense';
        }
        logmsg 4, "Found $address ($name) on $LinkDev";

        $data{$address}{mstype} = $type;
        $data{$address}{raw} = 'NA';

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
            logmsg 1, "(query) Initial temperature ($temperature) for $name is probably not valid (85C is a default): discarding readings.";
            next;
          } else {
            $data{$address}{temperature} = $temperature;
          }
        } elsif ( ($temperature > ($data{$address}{temperature} + 10)) || ($temperature < ($data{$address}{temperature} - 10)) and ((time - $data{$address}{age}) < 60) ) {
          ### If the temperature is more than 10 above or below the previous recorded value it is not correct and the voltage will also be wrong
          ### If the current data is more than 60s old record it anyway
          logmsg 1, "(query) Spurious temperature ($temperature) for $name: keeping previous data ($data{$address}{temperature})";
          next;
        }
        $data{$address}{temperature} = $temperature;

        $voltage = restrict_num_decimal_digits($voltage,1);
        $data{$address}{$data{$address}{type}} = $voltage;
        if (! defined($data{$address}{minute})) {
          $data{$address}{minute} = $voltage;
          $data{$address}{time} = time();
        } elsif ( ($voltage > $data{$address}{minute}) || ((time() - $data{$address}{time}) >= $MinutePeriod ) ) {
          $data{$address}{minute} = $voltage;
          $data{$address}{time} = time();
        }
        if (! defined($data{$address}{FiveMinute})) {
          $data{$address}{FiveMinute} = $data{$address}{minute};
          $data{$address}{FiveTime} = time();
        } elsif ( ($data{$address}{minute} > $data{$address}{FiveMinute}) || ((time() - $data{$address}{FiveTime}) >= $FiveMinutePeriod ) ) {
          $data{$address}{FiveMinute} = $data{$address}{minute};
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
        return 'ERROR' if (! Reset());
        $returned = LinkData("p55${address}44\n");	# byte mode in pull-up mode, match rom, address, convert T
        return 'ERROR' if (! CheckData($returned));
        sleep 1;					# Give it time to convert T
      }

      return 'ERROR' if (! Reset());
  
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

      $returned = LinkData("b55${address}BEFFFFFFFFFFFFFFFFFF\n");	# byte mode, match rom, address, read command BE, 9 bytes FF
      return 'ERROR' if (! CheckData($returned));
      if ( (length($returned) != 38) || (! $returned =~ m/^55${address}BE[A-F0-9]{18}$/) ) {
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
        logmsg 2, "ERROR: returned data not valid for $data{$address}{name}: $returned";
        return 'ERROR';
      }
    } elsif ( ($data{$address}{type} eq 'ds2423') or ($data{$address}{type} eq 'rain') ) {
      my $page = 'C0';
      $page = 'E0' if ($data{$address}{type} eq 'rain');
      return 'ERROR' if (! Reset());
      $returned = LinkData("b55${address}A5${page}01".('F' x 84)."\n");	# byte mode, match rom, address, read memory + counter, address 01C0h
      return 'ERROR' if (! CheckData($returned));
      if ( (length($returned) != 108) || (! ($returned =~ m/^55${address}A5${page}01[A-F0-9]{84}$/)) ) {
        logmsg 3, "ERROR: Sent b55${address}A5${page}01 command; got: $returned";
        return 'ERROR';
      }
      if ($returned =~ s/^55${address}A5${page}01//) {
#        if (! CRC16($returned) ) {
#          logmsg 1, "ERROR: CRC failed for $LinkDev:$data{$address}{name}: $returned";
#          return 'ERROR' unless ($IgnoreCRCErrors);
#        }
        return $returned;
      } else {
        logmsg 2, "ERROR: returned data not valid for $data{$address}{name}: $returned";
        return 'ERROR';
      }

    } else {
      return 'ERROR' if (! Reset());
      $returned = LinkData("b55${address}B800\n");	# byte mode, match rom, address, Recall Memory page 00 to scratch pad
      return 'ERROR' if (! CheckData($returned));
      if ($returned ne "55${address}B800") {
        logmsg 3, "ERROR: Sent b55${address}B800 command; got: $returned";
        return 'ERROR';
      }

      return 'ERROR' if (! Reset());

      # BE00FFFFFFFFFFFFFFFFFF
      # BE00xxyyzzaabbccddeeff
      # xx: status register
      # yy: LSB for temperature
      # zz: MSB for temperature
      # aa: LSB for voltage
      # bb: MSB for voltage
      # ff: CRC

      $returned = LinkData("b55${address}BE00FFFFFFFFFFFFFFFFFF\n");	# byte mode, match rom, address, read scratch pad for memory page 00
      return 'ERROR' if (! CheckData($returned));
      if ( (length($returned) != 40) || (! ($returned =~ m/^55${address}BE00[A-F0-9]{18}$/)) ) {
        logmsg 3, "ERROR: Sent b55${address}BE00FFFFFFFFFFFFFFFFFF command; got: $returned";
        return 'ERROR';
      }
      if ( $returned =~ m/^55${address}BE00F{18}$/ ) {
        logmsg 4, "ERROR: Sent b55${address}BE00FFFFFFFFFFFFFFFFFF command; got: $returned";
        return 'ERROR';
      }
      if ($returned =~ s/^55${address}BE00//) {
        if (! CRC($returned) ) {
          logmsg 1, "ERROR: CRC failed for $LinkDev:$data{$address}{name}";
          return 'ERROR' unless ($IgnoreCRCErrors);
        }
        $returned =~ s/^..//;
        return $returned;
      } else {
        logmsg 2, "ERROR: returned data not valid for $data{$address}{name}: $returned";
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
  return $num unless(defined($num));
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

  foreach my $address (@addresses) {
    $address =~ s/[\r\n\0]//g;		# Remove any CR, LF or NULL characters first
    $address =~ s/^[?!,]*//;		# THEN remove appropriate any leading characters
    if ((defined($deviceDB{$address})) && (defined($deviceDB{$address}{name})) ) {
      $output .= sprintf "Device found:   %-18s %16s %s,%s\n", $deviceDB{$address}{name}, $address, $data{$address}{linkdev}, $data{$address}{channel};
    } else {
      $output .= sprintf "UNKNOWN device:   %16s %s,%s\n", $address, $data{$address}{linkdev}, $data{$address}{channel};
    }
  }
  foreach $tmp (keys(%deviceDB)) {
    if (! grep $_ eq $tmp, @addresses) {
      $output .= sprintf "NOT RESPONDING: %-18s %16s\n", $deviceDB{$tmp}{name}, $tmp unless ($deviceDB{$tmp}{name} eq 'ignore');
    }
  }
  $output .= "-------------------------------------------------------------------------------\n";
  foreach my $LinkDev (keys(%addresses)) {
    my $age = 0;
    if (defined($LinkDevData{$LinkDev}{SearchTime})) {
      $age = time - $LinkDevData{$LinkDev}{SearchTime};
    }
    $output .= "$LinkDev last searched $age seconds ago\n";
  }
  return $output;
}

sub refresh {
  my $output = '';
  $output .= reload();
  $output .= search();
  return $output;
}

sub reload {
  my $output = '';
  logmsg(1, "Re-reading device file.");
  $output .= "Re-reading device file.\n";
  ParseDeviceFile();
  return $output;
}

sub search {
  my $LinkDev = shift;
  $LinkDev = '' if (!(defined($LinkDev)));
  $LinkDev =~ s/^ *//;
  my $output = '';
  if ( ($LinkDev eq 'all') || ($LinkDev eq '') ) {
    logmsg 2, "Scheduling search for devices on all Links.";
    $output .= "Scheduling search for devices on all Links.\n";
    foreach $LinkDev (@LinkHubs) {
      $LinkDevData{$LinkDev}{SearchNow} = 1;
    }
  } else {
    if (defined($LinkDevData{$LinkDev})) {
      logmsg 2, "Scheduling search for devices on $LinkDev.";
      $output .= "Scheduling search for devices on $LinkDev.\n";
      $LinkDevData{$LinkDev}{SearchNow} = 1;
    } else {
      logmsg 1, "'$LinkDev' is not a configured Link.";
      $output .= "'$LinkDev' is not a configured Link.\n";
    }
  }
  #logmsg 2, "Search for new devices will be done on the next pass.";
  #$output .= "Search for new devices will be done on the next pass.\n";
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

  my ($address, $name, $temperature, $type, $voltage, $age, $linkdev, $channel);
  foreach $address (@addresses) {
    eval {
      $name        = $data{$address}{name};
      $temperature = $data{$address}{temperature};
      $type        = $data{$address}{type};
      $voltage     = $data{$address}{$type};
      $linkdev     = $data{$address}{linkdev};
      $channel     = $data{$address}{channel};
      if (defined($data{$address}{age})) {
        $age       = time - $data{$address}{age};
      } else {
        $age       = '0';
      }
      $temperature = 'NA' unless defined($temperature);
      $voltage     = 'NA' unless defined($voltage);
      $name        = "* $address" if ($name eq $address);
      $type        =~ s/^pressure[0-9]+$/pressure/;
      $type        =~ s/^depth[0-9]+$/depth/;
      if ( defined($channel) ) {
        $channel = ",$channel" unless ($channel eq '');
      } else {
        $channel = '';
      }
      if ($type eq 'ds2401') {
        $OutputData{$name} = sprintf "%-18s - serial number: %-18s                      \t%s%s\n", $name, $voltage, $linkdev, $channel;
      } elsif ( ($type eq 'temperature') || ($type eq 'tsense') || ($type eq 'ds1820') ) {
        $OutputData{$name} = sprintf "%-18s - temperature: %5s                      (age: %3d s)\t%s%s\n", $name, $temperature, $age, $linkdev, $channel;
      } else {
        $OutputData{$name} = sprintf "%-18s - temperature: %5s - %10s: %5s  (age: %3d s)\t%s%s\n", $name, $temperature, $type, $voltage, $age, $linkdev, $channel;
      }
    }
  }
  foreach $name (sort(keys(%OutputData))) {
    $output .= $OutputData{$name};
  }

  $output .= "-------------------------------------------------------------------------------\n";
  foreach $LinkDev (keys(%addresses)) {
    $age = 0;
    if (defined($LinkDevData{$LinkDev}{SearchTime})) {
      $age = time - $LinkDevData{$LinkDev}{SearchTime};
    }
    $output .= "$LinkDev last searched $age seconds ago\n";
  }
  return $output;
}

sub value {
  my $output = '';
  my $search = lc(shift);

  my @addresses;
  foreach my $LinkDev (keys(%addresses)) {
    (@addresses) = (@addresses, @{$addresses{$LinkDev}});
  }

  my ($address, $name, $temperature, $type, $configtype, $mstype, $voltage, $minute, $FiveMinute, $time, $FiveTime, $age, $raw, $icurrent, $iminute, $itime, $linkdev, $channel);
  foreach $address (@addresses) {
    $name          = $data{$address}{name};
    if (($search eq lc($name)) || ($search eq lc($address))) {
      $temperature = $data{$address}{temperature};
      $type        = $data{$address}{type};
      $mstype      = $data{$address}{mstype};
      $voltage     = $data{$address}{$type};
      $linkdev     = $data{$address}{linkdev};
      $channel     = $data{$address}{channel};
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
      $icurrent    = $data{$address}{icurrent};
      $iminute     = $data{$address}{iminute};
      if ($data{$address}{itime}) {
        $itime      = time - $data{$address}{itime};
      } else {
        $itime      = 'NA';
      }
      $temperature = 'NA' unless defined($temperature);
      $voltage     = 'NA' unless defined($voltage);
      $minute      = 'NA' unless defined($minute);
      $FiveMinute  = 'NA' unless defined($FiveMinute);
      $raw         = 'NA' unless defined($raw);
      $icurrent    = 'NA' unless defined($icurrent);
      $iminute     = 'NA' unless defined($iminute);
      $configtype  = $type;
      $type        =~ s/^pressure[0-9]+$/pressure/;
      $type        =~ s/^depth[0-9]+$/depth/;
      if ( defined($channel) ) {
        $channel = ",$channel" unless ($channel eq '');
      } else {
        $channel = '';
      }
      if ( ($type eq 'temperature') || ($type eq 'tsense') || ($type eq 'ds1820') ) {
        $output .= "name: $name\naddress: $address\ntype: $type\ntemperature: $temperature\nupdated: $time\nage: $age\nlinkdev: $linkdev$channel\nConfigType: $configtype\n";
      } elsif ($type eq 'ds2401') {
        $output .= "name: $name\naddress: $address\ntype: $type\nserial number: $voltage\nlinkdev: $linkdev$channel\n";
      } else {
        $output .= "name: $name\naddress: $address\ntype: $type\ntemperature: $temperature\n$type: $voltage\n1MinuteMax: $minute\n5MinuteMax: $FiveMinute\nupdated: $time\nage: $age\nRawVoltage: $raw\nlinkdev: $linkdev$channel\nConfigType: $configtype\nMStype: $mstype\nInstantaneousCurrent: $icurrent\nCurrent1MinuteMax: $iminute\nCurrentTime: $itime\n";
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
list          : lists all devices including unkown and those not responding.
listdb        : lists all devices in the database along with their type.
value all     : returns the current values from all sensors.
value <name>  : return all data for sensor <name>.
refresh       : re-read device file and schedule a full search for devices.
reload        : re-read device file
search <link> : search for devices on <link> (<link> can also be blank or 'all')
help          : this message.
EOF
;
  return $output;
}


sub RecordRRDs {
  my @addresses;
  my ($address, $name, $type, $temperature, $minute, $iminute, $age, $rrdage);
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
      next if ($name eq $address);

      $type        = $data{$address}{type};
      next if ($type eq 'ds2401');

      $temperature = $data{$address}{temperature};
      $minute      = $data{$address}{minute};
      $minute      = $data{$address}{$type} if ( ($type eq 'ds2423') or ($type eq 'rain') or ($type =~ /^depth[0-9]+$/) );
      $iminute     = $data{$address}{iminute};

      $type        =~ s/^pressure[0-9]+$/pressure/;
      $type        =~ s/^depth[0-9]+$/depth/;
      $type        = 'temperature' if ( ($type eq 'temperature') || ($type eq 'tsense') || ($type eq 'ds1820') );

      $temperature = 'U' unless defined($temperature);
      $minute      = 'U' unless defined($minute);
      $iminute     = 'U' unless defined($iminute);
      if (defined($data{$address}{rrdage})) {
        $rrdage    = $data{$address}{rrdage};
      } else {
        $rrdage    = 0;
      }
      if (($updatetime - $rrdage) < 60) {
        logmsg 1, "ERROR last update to RRD for $name less than 60s ago (".($updatetime - $rrdage)."s)";
        next;
      }
      if (defined($data{$address}{age})) {
        $age       = time - $data{$address}{age};
        if ($age > 10) {
          $temperature = 'U';
          $minute      = 'U';
          $iminute     = 'U';
        }
      } else {
        $temperature = 'U';
        $minute      = 'U';
        $iminute     = 'U';
      }
      $rrdfile = "$RRDsDir/" . lc($name) . ".rrd";

      if (-w $rrdfile) {
        logmsg 5, "RRD: $updatetime:$temperature:$minute:$iminute";
        my $rrdcmd = "$updatetime:$temperature:$minute:$iminute";
        $rrdcmd = "$updatetime:$minute"		if ($type eq 'rain');
        $rrdcmd = "$updatetime:$temperature"	if ($type eq 'temperature');
        RRDs::update ($rrdfile, "$rrdcmd");
        $rrderror=RRDs::error;
        if ($rrderror) {
          logmsg 1, "ERROR while updating RRD file for $name: $rrderror";
        } else {
          $data{$address}{rrdage} = $updatetime;
        }
      } else {
        my $rrdtype = 'GAUGE';
        $rrdtype = 'COUNTER' if ($type eq 'rain');

        my @rrdcmd = ($rrdfile, "--step=60");

        @rrdcmd = (@rrdcmd, "DS:temperature:GAUGE:300:U:300")	unless ($type eq 'rain');
        @rrdcmd = (@rrdcmd, "DS:${type}:${rrdtype}:300:U:300")	unless ($type eq 'temperature');
        @rrdcmd = (@rrdcmd, "DS:icurrent:GAUGE:300:U:1024")	unless ( ($type eq 'rain') || ($type eq 'temperature') );

        @rrdcmd = (@rrdcmd, 
          "RRA:MIN:0.5:1:4000", "RRA:MIN:0.5:30:800", "RRA:MIN:0.5:120:800", "RRA:MIN:0.5:1440:800",
          "RRA:MAX:0.5:1:4000", "RRA:MAX:0.5:30:800", "RRA:MAX:0.5:120:800", "RRA:MAX:0.5:1440:800",
          "RRA:AVERAGE:0.5:1:4000", "RRA:AVERAGE:0.5:30:800", "RRA:AVERAGE:0.5:120:800", "RRA:AVERAGE:0.5:1440:800"
	);

        # Create RRD file
        logmsg (1, "Creating $rrdfile");
        RRDs::create (@rrdcmd);

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
  unless (open(DEVICES, '<', $DeviceFile)) {
    logmsg 1, "Can't open devices file ($DeviceFile): $!";
    return;
  }
  while (<DEVICES>) {
    chomp;
    s/\w*#.*//;
    next if (m/^\s*$/);
    if (m/^([0-9A-Fa-f]{16})\s+([A-Za-z0-9-_]+)\s+([A-Za-z0-9]+)\s*$/) {
      if (! defined($deviceDB{$1})) {
        $deviceDB{$1} = &share( {} );
      }
      $deviceDB{$1}{name} = $2;
      $deviceDB{$1}{type} = $3;

      if (! defined($data{$1})) {	# Check this seperately as assigning $deviceDB to $data otherwise if this is being run from a SIGHUP would cause existing values in $data to be lost
        $data{$1} = &share( {} );
      }
      $data{$1}{name} = $2;
      $data{$1}{type} = $3;
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
      unless ($socket) {
        $socket=undef;
      }
    } elsif ($main::LinkType eq 'LinkSerial') {
      $socket=Device::SerialPort->new($LinkDev);
      if ($socket) {
        $socket->baudrate(9600);
        $socket->databits(8);
        $socket->parity('none');
        $socket->stopbits(1);
        $socket->handshake("none");
        $socket->read_char_time(0);			# don't wait for each character
        $socket->read_const_time(100);			# 100 millisecond per unfulfilled "read" call
        $socket->write_settings || undef $socket;	# activate settings
      } else {
        close ($socket) if (defined($socket));
        $socket=undef;
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
  if ($SlowDown) {
    logmsg 5, "Connected to $LinkDev" if ($socket);
  } else {
    logmsg 1, "Connected to $LinkDev" if ($socket);
  }
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
        close ($main::socket) if (defined($main::socket));
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
        close ($main::socket) if (defined($main::socket));
        $main::socket = undef;
      }
    }
  }
  if (defined($returned)) {
    $returned =~ s/[?\r\n\0]*$//;
    $returned =~ s/^\?*//;
    $returned =~ s/\xff\xfa\x2c\x6a\x60\xff\xf0//;
  } else {
    $returned = '';
  }
  return $returned;
}

sub Reset {
  my $returned = LinkData("r\n");			# issue a 1-wire reset
  if ( (! CheckData($returned)) or ($returned eq '') ) {
    logmsg 3, "Reset on $main::LinkDev returned '$returned' (expected 'P' or 'N')";
    return 0;
  }
  return 1;
}

sub CheckData {
  my $returned = shift;
  if (defined($returned)) {
    if ($returned eq 'N') {
      logmsg 2, "$main::LinkDev reported that it has no devices. Scheduling a search." unless ($LinkDevData{$main::LinkDev}{SearchNow});
      $LinkDevData{$main::LinkDev}{SearchNow} = 1;
      return 1;		# This means that there aren't any devices on the bus which is not a data error
    }
    if ($returned eq 'S') {
      logmsg 2, "$main::LinkDev reported a short on the bus.";
      return 0;
    }
    if ($returned eq 'E') {
      logmsg 2, "$main::LinkDev reported an error processing the command.";
      return 0;
    }
    if ($returned eq '') {
      logmsg 1, "$main::LinkDev returned no data.";
      return 0;
    }
    if ($returned eq 'P') {
      return 1;		# This should only be after a reset but any other time the returned data will be checked outside this subroutine anyway
    }
  } else {
    return 0;
  }
  return 1;
}

sub QueryMSType {
  my $address = shift;
  my $returned;

  my $name = $data{$address}{name};

  my $retry = 0;

  while (1) {
    $retry++;
    if ($retry > 5) {
      logmsg 1, "Failed to read type of $main::LinkDev:$name.";
      return 0;
    }

    next if (! Reset());
    # byte mode, match rom, address, recall page 03 to scratch
    $returned = LinkData("b55${address}B803\n");
    next if (! CheckData($returned));
    if ($returned ne "55${address}B803") {
      logmsg 3, "ERROR: Sent b55${address}B803 command; got: $returned";
      next;
    }

    next if (! Reset());
    # byte mode, match rom, address, read scratch pad for memory page 03
    $returned = LinkData("b55${address}BE03FFFFFFFFFFFFFFFFFF\n");

    next if (! CheckData($returned));
    if ( (length($returned) != 40) || (! ($returned =~ s/^55${address}BE03([A-F0-9]{18})$/$1/)) ) {
      logmsg 3, "ERROR: Query of MS type for $main::LinkDev:$name returned: $returned";
      next;
    }
    if ( $returned =~ m/^F{18}$/ ) {
      logmsg 4, "ERROR: Got only F's on query of MS type for $main::LinkDev:$name.";
      next;
    }
    if (! CRC($returned) ) {
      logmsg 1, "CRC error on query of MS type for $main::LinkDev:$name";
      next;
    }
    $returned =~ s/[0-9A-F]{16}$//;		# we only need the first byte (2 chars)

    if ( defined($mstype{$returned}) ) {
      $data{$address}{mstype} = $mstype{$returned};
    } else {
      $data{$address}{mstype} = 'unknown';
    }
    if ($data{$address}{type} eq 'query') {
      $data{$address}{type} = $data{$address}{mstype};
      logmsg 2, "$name found to be type $returned (" . $data{$address}{mstype} . ")";
    }
    return 1;
  }
}

sub ChangeMSType {
  my $address = shift;
  my $returned;

  my $name = $data{$address}{name};

  my $type = '';
  foreach (keys(%mstype)) {
    $type = $_ if ($mstype{$_} eq $data{$address}{type});
  }
  if ($type) {
    logmsg 2, "Attempting to change $name type to ".$data{$address}{type}.".";

    my $retry = 0;

    while (1) {
      $retry++;
      if ($retry > 5) {
        logmsg 1, "Failed to change $main::LinkDev:$name type.";
        return 0;
      }

      next if (! Reset());

      $returned = LinkData("b55${address}4E03${type}\n");	# byte mode, match rom, address, write scratch 4E, register 03, value $type
      next if (! CheckData($returned));
      sleep 0.01;						# wait 10ms
      next if (! Reset());

      $returned = LinkData("b55${address}BE03FF\n");		# byte mode, match rom, address, read scratch BE, register 03
      next if (! CheckData($returned));
      next if ($returned ne "55${address}BE03${type}");
      sleep 0.01;						# wait 10ms
      next if (! Reset());
      $returned = LinkData("b55${address}4803\n");		# byte mode, match rom, address, copy scratch 48, register 03
      next if (! CheckData($returned));
      sleep 0.01;						# wait 10ms
      next if (! Reset());

      $returned = LinkData("b55${address}B803\n");		# byte mode, match rom, address, recall page 03 to scratch
      next if (! CheckData($returned));
      if ($returned ne "55${address}B803") {
        logmsg 3, "ERROR: Sent b55${address}B803 command; got: $returned";
        next;
      }
      sleep 0.01;						# wait 10ms
      next if (! Reset());

      logmsg 1, "Changed $name type from " . $data{$address}{mstype} . " to " . $data{$address}{type};
      return 1;
    }
  } else {
    logmsg 1, "Unkown type $data{$address}{type}. Cannot update type for $data{$address}{name}.";
    return 0;
  }
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
