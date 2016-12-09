#!/usr/bin/perl -w

use strict;

use Config;
$Config{useithreads} or die "Recompile Perl with threads to run this program.";

use threads;
use threads::shared;

use IO::Select;
use IO::Socket::INET;
use Device::SerialPort;
use Digest::CRC qw(crc16);

use FileHandle;
use IPC::Open2;
use POSIX ":sys_wait_h";

use Proc::Daemon;

my $version = '1.9.3';
$0 =~ s/.*\///;		# strip path from script name
my $script = $0;

### define variables shared by all threads
my %data : shared;
my %MastersData : shared;
my %deviceDB : shared;
my %addresses : shared;
my %agedata : shared;
my @threadNames : shared;


### Begin processing config file
my $ConfigFile = '/etc/1wired/1wired.conf';
$ConfigFile = shift if ($ARGV[0]);

my ($LogFile, $DeviceFile, $PidFile, $ListenPort, @LinkHubs, @LinkTHs, @MQTTSubs, @HomieSubs, $SleepTime, $RunAsDaemon, $SlowDown, $LogLevel, $UseRRDs, $RRDsDir, $AutoSearch, $ReSearchOnError, $UpdateMSType, $umask) :shared;

ParseConfigFile();

my %mstype = (
        '00' => 'temperature',
        '19' => 'humidity',
        '1A' => 'voltage',
        '1B' => 'light',
        '1C' => 'current',
        '21' => 'pressure150',
        '22' => 'depth15',
        '23' => 'pressure50',
        '24' => 'pressure100',
        '25' => 'pressure200',
        '26' => 'pressure72.5',
        '27' => 'pressure145',

        '30' => 'bme280',
        '31' => 'bmp280',
        '32' => 'dht22',
        '33' => 'bmp180',
        '34' => 'uvm30a',
        '35' => 'hcsr04',
    );

my %ArduinoSensors = (
	'bme280' => { 'sensor0' => 'temperature', 'sensor1' => 'pressure', 'sensor2' => 'humidity' },
	'bmp280' => { 'sensor0' => 'temperature', 'sensor1' => 'pressure'},
	'dht22'  => { 'sensor0' => 'temperature', 'sensor1' => 'humidity'},
	'bmp180' => { 'sensor0' => 'temperature', 'sensor1' => 'pressure'},
	'uvm30a' => { 'sensor1' => 'UVindex'},
	'hcsr04' => { 'sensor1' => 'distance'},
    );

if ($UseRRDs) {
  use RRDs;
  die "Can't write to RRD dir ($RRDsDir)" unless ( (-w $RRDsDir) && (-d $RRDsDir) );
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
sub logmsg {
  my $tid = threads->tid();
  my $level = shift;
  my $tname = 'main';

  $tname = $threadNames[$tid] if ( defined($threadNames[$tid]) );

  if ($level <= $LogLevel) {
    if ($RunAsDaemon) {
      print LOG scalar localtime, " $0\[$$\]: ($tname) @_\n";
    } else {
      print scalar localtime, " $0\[$$\]: ($tname) @_\n";
    }
  }
}
### End defining logmsg sub

### define non-shared variables
my $tmp;
my @tmp;


### Begin parsing device file
ParseDeviceFile();
### End parsing device file

if ($PidFile) {
  if (open(PID,  ">$PidFile")) {
    print PID $$;
    close PID;
  } else {
    logmsg 1, "Can't open pid file ($PidFile): $!";
  }
}

logmsg(1, "Starting $script $version");
if ( (sprintf '%.4d', umask) ne $umask ) {
  umask $umask;
}
logmsg(1, "Using umask of '$umask' for creation of all new RRDs and logfiles");

### Beginning of monitoring threads
my %threads : shared;
foreach my $LinkDev (@LinkHubs) {
  $MastersData{$LinkDev} = &share( {} );
  $addresses{$LinkDev} = &share( [] );
  $threads{$LinkDev} = shared_clone(threads->create(\&monitor_linkhub, $LinkDev));
  $threadNames[$threads{$LinkDev}->tid] = $LinkDev;
}
foreach my $LinkDev (@LinkTHs) {
  $MastersData{$LinkDev} = &share( {} );
  $addresses{$LinkDev} = &share( [] );
  $threads{$LinkDev} = shared_clone(threads->create(\&monitor_linkth, $LinkDev));
  $threadNames[$threads{$LinkDev}->tid] = $LinkDev;
}
foreach my $MQTTSub (@MQTTSubs) {
  $MastersData{$MQTTSub} = &share( {} );
  $addresses{$MQTTSub} = &share( [] );
  $threads{$MQTTSub} = shared_clone(threads->create(\&monitor_mqttsub, $MQTTSub));
  $threadNames[$threads{$MQTTSub}->tid] = $MQTTSub;
}
foreach my $HomieSub (@HomieSubs) {
  $MastersData{$HomieSub} = &share( {} );
  $addresses{$HomieSub} = &share( [] );
  $threads{$HomieSub} = shared_clone(threads->create(\&monitor_homiesub, $HomieSub));
  $threadNames[$threads{$HomieSub}->tid] = $HomieSub;
}

$threads{threadstatus} = shared_clone(threads->create(\&monitor_threadstatus));
$threadNames[$threads{threadstatus}->tid] = "Threads status";

#$threads{agedata} = shared_clone(threads->create(\&monitor_agedata));
#$threadNames[$threads{agedata}->tid] = "Age Data";

### End of monitoring thread


### Beginning of RRD recording
if ($UseRRDs) {
  $threads{RRDthread} = shared_clone(threads->create(\&RecordRRDs));
  $threadNames[$threads{RRDthread}->tid] = "RRD thread";
}
### End of RRD recording

$SIG{'HUP'} = sub {
  reload();
};

$SIG{'__DIE__'} = sub {
  logmsg(1,"We've just died: '$_[0]'");
  die $_[0];
};

$SIG{'INT'}  = \&cleanshutdown;
$SIG{'QUIT'} = \&cleanshutdown;
$SIG{'TERM'} = \&cleanshutdown;


### Beginning of listener
my $server_sock;
if ($ListenPort =~ m/^\d+$/) {
  logmsg 3, "Creating listener on port $ListenPort";
  $server_sock = new IO::Socket::INET (
					LocalPort => $ListenPort,
					Proto    => 'tcp',
					Listen   => 5,
					);
  die "Cannot create socket on port $ListenPort: $!" unless $server_sock;
} else {
  logmsg 3, "Creating listener on socket $ListenPort";
  unlink "$ListenPort";
  $server_sock = IO::Socket::UNIX->new(
					Local   => "$ListenPort",
					Type   => SOCK_STREAM,
					Listen => 5,
					);
  die "Cannot create socket on $ListenPort: $!" unless $server_sock;
  chmod 0770, "$ListenPort";
}

# Give all the threads a second to get going before starting the main loop
sleep 1;

logmsg 1, "Listening on socket $ListenPort";

# with all the threads started make sure $0 for the main thread is the script name
$0 = $script;

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
    #$listener = shared_clone(threads->create(\&report, $client));
    report($client);
    close ($client) if (defined($client));	# This socket is handled by the new thread
    $client=undef;
    logmsg 5, "Closed connection on socket $ListenPort";

    ### Clean up any threads that have completed.
    foreach my $thread (threads->list(threads::joinable)) {
      if ($thread->tid && !threads::equal($thread, threads->self)) {
        $thread->join;
        $thread = undef;
      }
    }

  }
}

cleanshutdown();
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
  my $tid = threads->tid();
  $0 = $LinkDev;
  our $socket;

  my $returned;
  our $select;

  $SIG{'KILL'} = sub {
    logmsg(3, "Stopping monitor_linkhub thread for $LinkDev.");
    Reset();
    $returned = LinkData("\n");		# Discard returned reset data
    $socket->close if (defined($socket));;
    threads->exit();
  };

  logmsg 1, "Monitoring LinkHub $LinkDev";
  our $MasterType = 'LinkHubE';
  if ($LinkDev =~ m/^\/dev\//) {
    $MasterType = 'LinkSerial'
  };

  my @addresses;
  my $count = 0;

  my ($temperature, $voltage, $icurrent);
  my $address;
  my ($type, $name);

  my $DoSearch = 1;
  my $LastDev;

  $MastersData{$LinkDev}{DataError} = 0;

  while(1) {
    if ( $MastersData{$LinkDev}{DataError} >= 5 ) {
      logmsg 1, "ERROR on $LinkDev: $MastersData{$LinkDev}{DataError} concurrent data errors, trying LinkHub reset.";
      $returned = LinkData('\!');		# Reset LinkHub (1-wire device not ethernet device)
      $MastersData{$LinkDev}{DataError} = 0;
      sleep 1;					# Give it a second to finish reseting
      $returned = LinkData("\n");		# Discard returned reset data
      $returned = LinkData("\n");
      $returned = LinkData("\n");
      $returned = LinkData("\n");
      $returned = LinkData("\n");
      $returned = LinkData("\n");
      $returned = LinkData("\n");
      $returned = LinkData("\n");
      $returned = LinkData("\n");
      $returned = LinkData("\n");
    }
    $count++;
    $agedata{$LinkDev} = time();

    $socket = LinkConnect($LinkDev) if ( (! defined($socket)) or (! $socket) );
    if ( (! defined($socket)) or (! $socket) ) {	# Failed to connect
      sleep 10;						# Wait for 10 seconds before retrying
      next;
    }

    $select = IO::Select->new($socket) if ($MasterType eq 'LinkHubE');

    $returned = LinkData(" \n");				# Discard any existing data, a space will return the link version, without it a timeout will occur waiting for some data
    # Do not check this data as we are not interested in it and after initial run should be empty and CheckData would return false

    ### Begin search for devices on LinkHub
    if ( ($MastersData{$LinkDev}{SearchNow}) || (($DoSearch) || (($AutoSearch) && ($count == 1))) ) {
      $MastersData{$LinkDev}{ds1820} = 0;
      $LastDev = 0;
      logmsg 3, "INFO: Searching for devices on $LinkDev";
      @addresses = ();

      unless (Reset()) {
        logmsg 1, "ERROR on $LinkDev: Initial reset failed during search. Closing connection and waiting 1 second before retrying.";
        close ($socket) if ( (defined($socket)) && ($MasterType eq 'LinkHubE') );
        $socket = undef;
        sleep 1;
        next;
      }

      $returned = LinkData("f\n");		# request first device ID
      if (! CheckData($returned)) {		# error or no data returned so we'll start again and keep trying
        logmsg 1, "ERROR on $LinkDev: Error or no data returned on search. Retrying in 1 second.";
        sleep 1;
        next;
      }

      unless ( defined($MastersData{$LinkDev}{channels}) ) {
        if ( $returned =~ m/,[1-5]$/ ) {
          $MastersData{$LinkDev}{channels} = 1;
          logmsg 2, "INFO: Channel reporting supported on $LinkDev.";
        } else {
          logmsg 3, "Checking for channel reporting support on $LinkDev";
          $returned = LinkData('\$');		# Toggles channel reporting
          if ( ( $returned ne '' ) && (! CheckData($returned) ) ) {
            logmsg 2, "ERROR on $LinkDev: Error toggling channel reporting.";
            next;
          }
          $returned = LinkData("f\n");		# request first device ID again
          if (! CheckData($returned)) {
            logmsg 2, "ERROR on $LinkDev: Error requesting first device.";
            next;
          }

          if ($returned =~ m/,[1-5]$/) {
            $MastersData{$LinkDev}{channels} = 1;
            logmsg 2, "INFO: Channel reporting enabled on $LinkDev.";
          } else {
            $MastersData{$LinkDev}{channels} = 0;
            logmsg 3, "INFO: Channel reporting NOT supported on $LinkDev.";
          }
        }
      }

      $returned =~ s/^[-+,]*//gs;
      if ($returned eq 'N') {			# no devices found so we'll start again and keep trying until something is found
        logmsg 2, "WARNING on $LinkDev: No devices found. Retrying in 1 second.";
        sleep 1;
        next;
      }

      # We have found at least one device so we can turn off the need to do more searches
      $DoSearch = 0 unless ($AutoSearch);
      $MastersData{$LinkDev}{SearchNow} = 0;	# Default to not needing a new search unless an error is found
      my $channel = '';
      if ($MastersData{$LinkDev}{channels}) {
        $returned =~ s/,([1-5])$//;
        $channel = $1;
      }
      if ($returned =~ m/^.$/) {		# Error but we'll keep searching in case there are more devices
        logmsg 1, "ERROR on $LinkDev: First device search returned '$returned'";
        $MastersData{$LinkDev}{SearchNow} = 1 if ($ReSearchOnError);
      } elsif ($returned eq '0000000000000000') {
        logmsg 1, "ERROR on $LinkDev: First device search returned '$returned'";
        $MastersData{$LinkDev}{SearchNow} = 1 if ($ReSearchOnError);
      } elsif ($returned =~ s/^(..)(..)(..)(..)(..)(..)(..)(..)$/$8$7$6$5$4$3$2$1/) {
        if (! CRCow($returned)) {
          logmsg 1, "ERROR on $LinkDev: CRC FAILED for device ID $returned";
          $MastersData{$LinkDev}{SearchNow} = 1 if ($ReSearchOnError);
        } else {
          #next if ( $returned =~ m/^01/);
          if (! defined($data{$returned})) {
            $data{$returned} = &share( {} );
          }
          if ( (defined($data{$returned}{name})) && ($data{$returned}{name} eq 'ignore') ) {
            logmsg 1, "INFO: Ignoring $returned on $LinkDev.";
          } else {
            if (grep( /^$returned$/,@addresses ) ) {
              logmsg 5, "INFO: $LinkDev:$returned already found.";
            } else {
              push (@addresses, $returned);
            }
          }
          $data{$returned}{master}  = $LinkDev;
          $data{$returned}{channel} = $channel;
          $data{$returned}{name} = $returned if (! defined($data{$returned}{name}));
          $data{$returned}{type} = 'unknown' if (! defined($data{$returned}{type}));
          if ( $returned =~ m/^01/) {
            # DS2401
            $data{$returned}{type} = 'ds2401' if ($data{$returned}{type} eq 'unknown');
            $returned =~ m/^..(.{12})..$/;                # 48bit serial number
            $data{$returned}{ds2401} = $1;
          }
          if ( ($returned =~ m/^10/) && ($data{$returned}{type} ne 'ds1820') ) {
            # DS1820 or DS18S20
            logmsg 3, "INFO: Setting device $returned type to 'ds1820'";
            $data{$returned}{type} = 'ds1820';
            $MastersData{$LinkDev}{ds1820} = 1;
          }
          if ( ($returned =~ m/^1D/) && (! ( ($data{$returned}{type} eq 'ds2423') or ($data{$returned}{type} eq 'rain') ) ) ) {
            # DS2423
            logmsg 3, "INFO: Setting device $returned type to 'ds2423'";
            $data{$returned}{type} = 'ds2423';
          }
          if ( $returned =~ m/^26/) {
            # DS2438
            $data{$returned}{type} = 'query' if ($data{$returned}{type} eq 'unknown');
          }
          if ( ($returned =~ m/^28/) && ($data{$returned}{type} ne 'ds18b20') ) {
            # DS18B20
            logmsg 3, "INFO: Setting device $returned type to 'ds18b20'";
            $data{$returned}{type} = 'ds18b20';
          }
          if ( $returned =~ m/^2A/) {
            # Arduino, can be lots of different things. Query unless that has already been done.
            $data{$returned}{arduino} = 'query'         unless ( defined($data{$returned}{arduino}) );
            $data{$returned}{uptime}  = 0               unless ( defined($data{$returned}{uptime}) );
            $data{$returned}{type}    = 'arduino-query' if     ( $data{$returned}{type} eq 'unknown');
          }
          logmsg 5, "INFO: Found $returned ($data{$returned}{name}) on $LinkDev";
        }
      } else {
        logmsg 1, "ERROR on $LinkDev: Bad data returned on search: $returned";
        $MastersData{$LinkDev}{SearchNow} = 1 if ($ReSearchOnError);
      }
      while (!($LastDev)) {
        $returned = LinkData("n\n");			# request next device ID
        if (! CheckData($returned)) {
          logmsg 2, "ERROR on $LinkDev: Error requesting next device.";
          next;
        }
        if ($returned =~ m/^.$/) {			# Error searching so we'll just move on in case there are more devices
          logmsg 1, "ERROR on $LinkDev: Device search returned '$returned'";
          $MastersData{$LinkDev}{SearchNow} = 1 if ($ReSearchOnError);
          next;
        }
        $LastDev = 1 if (!($returned =~ m/^\+,/));	# This is the last device
        $returned =~ s/^[-+,]*//gs;
        my $channel = '';
        if ($MastersData{$LinkDev}{channels}) {
          $returned =~ s/,([1-5])$//;
          $channel = $1;
        }
        if ($returned =~ s/^(..)(..)(..)(..)(..)(..)(..)(..)$/$8$7$6$5$4$3$2$1/) {
          if ($returned eq '0000000000000000') {
            logmsg 1, "ERROR on $LinkDev: Device search returned '$returned'";
            $MastersData{$LinkDev}{SearchNow} = 1 if ($ReSearchOnError);
            next;
          }
          if (! CRCow($returned)) {
            logmsg 1, "ERROR on $LinkDev: CRC failed for device ID $returned";
            $MastersData{$LinkDev}{SearchNow} = 1 if ($ReSearchOnError);
            next;
          }
          #next if ( $returned =~ m/^01/);
          if (! defined($data{$returned})) {
            $data{$returned} = &share( {} );
          }
          if ( (defined($data{$returned}{name})) && ($data{$returned}{name} eq 'ignore') ) {
            logmsg 1, "INFO: Ignoring $returned on $LinkDev.";
          } else {
            if (grep( /^$returned$/,@addresses ) ) {
              logmsg 5, "INFO: $LinkDev:$returned already found.";
              next;
            }
            push (@addresses, $returned);
          }
          $data{$returned}{master}  = $LinkDev;
          $data{$returned}{channel} = $channel;
          $data{$returned}{name} = $returned if (! defined($data{$returned}{name}));
          $data{$returned}{type} = 'unknown' if (! defined($data{$returned}{type}));
          if ( $returned =~ m/^01/) {
            # DS2401
            $data{$returned}{type} = 'ds2401' if ($data{$returned}{type} eq 'unknown');
            $returned =~ m/^..(.{12})..$/;                # 48bit serial number
            $data{$returned}{ds2401} = $1;
          }
          if ( ($returned =~ m/^10/) && ($data{$returned}{type} ne 'ds1820') ) {
            # DS1820 or DS18S20
            logmsg 3, "INFO: Setting device $returned type to 'ds1820'";
            $data{$returned}{type} = 'ds1820';
            $MastersData{$LinkDev}{ds1820} = 1;
          }
          if ( ($returned =~ m/^1D/) && (! ( ($data{$returned}{type} eq 'ds2423') or ($data{$returned}{type} eq 'rain') ) ) ) {
            # DS2423
            logmsg 3, "INFO: Setting device $returned type to 'ds2423'";
            $data{$returned}{type} = 'ds2423';
          }
          if ( $returned =~ m/^26/) {
            # DS2438
            $data{$returned}{type} = 'query' if ($data{$returned}{type} eq 'unknown');
          }
          if ( ($returned =~ m/^28/) && ($data{$returned}{type} ne 'ds18b20') ) {
            # DS18B20
            logmsg 3, "INFO: Setting device $returned type to 'ds18b20'";
            $data{$returned}{type} = 'ds18b20';
          }
          if ( $returned =~ m/^2A/) {
            # Arduino, can be lots of different things. Query unless that has already been done.
            $data{$returned}{arduino} = 'query'         unless ( defined($data{$returned}{arduino}) );
            $data{$returned}{uptime}  = 0               unless ( defined($data{$returned}{uptime}) );
            $data{$returned}{type}    = 'arduino-query' if     ( $data{$returned}{type} eq 'unknown');
          }
          logmsg 5, "INFO: Found $returned ($data{$returned}{name}) on $LinkDev";
        } else {
          logmsg 1, "ERROR on $LinkDev: Bad data returned on search: $returned";
          $MastersData{$LinkDev}{SearchNow} = 1 if ($ReSearchOnError);
        }
      }
      if ($MastersData{$LinkDev}{SearchNow}) {
        logmsg 1, "ERROR on $LinkDev: An error occured during the search. Another search has been requested.";
        $returned = LinkData("\n");		# Discard returned data
        $returned = LinkData("\n");
        $returned = LinkData("\n");
        $returned = LinkData("\n");
        $returned = LinkData("\n");
        $returned = LinkData("\n");
        $returned = LinkData("\n");
        $returned = LinkData("\n");
        $returned = LinkData("\n");
        $returned = LinkData("\n");
      }
      logmsg 5, "INFO: Found last device on $LinkDev.";

      @{$addresses{$LinkDev}} = @addresses;
      $MastersData{$LinkDev}{SearchTime} = time();
    }
    ### End search for devices on LinkHub

    ### Begin addressing ALL devices
    next if (! Reset());

    ### BEGIN setting all 2438's to read input voltage rather than supply voltage
    $returned = LinkData("bCC4E0071\n");	# byte mode, skip rom (address all devices), write scratch 4E, register 00, value 71
    if (! CheckData($returned)) {
      logmsg 2, "ERROR on $LinkDev: Error when requesting all 2438's write scratch for reading input voltage.";
      next;
    }
    sleep 0.01;					# wait 10ms
    next if (! Reset());
    $returned = LinkData("bCCBE00FFFFFFFFFFFFFFFFFF\n");	# byte mode, skip rom (address all devices), read scratch BE, register 00
    if (! CheckData($returned)) {
      logmsg 2, "ERROR on $LinkDev: Error when requesting all 2438's read scratch for reading input voltage.";
      next;
    }
    sleep 0.01;					# wait 10ms
    next if (! Reset());
    $returned = LinkData("bCC4800\n");		# byte mode, skip rom (address all devices), copy scratch 48, register 00
    if (! CheckData($returned)) {
      logmsg 2, "ERROR on $LinkDev: Error when requesting all 2438's copy scratch for reading input voltage.";
      next;
    }
    sleep 0.01;					# wait 10ms
    next if (! Reset());
    ### END setting all 2438's to read input voltage rather than supply voltage

    ### CHECK WHETHER USING PULL-UP MODE HERE IS A BUG
    $returned = LinkData("pCC44\n");		# byte mode in pull-up mode, skip rom (address all devices), convert T
    if (! CheckData($returned)) {
      logmsg 2, "ERROR on $LinkDev: Error requesting all 2438's convert temperature.";
      next;
    }
    sleep 0.1;					# wait 100ms for temperature conversion
    next if (! Reset());
    $returned = LinkData("bCCB4\n");		# byte mode, skip rom (address all devices), convert V
    if (! CheckData($returned)) {
      logmsg 2, "ERROR on $LinkDev: Error requesting all 2438's convert voltage.";
      next;
    }
    sleep 0.01;					# wait 10ms for voltage conversion
    ### End addressing ALL devices

    ### Begin query of devices on LinkHub
    foreach $address (@addresses) {
      last if (! defined($socket));
      next if ( (! defined($data{$address})) || (! defined($data{$address}{master})) );
      next if ($data{$address}{type} eq 'ds2401');
      if ($data{$address}{master} eq $LinkDev) {
        $data{$address}{name} = $address if (! defined($data{$address}{name}));
        $name = $data{$address}{name};

        # If this is a Multi Sensor then query it for it's type and update it if neccessary
        if ( ($address =~ m/^26/) && (! defined($data{$address}{mstype})) ) {
          QueryMSType($address);
          if ($data{$address}{type} ne $data{$address}{mstype}) {
            logmsg 1, "WARNING on $LinkDev:$name: multisensor type mismatch: config: $data{$address}{type}; sensor: $data{$address}{mstype}";
            ChangeMSType($address) if ($UpdateMSType);
          }
        }

        # If this is a Arduino then query it for it's type and update it if neccessary
        if ( ($address =~ m/^2A/) && ($data{$address}{arduino} eq 'query') ) {
          QueryArduinoType($address);
          if ($data{$address}{arduino} eq 'query') {
            logmsg 1, "WARNING on $LinkDev:$name: Could not determine sensor type of Arduino. Skipping device.";
            next;
          }
          if ($data{$address}{type} eq 'arduino-query') {
            $data{$address}{type} = "arduino-$data{$address}{arduino}";
            logmsg 2, "INFO: $name reports as Arduino type " . $data{$address}{arduino} . ".";
          } elsif ($data{$address}{type} ne "arduino-$data{$address}{arduino}") {
            logmsg 1, "WARNING on $LinkDev:$name: Arduino type mismatch: config: $data{$address}{type}; sensor: $data{$address}{arduino}. Ignoring config.";
            $data{$address}{type} = "arduino-$data{$address}{arduino}";
          }
        }

        if (! $data{$address}{type}) {
          logmsg 2, "WARNING on $LinkDev:$name is of an unknown multisensor type.";
          $data{$address}{type} = 'unknown';
        }
        $type = $data{$address}{type};

        logmsg 5, "INFO: querying $name ($address) as $type";

        $returned = query_device($socket,$select,$address,$LinkDev);

        my $retry = 0;
        while ($returned eq 'ERROR') {
          $retry++;
          if ($retry > 5) {
            logmsg 1, "ERROR on $LinkDev:$name: Didn't get valid data";
            last;
          }
          logmsg 6 - $retry, "ERROR on $LinkDev:$name: Didn't get valid data, retrying... (attempt $retry)";
          $returned = query_device($socket,$select,$address,$LinkDev);
        }

        if ($returned ne 'ERROR') {
          if ( ($data{$address}{type} eq 'ds2423') or ($data{$address}{type} eq 'rain') ) {
            my ($channelA, $channelB) = split(/\n/, $returned);
            chomp($channelA);
            $channelA =~ s/^.{64}(..)(..)(..)(..)0{8}.{4}$/$4$3$2$1/;	# 32bytes{64}, InputA{8}, 32x0bits, CRC16
            $channelA = hex $channelA;

            chomp($channelB);
            $channelB =~ s/^.{64}(..)(..)(..)(..)0{8}.{4}$/$4$3$2$1/;	# 32bytes{64}, InputB{8}, 32x0bits, CRC16
            $channelB = hex $channelB;

            $data{$address}{channelA} = $channelA;
            $data{$address}{channelB} = $channelB;

            if ($data{$address}{type} eq 'rain') {
              if ( (defined($data{$address}{rain})) and ($channelB > ($data{$address}{rain} + 10)) and ((time - $data{$address}{age}) < 60) ) {
                ### If the counter is more than 10 above the previous recorded value it is probably not correct
                ### If the current data is more than 60s old record it anyway
                ### The counter can go backwards if it wraps so only check for large increases
                logmsg 1, "WARNING on $LinkDev:$name: (query) Spurious rain reading ($channelB): keeping previous data ($data{$address}{rain})";
              } else {
               $data{$address}{rain} = $channelB;;
              }
            }
            $data{$address}{age} = time();

          } elsif ( ($type eq 'temperature') || ($type eq 'ds18b20') || ($type eq 'ds1820') ) {
            $temperature = $returned;
            $temperature =~ s/^(....).*$/$1/;
            #e.g.  a return value of 5701 represents 0x0157, or 343 in decimal.
            if ( $data{$address}{type} eq 'ds1820') {
              $temperature =~ m/^(..)(..)$/;
              $temperature = hex $1;
              $temperature = $temperature - 256 if ( $2 eq 'FF' );
              $temperature = $temperature/2;
            } else {
              $temperature =~ s/^(..)(..)$/$2$1/;
              $temperature = hex $temperature;
              $temperature = $temperature/16;
              $temperature -= 4096 if ($temperature > 4000);
            }
            $temperature = restrict_num_decimal_digits($temperature,1);
  
            if ( $temperature == 85 ) {
                ### If the temperature is 85C it is probably a default value and should be ignored
                logmsg 3, "WARNING on $LinkDev:$name: (query) exactly 85C is invalid (85C is the default for ds1820): discarding readings.";
                next;
            }
            $data{$address}{temperature} = $temperature;
            $data{$address}{age} = time();

          } elsif ($type =~ m/^arduino-/) {
            $data{$address}{raw} = $returned;

            # Check uptime first so we can skip it if it's just rebooted
            $returned =~ s/(....)..$//;	# last two bytes then CRC
            $voltage = $1;
            #e.g.  a return value of 5701 represents 0x0157, or 343 in decimal.
            $voltage =~ s/^(..)(..)$/$2$1/;
            $voltage = hex $voltage;
            $voltage = $voltage;
            $data{$address}{uptime} = $voltage if (! defined($data{$address}{uptime}) );
            if ( $data{$address}{uptime} > $voltage ) {
              logmsg 1, "WARNING on $LinkDev:$name: (query) Arduino rebooted, previous uptime $data{$address}{uptime} seconds.";
              $data{$address}{uptime} = $voltage;
              next;	# ignore first reading after a reboot
            }
            $data{$address}{uptime} = $voltage;

            $returned =~ s/^(....)(.*)$/$2/;
            $temperature = $1;
            if ( $temperature eq 'FFFF' ) {
              ### The temperature is at the default value and should be ignored
              logmsg 3, "WARNING on $LinkDev:$name: (query) temperature is at the default value: discarding readings." if (defined($ArduinoSensors{$data{$address}{arduino}}{sensor0}));
              $data{$address}{temperature} = 'NA';
            } else {
              #e.g.  a return value of 5701 represents 0x0157, or 343 in decimal.
              $temperature =~ s/^(..)(..)$/$2$1/;
              $temperature = hex $temperature;
              $temperature = $temperature/10;
              $temperature = restrict_num_decimal_digits($temperature,1);
              $data{$address}{temperature} = $temperature;
              $data{$address}{age} = time();
            }
            $returned =~ s/^(....)(.*)$/$2/;
            $voltage = $1;
            if ( $voltage eq 'FFFF' ) {
              ### The sensor is at the default value and should be ignored
              logmsg 3, "WARNING on $LinkDev:$name: (query) sensor1 is at the default value: discarding readings.";
              $data{$address}{sensor1} = 'NA';
            } else {
              #e.g.  a return value of 5701 represents 0x0157, or 343 in decimal.
              $voltage =~ s/^(..)(..)$/$2$1/;
              $voltage = hex $voltage;
              $voltage = $voltage/10;
              $voltage = restrict_num_decimal_digits($voltage,1);
              $data{$address}{sensor1} = $voltage;
              $data{$address}{age} = time();
            }
            $returned =~ s/^(....)(.*)$/$2/;
            $voltage = $1;
            if ( $voltage eq 'FFFF' ) {
              ### The sensor is at the default value and should be ignored
              logmsg 3, "WARNING on $LinkDev:$name: (query) sensor2 is at the default value: discarding readings.";
              $data{$address}{sensor2} = 'NA';
            } else {
              #e.g.  a return value of 5701 represents 0x0157, or 343 in decimal.
              $voltage =~ s/^(..)(..)$/$2$1/;
              $voltage = hex $voltage;
              $voltage = $voltage/10;
              $voltage = restrict_num_decimal_digits($voltage,1);
              $data{$address}{sensor2} = $voltage;
              $data{$address}{age} = time();
            }
            $returned =~ s/^(....)(.*)$/$2/;
            $voltage = $1;
            if ( $voltage eq 'FFFF' ) {
              ### The sensor is at the default value and should be ignored
              logmsg 3, "WARNING on $LinkDev:$name: (query) sensor3 is at the default value: discarding readings.";
              $data{$address}{sensor3} = 'NA';
            } else {
              #e.g.  a return value of 5701 represents 0x0157, or 343 in decimal.
              $voltage =~ s/^(..)(..)$/$2$1/;
              $voltage = hex $voltage;
              $voltage = $voltage/10;
              $voltage = restrict_num_decimal_digits($voltage,1);
              $data{$address}{sensor3} = $voltage;
              $data{$address}{age} = time();
            }
            $returned =~ s/^(....)(.*)$/$2/;
            $voltage = $1;
            if ( $voltage eq 'FFFF' ) {
              ### The sensor is at the default value and should be ignored
              logmsg 3, "WARNING on $LinkDev:$name: (query) sensor4 is at the default value: discarding readings.";
              $data{$address}{sensor4} = 'NA';
            } else {
              #e.g.  a return value of 5701 represents 0x0157, or 343 in decimal.
              $voltage =~ s/^(..)(..)$/$2$1/;
              $voltage = hex $voltage;
              $voltage = $voltage/10;
              $voltage = restrict_num_decimal_digits($voltage,1);
              $data{$address}{sensor4} = $voltage;
              $data{$address}{age} = time();
            }
            $data{$address}{age} = time();

          } else {
            $temperature = $returned;
            $voltage     = $returned;
            $icurrent    = $returned;
            $temperature =~ s/^(....).*$/$1/;
            $voltage =~ s/^....(....).*$/$1/;
            $icurrent =~ s/^........(....).*$/$1/;

            $icurrent =~ s/^(..)(..)$/$2$1/;
            $icurrent = hex $icurrent;
            #$icurrent -= 65535 if ($icurrent > 1023);			# This gives + or - current
            $icurrent = 65535 - $icurrent if ($icurrent > 1023);	# This gives only + current
            $data{$address}{icurrent} = $icurrent;

            #e.g.  a return value of 5701 represents 0x0157, or 343 in decimal.
            $temperature =~ s/^(..)(..)$/$2$1/;
            $temperature = hex $temperature;
            $temperature = $temperature>>3;
            $temperature = $temperature*0.03125;
            $temperature = restrict_num_decimal_digits($temperature,1);
  
            if ( $temperature == 0 ) {
                ### If the temperature is 0C it is probably a default value and should be ignored
                logmsg 3, "WARNING on $LinkDev:$name: (query) exactly 0C is probably not valid (0C is the default for ds2438): discarding readings.";
                next;
            }
            if ( ( defined($data{$address}{temperature}) ) && ( ($temperature > ($data{$address}{temperature} + 10)) || ($temperature < ($data{$address}{temperature} - 10)) and ((time - $data{$address}{age}) < 60) ) ) {
              ### If the temperature is more than 10 above or below the previous recorded value it is not correct and the voltage will also be wrong
              ### If the current data is more than 60s old record it anyway
              logmsg 1, "ERROR on $LinkDev:$name: (query) Spurious temperature ($temperature): keeping all previous data ($data{$address}{temperature})";
              next;
            }
            $data{$address}{temperature} = $temperature;

            $voltage =~ s/^(..)(..)$/$2$1/;
            $voltage = hex $voltage;
            $voltage = 0 if ($voltage == 1023);	# 1023 inidicates a short or 0V
            $voltage = $voltage*0.01;		# each unit is 10mV
            $data{$address}{raw} = $voltage;

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
            if ($type =~ m/^pressure([0-9.]+)$/) {
              if ( $voltage > 4.95 ) {
                ### 5V with a 1% tolerance. Returning this is beyond the pressure sensors capability and must therefore be some sort of electrical short
                logmsg 2, "WARNING on $LinkDev:$name: (query) ${voltage}V returned, this is beyond the pressure sensors capability: discarding readings. Possible electrical short.";
                next;
              }
              # 0.5V = 0psi
              # 4V for pressure range
              $voltage = ($voltage - 0.5) / 4 * $1;
            }
            if ( ($type eq 'temperature') || ($type eq 'ds18b20') || ($type eq 'ds1820') ) {
              $voltage = $temperature;
            }
            if ($type eq 'depth15') {
              $voltage = restrict_num_decimal_digits($voltage,2);
            } else {
              $voltage = restrict_num_decimal_digits($voltage,1);
            }
            $type        =~ s/^pressure[0-9.]+$/pressure/;
            $type        =~ s/^depth-?[0-9.]+$/depth/;
            $data{$address}{$type} = $voltage;

            my $thisminute = int(time()/60)*60;		# Round off to previous minute mark

            if (! defined($data{$address}{MinuteMax})) {
              $data{$address}{MinuteMax}	= $voltage;
              $data{$address}{TimeMax}		= time();
            } else {
              if ( $data{$address}{TimeMax} < $thisminute ) {
                $data{$address}{FiveMinuteMax}	= $data{$address}{FourMinuteMax};
                $data{$address}{FiveTimeMax}	= $data{$address}{FourTimeMax};
                $data{$address}{FourMinuteMax}	= $data{$address}{ThreeMinuteMax};
                $data{$address}{FourTimeMax}	= $data{$address}{ThreeTimeMax};
                $data{$address}{ThreeMinuteMax}	= $data{$address}{TwoMinuteMax};
                $data{$address}{ThreeTimeMax}	= $data{$address}{TwoTimeMax};
                $data{$address}{TwoMinuteMax}	= $data{$address}{OneMinuteMax};
                $data{$address}{TwoTimeMax}	= $data{$address}{OneTimeMax};
                $data{$address}{OneMinuteMax}	= $data{$address}{MinuteMax};
                $data{$address}{OneTimeMax}	= $data{$address}{TimeMax};
                $data{$address}{MinuteMax}	= $voltage;
                $data{$address}{TimeMax}	= time();
              } elsif ($voltage > $data{$address}{MinuteMax}) {
                $data{$address}{MinuteMax}	= $voltage;
                $data{$address}{TimeMax}	= time();
              }
            }

            if (! defined($data{$address}{MinuteMin})) {
              $data{$address}{MinuteMin}	= $voltage;
              $data{$address}{TimeMin}		= time();
            } else {
              if ( $data{$address}{TimeMin} < $thisminute ) {
                $data{$address}{FiveMinuteMin}	= $data{$address}{FourMinuteMin};
                $data{$address}{FiveTimeMin}	= $data{$address}{FourTimeMin};
                $data{$address}{FourMinuteMin}	= $data{$address}{ThreeMinuteMin};
                $data{$address}{FourTimeMin}	= $data{$address}{ThreeTimeMin};
                $data{$address}{ThreeMinuteMin}	= $data{$address}{TwoMinuteMin};
                $data{$address}{ThreeTimeMin}	= $data{$address}{TwoTimeMin};
                $data{$address}{TwoMinuteMin}	= $data{$address}{OneMinuteMin};
                $data{$address}{TwoTimeMin}	= $data{$address}{OneTimeMin};
                $data{$address}{OneMinuteMin}	= $data{$address}{MinuteMin};
                $data{$address}{OneTimeMin}	= $data{$address}{TimeMin};
                $data{$address}{MinuteMin}	= $voltage;
                $data{$address}{TimeMin}	= time();
              } elsif ($voltage < $data{$address}{MinuteMin}) {
                $data{$address}{MinuteMin}	= $voltage;
                $data{$address}{TimeMin}	= time();
              }
            }

            $data{$address}{age} = time();
          }
        }
      }
    }
    logmsg 5, "INFO: Finished querying devices on $LinkDev.";
    ### End query of devices on LinkHub

    $count = 0 if ($count > 1);
    if ($SlowDown) {		# This will slow down the rate of queries and close the socket allowing other connections to the 1wire master
      logmsg 5, "INFO: Finished loop for $LinkDev, closing socket.";
      close ($socket) if (defined($socket));
      $socket = undef;
      sleep $SlowDown;
    }
  }
}

sub monitor_linkth {
  my $LinkDev = shift;
  my $tid = threads->tid();
  $0 = $LinkDev;
  $SIG{'KILL'} = sub {
    logmsg(3, "Stopping monitor_linkth thread for $LinkDev.");
    threads->exit();
  };

  logmsg 1, "Monitoring LinkTH $LinkDev";
  our $MasterType = 'LinkSerial';

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
      sleep ($SleepTime + ($retry * 0.2));
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
        $data{$address}{master} = $LinkDev;
        if (! defined($data{$address}{type})) {
          if ( defined($mstype{$type}) ) {
            $data{$address}{type} = $mstype{$type};
          } else {
            $data{$address}{type} = 'unknown';
          }
        }

        $data{$address}{name} = $address if (! defined($data{$address}{name}));
        $name = $data{$address}{name};

        if ( ($address =~ m/^28/) && ($data{$address}{type} ne 'ds18b20') ) {
          logmsg 3, "Setting device $name type to 'ds18b20'";
          $data{$address}{type} = 'ds18b20';
        }
        logmsg 5, "Found $address ($name) on $LinkDev";

        $data{$address}{mstype} = $type;
        $data{$address}{raw} = 'NA';

        if ($data{$address}{type} eq 'depth15') {
          # 266.67 mV/psi; 0.5V ~= 0psi
          #$voltage = ($voltage - 0.5) * 3.75;
          $voltage = ($voltage - 0.43) * 3.891;
          # 1.417psi/metre
          $voltage = $voltage / 1.417;
        }
        if ($type =~ m/^pressure([0-9.]+)$/) {
          if ( $voltage > 4.95 ) {
            ### 5V with a 1% tolerance. Returning this is beyond the pressure sensors capability and must therefore be some sort of electrical short
            logmsg 2, "WARNING on $LinkDev:$name: (query) ${voltage}V returned, this is beyond the pressure sensors capability: discarding readings. Possible electrical short.";
            next;
          }
          # 0.5V = 0psi
          # 4V for pressure range
          $voltage = ($voltage - 0.5) / 4 * $1;
        }
        if ( ($data{$address}{type} eq 'temperature') || ($data{$address}{type} eq 'ds18b20') || ($data{$address}{type} eq 'ds1820') ) {
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

        my $thisminute = int(time()/60)*60;		# Round off to previous minute mark

        if (! defined($data{$address}{MinuteMax})) {
          $data{$address}{MinuteMax}		= $voltage;
          $data{$address}{TimeMax}		= time();
        } else {
          if ( $data{$address}{TimeMax} < $thisminute ) {
            $data{$address}{FiveMinuteMax}	= $data{$address}{FourMinuteMax};
            $data{$address}{FiveTimeMax}	= $data{$address}{FourTimeMax};
            $data{$address}{FourMinuteMax}	= $data{$address}{ThreeMinuteMax};
            $data{$address}{FourTimeMax}	= $data{$address}{ThreeTimeMax};
            $data{$address}{ThreeMinuteMax}	= $data{$address}{TwoMinuteMax};
            $data{$address}{ThreeTimeMax}	= $data{$address}{TwoTimeMax};
            $data{$address}{TwoMinuteMax}	= $data{$address}{OneMinuteMax};
            $data{$address}{TwoTimeMax}		= $data{$address}{OneTimeMax};
            $data{$address}{OneMinuteMax}	= $data{$address}{MinuteMax};
            $data{$address}{OneTimeMax}		= $data{$address}{TimeMax};
            $data{$address}{MinuteMax}		= $voltage;
            $data{$address}{TimeMax}		= time();
          } elsif ($voltage > $data{$address}{MinuteMax}) {
            $data{$address}{MinuteMax}		= $voltage;
            $data{$address}{TimeMax}		= time();
          }
        }

        if (! defined($data{$address}{MinuteMin})) {
          $data{$address}{MinuteMin}		= $voltage;
          $data{$address}{TimeMin}		= time();
        } else {
          if ( $data{$address}{TimeMin} < $thisminute ) {
            $data{$address}{FiveMinuteMin}	= $data{$address}{FourMinuteMin};
            $data{$address}{FiveTimeMin}	= $data{$address}{FourTimeMin};
            $data{$address}{FourMinuteMin}	= $data{$address}{ThreeMinuteMin};
            $data{$address}{FourTimeMin}	= $data{$address}{ThreeTimeMin};
            $data{$address}{ThreeMinuteMin}	= $data{$address}{TwoMinuteMin};
            $data{$address}{ThreeTimeMin}	= $data{$address}{TwoTimeMin};
            $data{$address}{TwoMinuteMin}	= $data{$address}{OneMinuteMin};
            $data{$address}{TwoTimeMin}		= $data{$address}{OneTimeMin};
            $data{$address}{OneMinuteMin}	= $data{$address}{MinuteMin};
            $data{$address}{OneTimeMin}		= $data{$address}{TimeMin};
            $data{$address}{MinuteMin}		= $voltage;
            $data{$address}{TimeMin}		= time();
          } elsif ($voltage < $data{$address}{MinuteMin}) {
            $data{$address}{MinuteMin}		= $voltage;
            $data{$address}{TimeMin}		= time();
          }
        }

        $data{$address}{age} = time();
      }
    }
    @{$addresses{$LinkDev}} = @addresses;
    $MastersData{$LinkDev}{SearchTime} = time();
  }
  #$socket->close;
  #$socket = undef;
}

sub monitor_mqttsub {
  my $MQTTSub = shift;
  my $tid = threads->tid();
  $0 = $MQTTSub;
  my $pid;

  $SIG{'KILL'} = sub {
    logmsg(3, "Stopping monitor_mqttsub thread for $MQTTSub.");
    kill 9, $pid;
    close(RETURNED);
    threads->exit();
  };

  my $channel;
  if (! ($MQTTSub =~ m/^([^\/]+)\/(.+)$/) ) {
    logmsg(1, "MQTT topic defined in config file ($MQTTSub) is not valid (.*/.*). Exiting.");
    threads->exit();
  }
  logmsg 1, "Monitoring MQTT $MQTTSub";
  our $MasterType = 'MQTT';

  $MastersData{$MQTTSub}{channels} = 1;		# MQTT always supports channel reporting

  my %localaddresses;

  my $returned;

  my ($temperature, $voltage, $timestamp);
  my $address;
  my ($type, $name);

  LOOP: while (1) {
    logmsg 4, "Connecting by $MasterType to $MQTTSub";
    while (! ($pid = open(RETURNED, "-|", "mosquitto_sub", "-v", "-t", "$MQTTSub") ) ) {
      logmsg 3, "Failed to connect to $MasterType $MQTTSub. Sleeping for 10 seconds before retrying";
      sleep 10;						# Wait for 10 seconds before retrying
    }

    while ($returned = <RETURNED>) {

      $agedata{$MQTTSub} = time();

      foreach (split(/\r?\n/, $returned)) {
        next if ($_ =~ / EOD$/);
        s/^([^ ]+)\/[^ ]+ //;
        my $topic = $1;
        @tmp = split(/,/, $_);				# $address $type,$temperatureC[,$value[,$timestamp]]
        if ($tmp[0] =~ s/ ([0-9A-F]{2})$//) {

          $address = $tmp[0];
          $type = $1;
          $temperature = $tmp[1];

          if (! defined($data{$address})) {
            $data{$address} = &share( {} );
          }

          $data{$address}{name} = $address if (! defined($data{$address}{name}));
          $name = $data{$address}{name};

          if (! ($temperature =~ m/^[0-9.]+$/) ) {
            logmsg 1, "ERROR on $MQTTSub:$name: returned temperature '$temperature' instead of a number.";
            next;
          }

          $tmp[2] = 0 if (! $tmp[2]);
          $voltage = $tmp[2];
          $voltage = 0 unless ($voltage =~ m/^\d+$/);

          $tmp[3] = time() if (! $tmp[3]);
          $timestamp = $tmp[3];
          $timestamp = time() unless ($timestamp =~ m/^\d+$/);

          $data{$address}{master}  = $MQTTSub;
          $data{$address}{channel} = $topic;
          if (! defined($data{$address}{type})) {
            if ( defined($mstype{$type}) ) {
              $data{$address}{type} = $mstype{$type};
            } else {
              $data{$address}{type} = 'unknown';
            }
          }

          if ( $data{$address}{type} =~ m/^depth-(\d+)$/ ) {
            $voltage = $1 - $voltage;
          }

          if (! $localaddresses{$address}) {
            $localaddresses{$address} = 1;
            logmsg 2, "Found $address ($name) on $MQTTSub";
            push (@{$addresses{$MQTTSub}}, $address);
            $MastersData{$MQTTSub}{SearchTime} = time();
          }

          if ( ($address =~ m/^28/) && ($data{$address}{type} ne 'ds18b20') ) {
            logmsg 3, "Setting device $name type to 'ds18b20'";
            $data{$address}{type} = 'ds18b20';
          }

          $data{$address}{mstype} = $type;
          $data{$address}{raw} = "$_";

          if ($data{$address}{type} eq 'depth15') {
            # 266.67 mV/psi; 0.5V ~= 0psi
            #$voltage = ($voltage - 0.5) * 3.75;
            $voltage = ($voltage - 0.43) * 3.891;
            # 1.417psi/metre
            $voltage = $voltage / 1.417;
          }
          if ($type =~ m/^pressure([0-9.]+)$/) {
            if ( $voltage > 4.95 ) {
              ### 5V with a 1% tolerance. Returning this is beyond the pressure sensors capability and must therefore be some sort of electrical short
              logmsg 2, "WARNING on $MQTTSub:$name: (query) ${voltage}V returned, this is beyond the pressure sensors capability: discarding readings. Possible electrical short.";
              next;
            }
            # 0.5V = 0psi
            # 4V for pressure range
            $voltage = ($voltage - 0.5) / 4 * $1;
          }
          if ( ($data{$address}{type} eq 'temperature') || ($data{$address}{type} eq 'ds18b20') || ($data{$address}{type} eq 'ds1820') ) {
            $voltage = $temperature;
          }
          $temperature = restrict_num_decimal_digits($temperature,3);

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

          my $thisminute = int(time()/60)*60;		# Round off to previous minute mark

          if (! defined($data{$address}{MinuteMax})) {
            $data{$address}{MinuteMax}		= $voltage;
            $data{$address}{TimeMax}		= time();
          } else {
            if ( $data{$address}{TimeMax} < $thisminute ) {
              $data{$address}{FiveMinuteMax}	= $data{$address}{FourMinuteMax};
              $data{$address}{FiveTimeMax}	= $data{$address}{FourTimeMax};
              $data{$address}{FourMinuteMax}	= $data{$address}{ThreeMinuteMax};
              $data{$address}{FourTimeMax}	= $data{$address}{ThreeTimeMax};
              $data{$address}{ThreeMinuteMax}	= $data{$address}{TwoMinuteMax};
              $data{$address}{ThreeTimeMax}	= $data{$address}{TwoTimeMax};
              $data{$address}{TwoMinuteMax}	= $data{$address}{OneMinuteMax};
              $data{$address}{TwoTimeMax}	= $data{$address}{OneTimeMax};
              $data{$address}{OneMinuteMax}	= $data{$address}{MinuteMax};
              $data{$address}{OneTimeMax}	= $data{$address}{TimeMax};
              $data{$address}{MinuteMax}	= $voltage;
              $data{$address}{TimeMax}		= time();
            } elsif ($voltage > $data{$address}{MinuteMax}) {
              $data{$address}{MinuteMax}	= $voltage;
              $data{$address}{TimeMax}		= time();
            }
          }

          if (! defined($data{$address}{MinuteMin})) {
            $data{$address}{MinuteMin}		= $voltage;
            $data{$address}{TimeMin}		= time();
          } else {
            if ( $data{$address}{TimeMin} < $thisminute ) {
              $data{$address}{FiveMinuteMin}	= $data{$address}{FourMinuteMin};
              $data{$address}{FiveTimeMin}	= $data{$address}{FourTimeMin};
              $data{$address}{FourMinuteMin}	= $data{$address}{ThreeMinuteMin};
              $data{$address}{FourTimeMin}	= $data{$address}{ThreeTimeMin};
              $data{$address}{ThreeMinuteMin}	= $data{$address}{TwoMinuteMin};
              $data{$address}{ThreeTimeMin}	= $data{$address}{TwoTimeMin};
              $data{$address}{TwoMinuteMin}	= $data{$address}{OneMinuteMin};
              $data{$address}{TwoTimeMin}	= $data{$address}{OneTimeMin};
              $data{$address}{OneMinuteMin}	= $data{$address}{MinuteMin};
              $data{$address}{OneTimeMin}	= $data{$address}{TimeMin};
              $data{$address}{MinuteMin}	= $voltage;
              $data{$address}{TimeMin}		= time();
            } elsif ($voltage < $data{$address}{MinuteMin}) {
              $data{$address}{MinuteMin}	= $voltage;
              $data{$address}{TimeMin}		= time();
            }
          }

          $data{$address}{age} = $timestamp;
        }
      }
    }
    close(RETURNED);
    logmsg 1, "ERROR on $MQTTSub: Connection closed. Sleeping 1 second before retrying.";
    sleep 1;
  }
}

sub monitor_homiesub {
  my $HomieSub = shift;
  my $tid = threads->tid();
  $0 = $HomieSub;
  my $pid;
  my ($HomieHost, $HomieTopic);

  $SIG{'KILL'} = sub {
    logmsg(3, "Stopping monitor_homiesub thread for $HomieSub.");
    kill 9, $pid;
    close(RETURNED);
    threads->exit();
  };

  if (! ($HomieSub =~ m/^([^:]+:|)([^\/]+\/.+)$/) ) {
    logmsg(1, "MQTT broker and/or Homie topic defined in config file ($HomieSub) is not valid (.*/.*). Exiting.");
    threads->exit();
  } else {
    $HomieHost = $1;
    $HomieTopic = $2;
    $HomieHost =~ s/:$//;
  }
  logmsg 1, "Monitoring Homie $HomieSub";
  our $MasterType = 'Homie';

  $MastersData{$HomieSub}{channels} = 0;		# channel reporting is not relevant to Homie

  my %localaddresses;

  my $returned;

  my ($property, $value);
  my $address;
  my ($type, $name);

  LOOP: while (1) {
    logmsg 4, "Connecting by $MasterType to $HomieSub";
    if ( (defined($HomieHost)) && ($HomieHost ne '') ) {
      while (! ($pid = open(RETURNED, "-|", "mosquitto_sub", "-v", "-h", "$HomieHost", "-t", "$HomieTopic") ) ) {
        logmsg 3, "Failed to connect to $MasterType $HomieSub. Sleeping for 10 seconds before retrying";
        sleep 10;						# Wait for 10 seconds before retrying
      }
    } else {
      while (! ($pid = open(RETURNED, "-|", "mosquitto_sub", "-v", "-t", "$HomieSub") ) ) {
        logmsg 3, "Failed to connect to $MasterType $HomieSub. Sleeping for 10 seconds before retrying";
        sleep 10;						# Wait for 10 seconds before retrying
      }
    }

    while ($returned = <RETURNED>) {

      $agedata{$HomieSub} = time();

      foreach my $value (split(/\r?\n/, $returned)) {
        if ($value =~ m!^([^/]+)/([^/]+) \(null\)$!) {		# address has been set to null so remove from list
          delete $localaddresses{$2};
          logmsg 2, "Removed $2 on $HomieSub";
          (@{$addresses{$HomieSub}}) = (keys(%localaddresses));
          $MastersData{$HomieSub}{SearchTime} = time();
          delete $data{$2}{type};
          delete $data{$2}{nodes};
          delete $data{$2}{fwname};
          delete $data{$2}{fwversion};
          delete $data{$2}{localip};
          delete $data{$2}{uptime};
          delete $data{$2}{signal};
          delete $data{$2}{online};
          delete $data{$2};
          next;
        }

        $value =~ s!^([^/]+)/([^/]+)/([^ ]+) !!;
        my $topic = $1;
        $address = $2;
        my $key = $3;
        logmsg 6, "topic: $1, address: $2, key: $3, value: $value";

        if (! defined($data{$address})) {
          $data{$address} = &share( {} );
        }
        if (! defined($data{$address}{node})) {
          $data{$address}{node} = &share( {} );
        }
        if ( (! defined($data{$address}{master}) ) || ( $data{$address}{master} ne $HomieSub ) ) {
          if ( (! $localaddresses{$address}) && ($value ne "(null)") ) {
            $localaddresses{$address} = 1;
            logmsg 2, "Found $address on $HomieSub";
            push (@{$addresses{$HomieSub}}, $address);
            $MastersData{$HomieSub}{SearchTime} = time();
            $data{$address}{type}	= 'homie';
            $data{$address}{nodes}	= '' unless (defined($data{$address}{nodes}));
            $data{$address}{fwname}	= '' unless (defined($data{$address}{fwname}));
            $data{$address}{fwversion}	= '' unless (defined($data{$address}{fwversion}));
            $data{$address}{localip}	= '' unless (defined($data{$address}{localip}));
            $data{$address}{uptime}	= '' unless (defined($data{$address}{uptime}));
            $data{$address}{signal}	= '' unless (defined($data{$address}{signal}));
            $data{$address}{online}	= '' unless (defined($data{$address}{online}));
          }
          $data{$address}{name} = $address;	# This can be overridden by a name value later
          $data{$address}{master} = $HomieSub;
        }

        $value = '' if ($value eq "(null)");

        if ( $key =~ s/^\$// ) {

          if ( $key eq "name" ) {
            # Nothing to do here as it has now been set already
          } elsif ( $key eq "online" ) {
            if ( ( ( defined($data{$address}{online}) ) && ( $data{$address}{online} ne $value ) ) || ( (! defined($data{$address}{online}) ) && ( $value ne 'true' ) ) ) {
              if ( $value eq "true" ) {
                logmsg 1, "INFO on $HomieSub:$data{$address}{name}: (query) Sensor now online.";
              } else {
                logmsg 1, "WARNING on $HomieSub:$data{$address}{name}: (query) Sensor offline.";
              }
            }
          } elsif ( $key eq "uptime" ) {
            if ( ( defined($data{$address}{uptime}) ) && ( $data{$address}{uptime} > $value ) ) {
              logmsg 1, "WARNING on $HomieSub:$data{$address}{name}: (query) Sensor rebooted, previous uptime $data{$address}{uptime} seconds.";
            }
          } elsif ( $key eq "signal" ) {
            # -93 dBm is fairly arbitrary but experience shows that ESP8266's are completely stable to -90 dBm and usually to at least -97 dBm
            logmsg 1, "WARNING on $HomieSub:$data{$address}{name}: (query) WiFi signal very weak: $value dBm" if ( $value < -93 );
          } elsif ( $key eq "nodes" ) {
            if ( ( $data{$address}{$key} ne '' ) && ( $data{$address}{$key} ne $value ) ) {
              logmsg 2, "INFO on $HomieSub:$data{$address}{name}: $key changed from $data{$address}{$key} to $value";
            }
            foreach my $node (split(/,/, $value)) {
              my ($nodeid, $property) = $node =~ m!(.*):(.*)!;
              if ((defined($deviceDB{$address})) && (defined($deviceDB{$address}{type})) && ( $deviceDB{$address}{type} =~ m/^depth-(\d+)$/ ) && ($nodeid eq 'distance') ) {
                $nodeid = 'depth';
                $property = 'm';
              }
              if (! defined($data{$address}{node}{$nodeid})) {
                $data{$address}{node}{$nodeid} = &share( {} );
              }
              $data{$address}{node}{$nodeid}{type}  = $property;
              $data{$address}{node}{$nodeid}{value} = 'NA';
            }
          } elsif ( ( defined($data{$address}{$key}) ) && ( $data{$address}{$key} ne '' ) && ( $data{$address}{$key} ne $value ) ) {
            logmsg 2, "INFO on $HomieSub:$data{$address}{name}: $key changed from $data{$address}{$key} to $value";
          }
          $data{$address}{$key} = $value;
        } else {

          my ($nodeid, $property) = $key =~ m!(.*)/(.*)!;

          if ((defined($deviceDB{$address})) && (defined($deviceDB{$address}{type})) && ( $deviceDB{$address}{type} =~ m/^depth-(\d+)$/ ) && ($nodeid eq 'distance') ) {
            $nodeid = 'depth';
            $value = ($1 - $value)/100;
          }

          # This should've been defined when receiving the $nodes property above but check in case that hasn't been recieved.
          if (! defined($data{$address}{node}{$nodeid})) {
            $data{$address}{node}{$nodeid} = &share( {} );
          }
          $data{$address}{node}{$nodeid}{type} = $property;
          $data{$address}{node}{$nodeid}{value} = $value;

          my $thisminute = int(time()/60)*60;		# Round off to previous minute mark

          if (! defined($data{$address}{node}{$nodeid}{MinuteMax})) {
            $data{$address}{node}{$nodeid}{MinuteMax}		= $value;
            $data{$address}{node}{$nodeid}{TimeMax}		= time();
          } else {
            if ( $data{$address}{node}{$nodeid}{TimeMax} < $thisminute ) {
              $data{$address}{node}{$nodeid}{FiveMinuteMax}	= $data{$address}{node}{$nodeid}{FourMinuteMax};
              $data{$address}{node}{$nodeid}{FiveTimeMax}	= $data{$address}{node}{$nodeid}{FourTimeMax};
              $data{$address}{node}{$nodeid}{FourMinuteMax}	= $data{$address}{node}{$nodeid}{ThreeMinuteMax};
              $data{$address}{node}{$nodeid}{FourTimeMax}	= $data{$address}{node}{$nodeid}{ThreeTimeMax};
              $data{$address}{node}{$nodeid}{ThreeMinuteMax}	= $data{$address}{node}{$nodeid}{TwoMinuteMax};
              $data{$address}{node}{$nodeid}{ThreeTimeMax}	= $data{$address}{node}{$nodeid}{TwoTimeMax};
              $data{$address}{node}{$nodeid}{TwoMinuteMax}	= $data{$address}{node}{$nodeid}{OneMinuteMax};
              $data{$address}{node}{$nodeid}{TwoTimeMax}	= $data{$address}{node}{$nodeid}{OneTimeMax};
              $data{$address}{node}{$nodeid}{OneMinuteMax}	= $data{$address}{node}{$nodeid}{MinuteMax};
              $data{$address}{node}{$nodeid}{OneTimeMax}	= $data{$address}{node}{$nodeid}{TimeMax};
              $data{$address}{node}{$nodeid}{MinuteMax}	= $value;
              $data{$address}{node}{$nodeid}{TimeMax}		= time();
            } elsif ($value > $data{$address}{node}{$nodeid}{MinuteMax}) {
              $data{$address}{node}{$nodeid}{MinuteMax}	= $value;
              $data{$address}{node}{$nodeid}{TimeMax}		= time();
            }
          }

          if (! defined($data{$address}{node}{$nodeid}{MinuteMin})) {
            $data{$address}{node}{$nodeid}{MinuteMin}		= $value;
            $data{$address}{node}{$nodeid}{TimeMin}		= time();
          } else {
            if ( $data{$address}{node}{$nodeid}{TimeMin} < $thisminute ) {
              $data{$address}{node}{$nodeid}{FiveMinuteMin}	= $data{$address}{node}{$nodeid}{FourMinuteMin};
              $data{$address}{node}{$nodeid}{FiveTimeMin}	= $data{$address}{node}{$nodeid}{FourTimeMin};
              $data{$address}{node}{$nodeid}{FourMinuteMin}	= $data{$address}{node}{$nodeid}{ThreeMinuteMin};
              $data{$address}{node}{$nodeid}{FourTimeMin}	= $data{$address}{node}{$nodeid}{ThreeTimeMin};
              $data{$address}{node}{$nodeid}{ThreeMinuteMin}	= $data{$address}{node}{$nodeid}{TwoMinuteMin};
              $data{$address}{node}{$nodeid}{ThreeTimeMin}	= $data{$address}{node}{$nodeid}{TwoTimeMin};
              $data{$address}{node}{$nodeid}{TwoMinuteMin}	= $data{$address}{node}{$nodeid}{OneMinuteMin};
              $data{$address}{node}{$nodeid}{TwoTimeMin}	= $data{$address}{node}{$nodeid}{OneTimeMin};
              $data{$address}{node}{$nodeid}{OneMinuteMin}	= $data{$address}{node}{$nodeid}{MinuteMin};
              $data{$address}{node}{$nodeid}{OneTimeMin}	= $data{$address}{node}{$nodeid}{TimeMin};
              $data{$address}{node}{$nodeid}{MinuteMin}	= $value;
              $data{$address}{node}{$nodeid}{TimeMin}		= time();
            } elsif ($value < $data{$address}{node}{$nodeid}{MinuteMin}) {
              $data{$address}{node}{$nodeid}{MinuteMin}	= $value;
              $data{$address}{node}{$nodeid}{TimeMin}		= time();
            }
          }

          $data{$address}{age} = time();
        }
      }
    }
    close(RETURNED);
    logmsg 1, "ERROR on $HomieSub: Connection closed. Sleeping 1 second before retrying.";
    sleep 1;
  }
}

sub query_device {
  my $socket  = shift;
  my $select  = shift;
  my $address = shift;
  my $LinkDev = shift;

  my $returned;

  eval {
    if ( ( $data{$address}{type} eq 'ds18b20') || ( $data{$address}{type} eq 'ds1820') ) {

      if ( $data{$address}{type} eq 'ds1820') {
        # Original ds1820 needs the bus pulled higher for longer for parasitic power
        # It can also loose the data after a Skip ROM so we address them inidividually here
        return 'ERROR' if (! Reset());
        $returned = LinkData("p55${address}44\n");	# byte mode in pull-up mode, match rom, address, convert T
        if (! CheckData($returned)) {
          logmsg 2, "Error requesting convert temperature on $LinkDev:$data{$address}{name}.";
          return 'ERROR';
        }
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
      if (! CheckData($returned)) {
        logmsg 2, "ERROR on $LinkDev:$data{$address}{name} requesting convert temperature.";
        return 'ERROR';
      }
      if ( (length($returned) != 38) || (! $returned =~ m/^55${address}BE[A-F0-9]{18}$/) ) {
        logmsg 3, "ERROR on $LinkDev:$data{$address}{name}: Sent b55${address}BEFFFFFFFFFFFFFFFFFF command; got: $returned";
        return 'ERROR';
      }
      if ( $returned =~ m/^55${address}BEF{18}$/ ) {
        logmsg 4, "ERROR on $LinkDev:$data{$address}{name}: Sent b55${address}BEFFFFFFFFFFFFFFFFFF command; got: $returned";
        return 'ERROR';
      }
      if ($returned =~ s/^55${address}BE//) {
        return $returned;
      } else {
        logmsg 2, "ERROR on $LinkDev:$data{$address}{name}: returned data not valid for $data{$address}{name}: $returned";
        return 'ERROR';
      }
    } elsif ( ($data{$address}{type} eq 'ds2423') or ($data{$address}{type} eq 'rain') ) {
      my $result;
      foreach my $page ('C0', 'E0') {	# channel A, B
        return 'ERROR' if (! Reset());
        $returned = LinkData("b55${address}A5${page}01".('F' x 84)."\n");	# byte mode, match rom, address, read memory + counter command, address 01{page}h
        if (! CheckData($returned)) {
          logmsg 2, "ERROR on $LinkDev:$data{$address}{name} requesting read memory and counter.";
          return 'ERROR';
        }
        if ( (length($returned) != 108) || (! ($returned =~ m/^55${address}A5${page}01[A-F0-9]{84}$/)) ) {
          logmsg 3, "ERROR on $LinkDev:$data{$address}{name}: Sent b55${address}A5${page}01 command; got: $returned";
          return 'ERROR';
        }
        if ($returned =~ m/^55${address}A5${page}01F{84}$/) {
          logmsg 3, "ERROR on $LinkDev:$data{$address}{name}: didn't return any data";
          return 'ERROR';
        }
        if ($returned =~ s/^55${address}A5${page}01//) {
          if (! CRC16("A5${page}01$returned") ) {		# initial pass CRC16 is calculated with the command byte, two memory address bytes, the contents of the data memory, the counter and the 0-bits
            logmsg 1, "ERROR on $LinkDev:$data{$address}{name}: CRC failed";
            return 'ERROR';
          }
          $result .= "$returned\n";
        } else {
          logmsg 2, "ERROR on $LinkDev:$data{$address}{name}: returned data not valid: $returned";
          return 'ERROR';
        }
      }
      return $result;

    } elsif ( $data{$address}{type} =~ m/^arduino-/) {
      return 'ERROR' if (! Reset());

      # BEFFFFFFFFFFFFFFFFFFFFFF
      # BEaabbccddeeffgghhiijjxx
      # aa: LSB for temperature
      # bb: MSB for temperature
      # cc: LSB for sensor1
      # dd: MSB for sensor1
      # ee: LSB for sensor2
      # ff: MSB for sensor2
      # gg: LSB for sensor3
      # hh: MSB for sensor3
      # ii: LSB for sensor4
      # jj: MSB for sensor4
      # kk: LSB for uptime
      # ll: MSB for uptime
      # xx: CRC

      $returned = LinkData("b55${address}BEFFFFFFFFFFFFFFFFFFFFFFFFFF\n");	# byte mode, match rom, address, read sensor values
      if (! CheckData($returned)) {
        logmsg 2, "ERROR on $LinkDev:$data{$address}{name} requesting read scratch pad for memory page 0.";
        return 'ERROR';
      }
      if ( (length($returned) != 46) || (! ($returned =~ m/^55${address}BE[A-F0-9]{26}$/)) ) {
        logmsg 3, "ERROR on $LinkDev:$data{$address}{name}: Sent b55${address}BEFFFFFFFFFFFFFFFFFFFFFFFFFF command; got: $returned";
        return 'ERROR';
      }
      if ( $returned =~ m/^55${address}BEF{26}$/ ) {
        logmsg 4, "ERROR on $LinkDev:$data{$address}{name}: Sent b55${address}BEFFFFFFFFFFFFFFFFFFFFFFFFFF command; got: $returned";
        return 'ERROR';
      }
      if ($returned =~ s/^55${address}BE//) {
        if (! CRCow($returned) ) {
          # Arduino's are going to occasionally have timing issues and therefore CRC errors.
          # As such log level 1 for these would create excessive uneccessary logs
          logmsg 3, "ERROR on $LinkDev:$data{$address}{name}: CRC failed";
          return 'ERROR';
        }
        return $returned;
      } else {
        logmsg 2, "ERROR on $data{$address}{name}: returned data not valid: $returned";
        return 'ERROR';
      }

    } else {
      return 'ERROR' if (! Reset());
      $returned = LinkData("b55${address}B800\n");	# byte mode, match rom, address, Recall Memory page 00 to scratch pad
      if (! CheckData($returned)) {
        logmsg 2, "ERROR on $LinkDev:$data{$address}{name} requesting recall memory page 0 to scratch pad.";
        return 'ERROR';
      }
      if ($returned ne "55${address}B800") {
        logmsg 3, "ERROR on $LinkDev:$data{$address}{name}: Sent b55${address}B800 command; got: $returned";
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
      if (! CheckData($returned)) {
        logmsg 2, "ERROR on $LinkDev:$data{$address}{name} requesting read scratch pad for memory page 0.";
        return 'ERROR';
      }
      if ( (length($returned) != 40) || (! ($returned =~ m/^55${address}BE00[A-F0-9]{18}$/)) ) {
        logmsg 3, "ERROR on $LinkDev:$data{$address}{name}: Sent b55${address}BE00FFFFFFFFFFFFFFFFFF command; got: $returned";
        return 'ERROR';
      }
      if ( $returned =~ m/^55${address}BE00F{18}$/ ) {
        logmsg 4, "ERROR on $LinkDev:$data{$address}{name}: Sent b55${address}BE00FFFFFFFFFFFFFFFFFF command; got: $returned";
        return 'ERROR';
      }
      if ($returned =~ s/^55${address}BE00//) {
        if (! CRCow($returned) ) {
          logmsg 1, "ERROR on $LinkDev:$data{$address}{name}: CRC failed";
          return 'ERROR';
        }
        $returned =~ s/^..//;
        return $returned;
      } else {
        logmsg 2, "ERROR on $data{$address}{name}: returned data not valid: $returned";
        return 'ERROR';
      }
    }
  } or do {
    logmsg 2, "ERROR on $LinkDev: died during eval, last data: '$returned'";
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
  my $channel;
  foreach my $LinkDev (keys(%addresses)) {
    (@addresses) = (@addresses, @{$addresses{$LinkDev}});
  }

  foreach my $address (sort(@addresses)) {
    $address =~ s/[\r\n\0]//g;		# Remove any CR, LF or NULL characters first
    $address =~ s/^[?!,]*//;		# THEN remove appropriate any leading characters

    $channel     = $data{$address}{channel};
    if ( defined($channel) ) {
      $channel = ":$channel" unless ($channel eq '');
    } else {
      $channel = '';
    }

    unless ($data{$address}{name} eq 'ignore') {
      if ((defined($deviceDB{$address})) && (defined($deviceDB{$address}{name})) ) {
        $output .= sprintf "Device found:   %-18s %16s %s%s\n", $deviceDB{$address}{name}, $address, $data{$address}{master}, $channel;
      } else {
        $output .= sprintf "UNKNOWN device:                    %16s %s%s\n", $address, $data{$address}{master}, $channel;
      }
    }
  }
  foreach $tmp (sort(keys(%deviceDB))) {
    if (! grep $_ eq $tmp, @addresses) {
      $output .= sprintf "NOT RESPONDING: %-18s %16s\n", $deviceDB{$tmp}{name}, $tmp unless ($deviceDB{$tmp}{name} eq 'ignore');
    }
  }
  $output .= "     ---------------------------------------------------------------------     \n";
  foreach my $LinkDev (sort(keys(%addresses))) {
    my $age = 'NA';
    if (defined($MastersData{$LinkDev}{SearchTime})) {
      $age = $MastersData{$LinkDev}{SearchTime};
      if ( $age =~ m/^\d+$/ ) {
        $age = time - $age;
        $output .= sprintf "%40s last searched %d seconds ago\n", $LinkDev, $age;
      } else {
        $output .= sprintf "%40s search unknown: %s\n", $LinkDev, $age;
      }
    }
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
  my $msg;
  my $output = '';

  $msg = "Re-reading config file.";
  logmsg(1, $msg);
  $output .= $msg."\n";

  my $OldLogFile = $LogFile;
  my $OldDeviceFile = $DeviceFile;
  my $OldPidFile = $PidFile;
  my $OldListenPort = $ListenPort;
  my @OldLinkHubs = @LinkHubs;
  my @OldLinkTHs = @LinkTHs;
  my @OldMQTTSubs = @MQTTSubs;
  my @OldHomieSubs = @HomieSubs;
  my $OldSleepTime = $SleepTime;
  my $OldRunAsDaemon = $RunAsDaemon;
  my $OldSlowDown = $SlowDown;
  my $OldLogLevel = $LogLevel;
  my $OldUseRRDs = $UseRRDs;
  my $OldRRDsDir = $RRDsDir;
  my $OldAutoSearch = $AutoSearch;
  my $OldReSearchOnError = $ReSearchOnError;
  my $OldUpdateMSType = $UpdateMSType;
  my $Oldumask = $umask;

  ParseConfigFile();

  ### Some values can't (or shouldn't) be changed while running
  if ( $OldPidFile ne $PidFile ) {
    $PidFile = $OldPidFile;
    $msg = "ERROR: PidFile cannot be changed when running.";
    logmsg(1, $msg);
    $output .= $msg."\n";
  }
  if ( $OldListenPort ne $ListenPort ) {
    $ListenPort = $OldListenPort;
    $msg = "ERROR: ListenPort cannot be changed when running.";
    logmsg(1, $msg);
    $output .= $msg."\n";
  }
  if ( $OldRunAsDaemon ne $RunAsDaemon ) {
    $RunAsDaemon = $OldRunAsDaemon;
    $msg = "ERROR: RunAsDaemon cannot be changed when running.";
    logmsg(1, $msg);
    $output .= $msg."\n";
  }


  ### Some values will work automatically
  if ( $OldDeviceFile ne $DeviceFile ) {
    $msg = "DeviceFile changed from '$OldDeviceFile' to '$DeviceFile'";
    logmsg(1, $msg);
    $output .= $msg."\n";
  }
  if ( $OldSleepTime ne $SleepTime ) {
    $msg = "SleepTime changed from '$OldSleepTime' to '$SleepTime'";
    logmsg(1, $msg);
    $output .= $msg."\n";
  }
  if ( $OldSlowDown ne $SlowDown ) {
    $msg = "SlowDown changed from '$OldSlowDown' to '$SlowDown'";
    logmsg(1, $msg);
    $output .= $msg."\n";
  }
  if ( $OldLogLevel ne $LogLevel ) {
    $msg = "LogLevel changed from '$OldLogLevel' to '$LogLevel'";
    logmsg(1, $msg);
    $output .= $msg."\n";
  }
  if ( $OldAutoSearch ne $AutoSearch ) {
    $msg = "AutoSearch changed from '$OldAutoSearch' to '$AutoSearch'";
    logmsg(1, $msg);
    $output .= $msg."\n";
  }
  if ( $OldReSearchOnError ne $ReSearchOnError ) {
    $msg = "ReSearchOnError changed from '$OldReSearchOnError' to '$ReSearchOnError'";
    logmsg(1, $msg);
    $output .= $msg."\n";
  }
  if ( $OldUpdateMSType ne $UpdateMSType ) {
    $msg = "UpdateMSType changed from '$OldUpdateMSType' to '$UpdateMSType' (only affects devices found after this)";
    logmsg(1, $msg);
    $output .= $msg."\n";
  }
  if ( $Oldumask ne $umask ) {
    if ($umask =~ m/^0[01][0-7][0-7]$/) {
      $msg = "umask changed from '$Oldumask' to '$umask' (only affects files created after this)";
      logmsg(1, $msg);
      $output .= $msg."\n";
      umask $umask;
    } else {
      $msg = "ERROR: umask value defined in config file '$umask' is not a valid number. Reverting to '$Oldumask'";
      logmsg(1, $msg);
      $output .= $msg."\n";
      $umask = $Oldumask;
    }
  }

  if ( $OldLogFile ne $LogFile ) {
    print STDERR "LogLevel has been set to 0. NO LOGGING WILL OCCUR!\n" if (! $LogLevel);
    if ($LogLevel && $RunAsDaemon) {
      $msg = "LogFile changed from '$OldLogFile' to '$LogFile'";
      logmsg(1, $msg);
      $output .= $msg."\n";
      close(LOG);
      open(LOG, ">>$LogFile") or die "Can't open log file ($LogFile): $!";	# Dieing here isn't a bad idea if the logfile can't be used
      my $oldfh = select LOG;
      $|=1;
      select($oldfh);
      $oldfh = undef;
      logmsg(1, $msg);		# Put this message into the new log file too
    }
  }
  if (! (join(', ', sort(@OldLinkHubs)) eq join(', ', sort(@LinkHubs))) ) {
    my %OldLinkHubs = map { $_ => 1 } @OldLinkHubs;
    my %LinkHubs = map { $_ => 1 } @LinkHubs;
    foreach my $Link (@OldLinkHubs) {
      if (! defined($LinkHubs{$Link}) ) {
        logmsg(1, "LinkHub removed: $Link");
        foreach (keys(%data)) {
          delete $data{$_} if ( $data{$_}{master} eq $Link );
        }
        delete $MastersData{$Link};
        delete $addresses{$Link};
        $threads{$Link}->kill('KILL')->detach();
        $threadNames[$threads{$Link}->tid] = undef;
        delete $threads{$Link};
      }
    }
    foreach my $Link (@LinkHubs) {
      if (! defined($OldLinkHubs{$Link}) ) {
        logmsg(1, "LinkHub added: $Link");
        $MastersData{$Link} = &share( {} );
        $addresses{$Link} = &share( [] );
        $threads{$Link} = shared_clone(threads->create(\&monitor_linkhub, $Link));
        $threadNames[$threads{$Link}->tid] = $Link;
      }
    }
  }
  if (! (join(', ', sort(@OldLinkTHs)) eq join(', ', sort(@LinkTHs))) ) {
    my %OldLinkTHs = map { $_ => 1 } @OldLinkTHs;
    my %LinkTHs = map { $_ => 1 } @LinkTHs;
    foreach my $Link (@OldLinkTHs) {
      if (! defined($LinkTHs{$Link}) ) {
        logmsg(1, "LinkTH removed: $Link");
        foreach (keys(%data)) {
          delete $data{$_} if ( $data{$_}{master} eq $Link );
        }
        delete $MastersData{$Link};
        delete $addresses{$Link};
        $threads{$Link}->kill('KILL')->detach();
        $threadNames[$threads{$Link}->tid] = undef;
        delete $threads{$Link};
      }
    }
    foreach my $Link (@LinkTHs) {
      if (! defined($OldLinkTHs{$Link}) ) {
        logmsg(1, "LinkTH added: $Link");
        $MastersData{$Link} = &share( {} );
        $addresses{$Link} = &share( [] );
        $threads{$Link} = shared_clone(threads->create(\&monitor_linkth, $Link));
        $threadNames[$threads{$Link}->tid] = $Link;
      }
    }
  }
  if (! (join(', ', sort(@OldMQTTSubs)) eq join(', ', sort(@MQTTSubs))) ) {
    my %OldMQTTSubs = map { $_ => 1 } @OldMQTTSubs;
    my %MQTTSubs = map { $_ => 1 } @MQTTSubs;
    foreach my $MQTT (@OldMQTTSubs) {
      if (! defined($MQTTSubs{$MQTT}) ) {
        logmsg(1, "MQTTMaster removed: $MQTT");
        foreach (keys(%data)) {
          delete $data{$_} if ( $data{$_}{master} eq $MQTT );
        }
        delete $MastersData{$MQTT};
        delete $addresses{$MQTT};
        $threads{$MQTT}->kill('KILL')->detach();
        $threadNames[$threads{$MQTT}->tid] = undef;
        delete $threads{$MQTT};
      }
    }
    foreach my $MQTT (@MQTTSubs) {
      if (! defined($OldMQTTSubs{$MQTT}) ) {
        logmsg(1, "MQTTMaster added: $MQTT");
        $MastersData{$MQTT} = &share( {} );
        $addresses{$MQTT} = &share( [] );
        $threads{$MQTT} = shared_clone(threads->create(\&monitor_mqttsub, $MQTT));
        $threadNames[$threads{$MQTT}->tid] = $MQTT;
      }
    }
  }
  if (! (join(', ', sort(@OldHomieSubs)) eq join(', ', sort(@HomieSubs))) ) {
    my %OldHomieSubs = map { $_ => 1 } @OldHomieSubs;
    my %HomieSubs = map { $_ => 1 } @HomieSubs;
    foreach my $Homie (@OldHomieSubs) {
      if (! defined($HomieSubs{$Homie}) ) {
        logmsg(1, "HomieMaster removed: $Homie");
        foreach (keys(%data)) {
          delete $data{$_} if ( $data{$_}{master} eq $Homie );
        }
        delete $MastersData{$Homie};
        delete $addresses{$Homie};
        $threads{$Homie}->kill('KILL')->detach();
        $threadNames[$threads{$Homie}->tid] = undef;
        delete $threads{$Homie};
      }
    }
    foreach my $Homie (@HomieSubs) {
      if (! defined($OldHomieSubs{$Homie}) ) {
        logmsg(1, "HomieMaster added: $Homie");
        $MastersData{$Homie} = &share( {} );
        $addresses{$Homie} = &share( [] );
        $threads{$Homie} = shared_clone(threads->create(\&monitor_homiesub, $Homie));
        $threadNames[$threads{$Homie}->tid] = $Homie;
      }
    }
  }
  if ( $OldRRDsDir ne $RRDsDir ) {			### changes automatically but needs to be checked
    # This should be done before UseRRDs as if that has been turned on it needs to also check
    # $RRDsDir is writable before enabling incase this value hasn't changed.
    $msg = "RRDsDir changed from '$OldRRDsDir' to '$RRDsDir'";
    logmsg(1, $msg);
    $output .= $msg."\n";
    unless ( (-w $RRDsDir) && (-d $RRDsDir) ) {
      $msg = "ERROR: Can't write to new RRD dir '$RRDsDir'. Reverting to '$OldRRDsDir'";
      logmsg(1, $msg);
      $output .= $msg."\n";
      $RRDsDir = $OldRRDsDir;
    }
  }
  if ( $OldUseRRDs ne $UseRRDs ) {
    $msg = "UseRRDs changed from '$OldUseRRDs' to '$UseRRDs'";
    logmsg(1, $msg);
    $output .= $msg."\n";
    if ($UseRRDs) {
      eval {
        require RRDs;
      } or do {
        $msg = "ERROR: RRDs perl module not available. Reverting UseRRDs to off.";
        logmsg(1, $msg);
        $output .= $msg."\n";
        $UseRRDs = $OldUseRRDs;
      };
      unless ( (-w $RRDsDir) && (-d $RRDsDir) ) {
        $msg = "ERROR: Can't write to RRD dir '$RRDsDir'. Reverting UseRRDs to off.";
        logmsg(1, $msg);
        $output .= $msg."\n";
        $UseRRDs = $OldUseRRDs;
      }
      if ($UseRRDs) {		# If the tests passed then UseRRDs will still be true
        $msg = "Starting RRD recording";
        logmsg(1, $msg);
        $output .= $msg."\n";
        $threads{RRDthread} = shared_clone(threads->create(\&RecordRRDs));
        $threadNames[$threads{RRDthread}->tid] = "RRD thread";
      }
    } else {
      $msg = "Stopping RRD recording";
      logmsg(1, $msg);
      $output .= $msg."\n";
      $threads{RRDthread}->kill('KILL')->detach();
      $threadNames[$threads{RRDthread}->tid] = undef;
      delete $threads{RRDthread};
    }
  }

  $msg = "Re-reading device file.";
  logmsg(1, $msg);
  $output .= $msg."\n";
  ParseDeviceFile();

  return $output;
}

sub search {
  my $LinkDev = shift;
  $LinkDev = '' if (!(defined($LinkDev)));
  $LinkDev =~ s/^ *//;
  my $output = '';
  if ( ($LinkDev eq 'all') || ($LinkDev eq '') ) {
    logmsg 1, "Scheduling search for devices on all Links.";
    $output .= "Scheduling search for devices on all Links.\n";
    foreach $LinkDev (@LinkHubs) {
      $MastersData{$LinkDev}{SearchNow} = 1;
    }
  } else {
    if (defined($MastersData{$LinkDev})) {
      logmsg 1, "Scheduling search for devices on $LinkDev.";
      $output .= "Scheduling search for devices on $LinkDev.\n";
      $MastersData{$LinkDev}{SearchNow} = 1;
    } else {
      logmsg 1, "'$LinkDev' is not a configured Link.";
      $output .= "'$LinkDev' is not a configured Link.\n";
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

  my ($address, $name, $type, $master, $channel, $age);
  foreach $address (@addresses) {
    next if ($data{$address}{name} eq 'ignore');
    eval {
      $name        = $data{$address}{name};
      $name        = "* $address" if (! defined($deviceDB{$address}));
      $type        = $data{$address}{type};
      $master      = $data{$address}{master};
      $channel     = $data{$address}{channel};
      $age         = 'NA';
      if ($data{$address}{age}) {
        $age       = time() - $data{$address}{age};
      }
      if ( defined($channel) ) {
        $channel = ":$channel" unless ($channel eq '');
      } else {
        $channel = '';
      }
      $type        =~ s/^pressure[0-9.]+$/pressure/;
      $type        =~ s/^depth[0-9.]+$/depth/;

      if ($type eq 'ds2401') {
        my $serial      =  $data{$address}{$type};
        $serial         = 'NA' unless defined($serial);
        $OutputData{$name} = sprintf "%-18s - serial number: %-18s                        %s%s\n",           $name, $serial, $master, $channel;

      } elsif ( ($type eq 'temperature') || ($type eq 'ds18b20') || ($type eq 'ds1820') ) {
        my $temperature = $data{$address}{temperature};
          if (defined($temperature)) {
            $temperature = sprintf "%5.1f", $temperature;
          } else {
            $temperature = ' NA  ';
          }
        $OutputData{$name} = sprintf "%-18s - temperature: %s                      (age: %3s s)  %s%s\n", $name, $temperature, $age, $master, $channel;

      } elsif ($type eq 'ds2423') {
        my $channelA    = $data{$address}{channelA};
        $channelA       = 'NA' unless defined($channelA);
        my $channelB    = $data{$address}{channelB};
        $channelB       = 'NA' unless defined($channelB);
        $OutputData{$name} = sprintf "%-18s - %9s A: %-10s   B: %-10s (age: %3s s)  %s%s\n",                 $name, $type, $channelA, $channelB, $age, $master, $channel;

      } elsif ($type eq 'rain') {
        my $rain        = $data{$address}{$type};
        $rain           = 'NA' unless defined($rain);
        $OutputData{$name} = sprintf "%-18s - %11s: %-10s                 (age: %3s s)  %s%s\n",             $name, $type, $rain, $age, $master, $channel;

      } elsif ($type eq 'homie') {
        $OutputData{$name} = sprintf "%-18s - ", $name;
        foreach my $nodeid (sort(keys(%{$data{$address}{node}}))) {
          $OutputData{$name} .= sprintf "%11s: ", $nodeid;
          if (defined($data{$address}{node}{$nodeid}{value}) ) {
            if ($nodeid eq 'counter' ) {
              $OutputData{$name} .= sprintf "%5i", $data{$address}{node}{$nodeid}{value};
            } else {
              $OutputData{$name} .= sprintf "%5.1f", $data{$address}{node}{$nodeid}{value};
            }
          } else {
            $OutputData{$name} .= ' NA  ';
          }
        }
        $OutputData{$name} .= '                    ' if ( keys %{$data{$address}{node}} == 1 );
        $OutputData{$name} .= sprintf "  (age: %3s s)  %s%s\n", $age, $master, $channel;

      } elsif ($type =~ m/^arduino-/) {
        my $arduino     = $data{$address}{arduino};
        $arduino        = 'unknown' unless defined($arduino);
        my $raw         = $data{$address}{raw};
        $raw            = 'NA' unless defined($raw);
        my $uptime      = $data{$address}{uptime};
        $uptime         = 'NA' unless defined($uptime);
        $OutputData{$name} = sprintf "%-18s - ", $name;
        if (defined($ArduinoSensors{$arduino}{sensor0})) {
          my $temperature = $data{$address}{temperature};
          if ( (defined($temperature)) && ($temperature ne 'NA') ) {
            $temperature = sprintf "%5.1f", $temperature;
          } else {
            $temperature = ' NA  ';
          }
          $OutputData{$name} .= sprintf "temperature: %s   ", $temperature;
        } else {
          $OutputData{$name} .= "                     ";
        }
        if (defined($ArduinoSensors{$arduino}{sensor1})) {
          my $sensor1     = $data{$address}{sensor1};
          if (defined($sensor1) ) {
            $sensor1 = sprintf "%5.1f", $sensor1;
          } else {
            $sensor1 = ' NA  ';
          }
          $OutputData{$name} .= sprintf "%10s: %s", $ArduinoSensors{$arduino}{sensor1}, $sensor1;
        }
        if (defined($ArduinoSensors{$arduino}{sensor2})) {
          my $sensor2     = $data{$address}{sensor2};
          if (defined($sensor2) ) {
            $sensor2 = sprintf "%5.1f", $sensor2;
          } else {
            $sensor2 = ' NA  ';
          }
          $OutputData{$name} .= sprintf "%10s: %s", $ArduinoSensors{$arduino}{sensor2}, $sensor2;
        }
        if (defined($ArduinoSensors{$arduino}{sensor3})) {
          my $sensor3     = $data{$address}{sensor3};
          if (defined($sensor3) ) {
            $sensor3 = sprintf "%5.1f", $sensor3;
          } else {
            $sensor3 = ' NA  ';
          }
          $OutputData{$name} .= sprintf "%10s: %s", $ArduinoSensors{$arduino}{sensor3}, $sensor3;
        }
        if (defined($ArduinoSensors{$arduino}{sensor4})) {
          my $sensor4     = $data{$address}{sensor4};
          if (defined($sensor4) ) {
            $sensor4 = sprintf "%5.1f", $sensor4;
          } else {
            $sensor4 = ' NA  ';
          }
          $OutputData{$name} .= sprintf "%10s: %s", $ArduinoSensors{$arduino}{sensor4}, $sensor4;
        }
        $OutputData{$name} .= sprintf "  (age: %3s s)  %s%s\n", $age, $master, $channel;

      } else {
        my $temperature = $data{$address}{temperature};
        if (defined($temperature) ) {
          $temperature = sprintf "%5.1f", $temperature;
        } else {
          $temperature = ' NA  ';
        }
        my $voltage     = $data{$address}{$type};
        if (defined($voltage) ) {
          $voltage = sprintf "%5.1f", $voltage;
        } else {
          $voltage = ' NA  ';
        }
        $OutputData{$name} = sprintf "%-18s - temperature: %s   %10s: %s  (age: %3s s)  %s%s\n",       $name, $temperature, $type, $voltage, $age, $master, $channel;

      }
    }
  }
  foreach $name (sort(keys(%OutputData))) {
    $output .= $OutputData{$name};
  }

  $output .= "     ---------------------------------------------------------------------     \n";
  foreach $LinkDev (sort(keys(%addresses))) {
    $age = 'NA';
    if (defined($MastersData{$LinkDev}{SearchTime})) {
      $age = $MastersData{$LinkDev}{SearchTime};
      if ( $age =~ m/^\d+$/ ) {
        $age = time - $age;
        $output .= sprintf "%40s last searched %d seconds ago\n", $LinkDev, $age;
      } else {
        $output .= sprintf "%40s search unknown: %s\n", $LinkDev, $age;
      }
    }
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

  my ($address, $name, $type, $master, $channel, $rawage, $age, $configtype);
  foreach $address (@addresses) {
    $name          = $data{$address}{name};
    if (($search eq lc($name)) || ($search eq lc($address))) {
      $type        = $data{$address}{type};
      $master      = $data{$address}{master};
      $channel     = $data{$address}{channel};
      $rawage      = $data{$address}{age};
      $rawage      = 'NA' unless defined($rawage);
      $age         = 'NA';
      if ($data{$address}{age}) {
        $age       = time() - $data{$address}{age};
      }
      if ( defined($channel) ) {
        $channel = ":$channel" unless ($channel eq '');
      } else {
        $channel = '';
      }
      if ((defined($deviceDB{$address})) && (defined($deviceDB{$address}{type})) ) {
        $configtype  = $deviceDB{$address}{type};
      } else {
        $configtype  = 'NA';
      }
      $type        =~ s/^pressure[0-9.]+$/pressure/;
      $type        =~ s/^depth[0-9.]+$/depth/;

      if ($type eq 'ds2401') {
        my $serial      =  $data{$address}{$type};
        $serial         = 'NA' unless defined($serial);
        $output        .= "name: $name\naddress: $address\ntype: $type\nserial number: $serial\nmaster: $master$channel\n";

      } elsif ( ($type eq 'temperature') || ($type eq 'ds18b20') || ($type eq 'ds1820') ) {
        my $temperature = $data{$address}{temperature};
        $temperature    = 'NA' unless defined($temperature);
        my $raw         = $data{$address}{raw};
        $raw            = 'NA' unless defined($raw);
        $output        .= "name: $name\naddress: $address\ntype: $type\ntemperature: $temperature\nage: $age\nRawAge: $rawage\nRawData: $raw\nmaster: $master$channel\nConfigType: $configtype\n";

      } elsif ($type eq 'ds2423') {
        my $channelA    = $data{$address}{channelA};
        $channelA       = 'NA' unless defined($channelA);
        my $channelB    = $data{$address}{channelB};
        $channelB       = 'NA' unless defined($channelB);
        $output        .= "name: $name\naddress: $address\ntype: $type\nchannelA: $channelA\nchannelB: $channelB\nage: $age\nRawAge: $rawage\nmaster: $master$channel\nConfigType: $configtype\n";

      } elsif ($type eq 'rain') {
        my $rain        = $data{$address}{$type};
        $rain           = 'NA' unless defined($rain);
        $output        .= "name: $name\naddress: $address\ntype: $type\n$type: $rain\nage: $age\nRawAge: $rawage\nmaster: $master$channel\nConfigType: $configtype\n";

      } elsif ($type eq 'homie') {
        $output        .= "name: $name\naddress: $address\ntype: $type\n";
        foreach my $nodeid (sort(keys(%{$data{$address}{node}}))) {
          $output      .= $nodeid . ": ";
          $output      .= $data{$address}{node}{$nodeid}{value} . "\n";
        }
        $output        .= "age: $age\nRawAge: $rawage\nmaster: $master\nConfigType: $configtype\n";
        $output        .= "nodes: $data{$address}{nodes}\nfwname: $data{$address}{fwname}\nfwversion: $data{$address}{fwversion}\nipaddress: $data{$address}{localip}\nuptime: $data{$address}{uptime}\nsignal: $data{$address}{signal} dBm\nonline: $data{$address}{online}\n";

      } elsif ($type =~ m/^arduino-/) {
        my $arduino     = $data{$address}{arduino};
        $arduino        = 'unknown' unless defined($arduino);
        my $raw         = $data{$address}{raw};
        $raw            = 'NA' unless defined($raw);
        my $uptime      = $data{$address}{uptime};
        $uptime         = 'NA' unless defined($uptime);
        my $temperature = $data{$address}{temperature};
        $temperature    = 'NA' unless defined($temperature);
        $output        .= "name: $name\naddress: $address\ntype: $type\ntemperature: $temperature\n";
        if (defined($ArduinoSensors{$arduino}{sensor1})) {
          my $sensor1     = $data{$address}{sensor1};
          $sensor1        = 'NA' unless defined($sensor1);
          $output        .= "$ArduinoSensors{$arduino}{sensor1}: $sensor1\n";
        }
        if (defined($ArduinoSensors{$arduino}{sensor2})) {
          my $sensor2     = $data{$address}{sensor2};
          $sensor2        = 'NA' unless defined($sensor2);
          $output        .= "$ArduinoSensors{$arduino}{sensor2}: $sensor2\n";
        }
        if (defined($ArduinoSensors{$arduino}{sensor3})) {
          my $sensor3     = $data{$address}{sensor3};
          $sensor3        = 'NA' unless defined($sensor3);
          $output        .= "$ArduinoSensors{$arduino}{sensor3}: $sensor3\n";
        }
        if (defined($ArduinoSensors{$arduino}{sensor4})) {
          my $sensor4     = $data{$address}{sensor4};
          $sensor4        = 'NA' unless defined($sensor4);
          $output        .= "$ArduinoSensors{$arduino}{sensor4}: $sensor4\n";
        }
        $output        .= "age: $age\nRawAge: $rawage\nRawData: $raw\nmaster: $master$channel\nConfigType: $configtype\nuptime: $uptime\n";
      } else {
        my $temperature = $data{$address}{temperature};
        my $mstype      = $data{$address}{mstype};
        my $voltage     = $data{$address}{$type};
        my $raw         = $data{$address}{raw};
        my $icurrent    = $data{$address}{icurrent};

        $temperature = 'NA' unless defined($temperature);
        $mstype      = 'NA' unless defined($mstype);
        $voltage     = 'NA' unless defined($voltage);
        $raw         = 'NA' unless defined($raw);
        $icurrent    = 'NA' unless defined($icurrent);

        my $MinuteMax      = $data{$address}{MinuteMax};
        $MinuteMax         = 'NA' unless defined($MinuteMax);
        my $TimeMax        = 'NA';
        if ($data{$address}{TimeMax}) {
          $TimeMax         = time() - $data{$address}{TimeMax};
        }

        my $FiveMinuteMax  = $data{$address}{FiveMinuteMax};
        my $FourMinuteMax  = $data{$address}{FourMinuteMax};
        my $ThreeMinuteMax = $data{$address}{ThreeMinuteMax};
        my $TwoMinuteMax   = $data{$address}{TwoMinuteMax};
        my $OneMinuteMax   = $data{$address}{OneMinuteMax};

        # If we don't have five minutes data DON'T give a value. It should be looked up somewhere else.
        if ( defined($FiveMinuteMax) ) {
          for ($FourMinuteMax, $ThreeMinuteMax, $TwoMinuteMax, $OneMinuteMax) {
            $FiveMinuteMax = $_ if ($_ > $FiveMinuteMax);
          }
        } else {
          $FiveMinuteMax   = 'NA';
        }

        my $MinuteMin      = $data{$address}{MinuteMin};
        $MinuteMin         = 'NA' unless defined($MinuteMin);
        my $TimeMin        = 'NA';
        if ($data{$address}{TimeMin}) {
          $TimeMin         = time() - $data{$address}{TimeMin};
        }

        my $FiveMinuteMin  = $data{$address}{FiveMinuteMin};
        my $FourMinuteMin  = $data{$address}{FourMinuteMin};
        my $ThreeMinuteMin = $data{$address}{ThreeMinuteMin};
        my $TwoMinuteMin   = $data{$address}{TwoMinuteMin};
        my $OneMinuteMin   = $data{$address}{OneMinuteMin};

        # If we don't have five minutes data DON'T give a value. It should be looked up somewhere else.
        if ( defined($FiveMinuteMin) ) {
          for ($FourMinuteMin, $ThreeMinuteMin, $TwoMinuteMin, $OneMinuteMin) {
            $FiveMinuteMin = $_ if ($_ < $FiveMinuteMin);
          }
        } else {
          $FiveMinuteMin   = 'NA';
        }

        $output .= "name: $name\naddress: $address\ntype: $type\ntemperature: $temperature\n$type: $voltage\n";
        $output .= "1MinuteMax: $MinuteMax\n1MinuteMaxAge: $TimeMax\n5MinuteMax: $FiveMinuteMax\n";
        $output .= "1MinuteMin: $MinuteMin\n1MinuteMinAge: $TimeMin\n5MinuteMin: $FiveMinuteMin\n";
        $output .= "age: $age\nRawAge: $rawage\nRawVoltage: $raw\nInstantaneousCurrent: $icurrent\n";
        $output .= "master: $master$channel\nConfigType: $configtype\nMStype: $mstype\n";

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
  my $tid = threads->tid();
  $0 = "RecordRRDs";
  logmsg(3, "Starting RecordRRDs thread.");
  $SIG{'KILL'} = sub {
    logmsg(3, "Stopping RecordRRDs thread.");
    threads->exit();
  };

  my @addresses;
  my ($address, $name, $type, $age, $rrdage);
  my ($rrdcmd, $rrdfile, $rrderror, $updatetime);
  while(1) {
    sleep (60 - (time() % 60));		# update once per minute and give some time for the first data
    @addresses = ();
    foreach my $LinkDev (keys(%addresses)) {
      (@addresses) = (@addresses, @{$addresses{$LinkDev}});
    }

    logmsg (3, "Updating RRDs");

    $updatetime = time();

    foreach $address (@addresses) {
      $name        = $data{$address}{name};
      $type        = $data{$address}{type};
      next if (! defined($deviceDB{$address}));
      if ( ($type eq 'homie') && ($data{$address}{nodes} eq '') ) {		# We don't know about any nodes yet so we won't be able to put any data into the RRD
        logmsg (2, "homie device $name doesn't have any defined nodes, skipping RRD update.");
        next;
      }

      next if ($type eq 'ds2401');

      $rrdage    = 0;
      if (defined($data{$address}{rrdage})) {
        $rrdage    = $data{$address}{rrdage};
      }
      if (($updatetime - $rrdage) < 50) {
        logmsg 1, "ERROR for $name: last update to RRD less than 50s ago (".($updatetime - $rrdage)."s)";
        next;
      }

      $type        =~ s/^pressure[0-9.]+$/pressure/;
      $type        =~ s/^depth[0-9.]+$/depth/;
      $type        = 'temperature' if ( ($type eq 'ds18b20') || ($type eq 'ds1820') );

      $rrdfile = "$RRDsDir/" . lc($name) . ".rrd";

      if (! -w $rrdfile) {
        my @rrdcmd = ($rrdfile, "--step=60");

        if ($type eq 'temperature') {
          @rrdcmd = (@rrdcmd, "DS:temperature:GAUGE:300:U:U");
        } elsif ($type eq 'rain') {
          @rrdcmd = (@rrdcmd, "DS:rain:COUNTER:300:U:30");
        } elsif ($type eq 'homie') {
          foreach my $nodeid (sort(keys(%{$data{$address}{node}}))) {
            if ($data{$address}{node}{$nodeid}{type} eq 'counter') {
              @rrdcmd = (@rrdcmd, "DS:$nodeid:COUNTER:300:U:U");
            } else {
              @rrdcmd = (@rrdcmd, "DS:$nodeid:GAUGE:300:U:U");
            }
          }
        } elsif ($type eq 'ds2423') {
          @rrdcmd = (@rrdcmd, "DS:channelA:COUNTER:300:U:U");
          @rrdcmd = (@rrdcmd, "DS:channelB:COUNTER:300:U:U");
        } elsif ($type =~ m/^arduino-/) {
          my $sensor1name = (defined($ArduinoSensors{$data{$address}{arduino}}{sensor1})) ? $ArduinoSensors{$data{$address}{arduino}}{sensor1} : 'sensor1';
          my $sensor2name = (defined($ArduinoSensors{$data{$address}{arduino}}{sensor2})) ? $ArduinoSensors{$data{$address}{arduino}}{sensor2} : 'sensor2';
          my $sensor3name = (defined($ArduinoSensors{$data{$address}{arduino}}{sensor3})) ? $ArduinoSensors{$data{$address}{arduino}}{sensor3} : 'sensor3';
          my $sensor4name = (defined($ArduinoSensors{$data{$address}{arduino}}{sensor4})) ? $ArduinoSensors{$data{$address}{arduino}}{sensor4} : 'sensor4';
          @rrdcmd = (@rrdcmd, "DS:temperature:GAUGE:300:U:120");
          if ($sensor1name eq 'pressure') {
            @rrdcmd = (@rrdcmd, "DS:$sensor1name:GAUGE:300:U:1200");
          } elsif ($sensor1name eq 'humidity') {
            @rrdcmd = (@rrdcmd, "DS:$sensor1name:GAUGE:300:U:100");
          } else {
            @rrdcmd = (@rrdcmd, "DS:$sensor1name:GAUGE:300:U:U");
          }
          if ($sensor2name eq 'pressure') {
            @rrdcmd = (@rrdcmd, "DS:$sensor2name:GAUGE:300:U:1200");
          } elsif ($sensor2name eq 'humidity') {
            @rrdcmd = (@rrdcmd, "DS:$sensor2name:GAUGE:300:U:100");
          } else {
            @rrdcmd = (@rrdcmd, "DS:$sensor2name:GAUGE:300:U:U");
          }
          @rrdcmd = (@rrdcmd, "DS:$sensor3name:GAUGE:300:U:U");
          @rrdcmd = (@rrdcmd, "DS:$sensor4name:GAUGE:300:U:U");
        } else {
          @rrdcmd = (@rrdcmd, "DS:temperature:GAUGE:300:U:300");
          @rrdcmd = (@rrdcmd, "DS:${type}:GAUGE:300:U:300");
        }

        @rrdcmd = (@rrdcmd, 
          "RRA:MIN:0.5:1:4000", "RRA:MIN:0.5:30:800", "RRA:MIN:0.5:120:800", "RRA:MIN:0.5:1440:800",
          "RRA:MAX:0.5:1:4000", "RRA:MAX:0.5:30:800", "RRA:MAX:0.5:120:800", "RRA:MAX:0.5:1440:800",
          "RRA:AVERAGE:0.5:1:4000", "RRA:AVERAGE:0.5:30:800", "RRA:AVERAGE:0.5:120:800", "RRA:AVERAGE:0.5:1440:800"
        );

        # Create RRD file
        logmsg (1, "Creating $rrdfile");
        RRDs::create (@rrdcmd);

        $rrderror=RRDs::error;
        logmsg (1, "ERROR while creating/updating RRD file for $name: $rrderror (@rrdcmd)") if $rrderror;
        next;	# can't update RRD file until next time
      }

      $rrdcmd = "$updatetime";
      if ($type eq 'temperature') {
        $rrdcmd .= ":" . ( (defined($data{$address}{temperature})) ? $data{$address}{temperature} : 'U' );
      } elsif ($type eq 'rain') {
        $rrdcmd .= ":" . ( (defined($data{$address}{rain})) ? $data{$address}{rain} : 'U' );
      } elsif ($type eq 'homie') {
        foreach my $nodeid (sort(keys(%{$data{$address}{node}}))) {
          if ($data{$address}{node}{$nodeid}{type} eq 'counter') {
            $rrdcmd .= ":" . ( ( (defined($data{$address}{node}{$nodeid}{value})) && ($data{$address}{node}{$nodeid}{value} ne 'NA')) ? sprintf "%0.0f", $data{$address}{node}{$nodeid}{value} : 'U' );
          } else {
            $rrdcmd .= ":" . ( ( (defined($data{$address}{node}{$nodeid}{value})) && ($data{$address}{node}{$nodeid}{value} ne 'NA')) ? $data{$address}{node}{$nodeid}{value} : 'U' );
          }
        }
      } elsif ($type eq 'ds2423') {
        $rrdcmd .= ":" . ( (defined($data{$address}{channelA})) ? $data{$address}{channelA} : 'U' );
        $rrdcmd .= ":" . ( (defined($data{$address}{channelB})) ? $data{$address}{channelB} : 'U' );
      } elsif ($type =~ m/^arduino-/) {
        $rrdcmd .= ":" . ( (defined($ArduinoSensors{$data{$address}{arduino}}{sensor0})) ? $data{$address}{temperature} : 'U' );
        $rrdcmd .= ":" . ( (defined($ArduinoSensors{$data{$address}{arduino}}{sensor1})) ? $data{$address}{sensor1} : 'U' );
        $rrdcmd .= ":" . ( (defined($ArduinoSensors{$data{$address}{arduino}}{sensor2})) ? $data{$address}{sensor2} : 'U' );
        $rrdcmd .= ":" . ( (defined($ArduinoSensors{$data{$address}{arduino}}{sensor3})) ? $data{$address}{sensor3} : 'U' );
        $rrdcmd .= ":" . ( (defined($ArduinoSensors{$data{$address}{arduino}}{sensor4})) ? $data{$address}{sensor4} : 'U' );
      } else {
        $rrdcmd .= ":" . ( (defined($data{$address}{temperature})) ? $data{$address}{temperature} : 'U' );
        if ( $data{$address}{TimeMax} < $updatetime) {		# data collection thread may have already started the new minute
          # Has NOT started yet
          $rrdcmd .= ":" . ( (defined($data{$address}{MinuteMax})) ? $data{$address}{MinuteMax} : 'U' );
        } else {
          # Has started already, use the last minutes result
          $rrdcmd .= ":" . ( (defined($data{$address}{OneMinuteMax})) ? $data{$address}{OneMinuteMax} : 'U' );
        }
        $rrdcmd .= ":" . $data{$address}{$type} if ($type eq 'depth');
      }

      if (defined($data{$address}{age})) {
        $age       = time - $data{$address}{age};
        if ($age > 60) {
          if ($type eq 'rain') {
            logmsg 1, "ERROR for $name: Age of data ($age) for RRD update is > 60s. Using last known value as a rain sensor.";
          } else {
            logmsg 1, "ERROR for $name: Age of data ($age) for RRD update is > 60s. Setting value to undefined.";
            $rrdcmd =~ s/:\d+/:U/g;
          }
        }
      } else {
        # Without age data storing RRD values would be unreliable
        $rrdcmd =~ s/:\d+/:U/g;
      }

      logmsg 4, "RRD for $name: $rrdcmd";
      RRDs::update ($rrdfile, "$rrdcmd");
      $rrderror=RRDs::error;
      if ($rrderror) {
        logmsg 1, "ERROR updating RRD file for $name: $rrderror";
      } else {
        $data{$address}{rrdage} = $updatetime;
      }
    }
    logmsg (3, "Finished updating RRDs");
    sleep 1;	# this gets us out of the first second of each minute
  }
}

sub monitor_threadstatus {
  my $tid = threads->tid();
  $0 = "threadstatus";
  logmsg(4, "Starting monitor_threadstatus thread.");
  $SIG{'KILL'} = sub {
    logmsg(4, "Stopping monitor_threadstatus thread.");
    threads->exit();
  };

  sleep 1;		# give the other threads a second to get started
  while(1) {
    foreach my $thread (keys(%threads)) {
      if (! $threads{$thread}->is_running() ) {
        my $error = $threads{$thread}->error();
        if (defined($error)) {
          chomp($error);
          my $tid = $threads{$thread}->tid();
          logmsg (1, "ERROR: Thread '$thread' terminated: '$error'");
        } else {
          logmsg (1, "ERROR: Thread '$thread' has exited without being cleaned up.");
        }
        delete $threads{$thread};
        $MastersData{$thread}{SearchTime} = "thread died";
      }
    }
    sleep 1;
  }
}


sub monitor_agedata {
  my $tid = threads->tid();
  $0 = "agedata";
  logmsg(4, "Starting monitor_agedata thread.");
  $SIG{'KILL'} = sub {
    logmsg(4, "Stopping monitor_agedata thread.");
    threads->exit();
  };

  my $age;
  sleep 1;		# give the other threads a second to get started
  while(1) {
    foreach my $LinkDev (@LinkHubs,@LinkTHs,@MQTTSubs,@HomieSubs) {
      $age = (time() - $agedata{$LinkDev});
      logmsg (1,"Age data for $LinkDev: $age seconds");
      if ($age > 15) {
        logmsg (1,"Age data (${age}s) for $LinkDev indicates it is stale.");
        $threads{$LinkDev}->kill('HUP');
      }
    }
    sleep 1;
  }
}


sub ParseConfigFile {
  $LogFile = '/var/log/1wired/1wired.log';
  $DeviceFile = '/etc/1wired/devices';
  $PidFile = '';
  $ListenPort = 2345;
  @LinkHubs = ();
  @LinkTHs = ();
  @MQTTSubs = ();
  @HomieSubs = ();
  $SleepTime = 0;
  $RunAsDaemon = 1;
  $SlowDown = 0;
  $LogLevel = 5;
  $UseRRDs = 0;
  $RRDsDir = '/var/1wired';
  $AutoSearch = 1;
  $ReSearchOnError = 1;
  $UpdateMSType = 0;
  $umask = 0002;

  my ($option, $value);
  open(CONFIG,  "<$ConfigFile") or die "Can't open config file ($ConfigFile): $!";
  while (<CONFIG>) {
    chomp;
    s!\w*//.*!!;
    next if (m/^#/);
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
      } elsif ($option eq 'MQTTSubs') {
        @MQTTSubs = split(/,\s*/, $value);
      } elsif ($option eq 'HomieSubs') {
        @HomieSubs = split(/,\s*/, $value);
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
          if ($value =~ m/^(0|false|no)$/i) {
            $AutoSearch = 0;
          } else {
            $AutoSearch = 1;
          }
        } else {
          print STDERR "AutoSearch value defined in config file ($value) is not valid. Using default ($AutoSearch).\n";
        }
      } elsif ($option eq 'UpdateMSType') {
        if ($value =~ m/^(1|0|true|false|yes|no)$/i) {
          if ($value =~ m/^(1|true|yes)$/i) {
            $UpdateMSType = 1;
          } else {
            $UpdateMSType = 0;
          }
        } else {
          print STDERR "UpdateMSType value defined in config file ($value) is not valid. Using default ($UpdateMSType).\n";
        }
      } elsif ($option eq 'ReSearchOnError') {
        if ($value =~ m/^(1|0|true|false|yes|no)$/i) {
          if ($value =~ m/^(0|false|no)$/i) {
            $ReSearchOnError = 0;
          } else {
            $ReSearchOnError = 1;
          }
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
          if ($value =~ m/^(1|true|yes)$/i) {
            $UseRRDs = 1;
          } else {
            $UseRRDs = 0;
          }
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
      } elsif ($option eq 'umask') {
        if ($value =~ m/^0[01][0-7][0-7]$/) {
          $umask = $value;
        } else {
          print STDERR "umask value defined in config file ($value) is not a valid number. Using default ($umask).\n";
        }
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
} ### END ParseConfigFile


sub ParseDeviceFile {
  my ($address, $name, $type);
  my $errors = 0;
  my %ListedDevices;
  unless (open(DEVICES, '<', $DeviceFile)) {
    logmsg 1, "ERROR: Can't open devices file ($DeviceFile): $!";
    return;
  }
  while (<DEVICES>) {
    chomp;
    s/\w*#.*//;
    next if (m/^\s*$/);
    if (m/^([0-9A-Za-z-]+)\s+([A-Za-z0-9-_]+)\s+([A-Za-z0-9.:-]+)\s*$/) {
      ($address, $name, $type) = ($1, $2, $3);
      if (! defined($deviceDB{$address})) {
        $deviceDB{$address} = &share( {} );
      }
      $deviceDB{$address}{name} = $name;
      $deviceDB{$address}{type} = $type;
      $ListedDevices{$address} = $name;

      if (! defined($data{$address})) {	# Check this seperately as assigning $deviceDB to $data otherwise if this is being run from a SIGHUP would cause existing values in $data to be lost
        $data{$address} = &share( {} );
      }
      if ( ( defined($data{$address}{name}) ) && ( $data{$address}{name} ne $name ) ) {
        logmsg 4, "Device $address name changed from '$data{$address}{name}' to '$name'";
      }
      $data{$address}{name} = $name;
      if ( ( defined($data{$address}{type}) ) && ( $data{$address}{type} ne $type ) ) {
        logmsg 4, "Device $address type changed from '$data{$address}{type}' to '$type'";
      }
      $data{$address}{type} = $type;
      logmsg 4, "Device configured: address: $address, name: $name, type: $type";
      if ( $type =~ m/^homie:/ ) {
        $data{$address}{type} = 'homie';
        $deviceDB{$address}{type} =~ s/^homie://;
      }
    } else {
      logmsg 1, "ERROR: Unrecognised line in devices file: '$_'";
      $errors = 1;
    }
  }
  close(DEVICES);
  logmsg 1, "ERROR: Couldn't parse devices file ($DeviceFile)." if ($errors);
  if (! keys(%ListedDevices)) {
    logmsg 1, "WARNING: No devices defined in $DeviceFile";
  }
  foreach ( keys(%deviceDB) ) {
    if (! defined($ListedDevices{$_}) ) {
      logmsg 2, "$_ ($deviceDB{$_}{name}) no longer listed in $DeviceFile";
      delete $deviceDB{$_};
      $data{$_}{name} = $_;
    }
  }
} ### END ParseDeviceFile


sub LinkConnect {
  my $LinkDev = shift;
  my $socket;
  my $retry = 0;
  logmsg 4, "Connecting to $main::MasterType $LinkDev";

  $socket = undef;
  while (! $socket) {
    if ($main::MasterType eq 'LinkHubE') {
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
    } elsif ($main::MasterType eq 'LinkSerial') {
      $socket=Device::SerialPort->new($LinkDev);
      if ($socket) {
        $socket->baudrate(9600)		|| undef $socket;
        $socket->databits(8)		|| undef $socket;
        $socket->parity('none')		|| undef $socket;
        $socket->stopbits(1)		|| undef $socket;
        $socket->handshake("none")	|| undef $socket;
        $socket->read_char_time(0);				# don't wait for each character
        $socket->read_const_time(100);				# 100 millisecond per unfulfilled "read" call
        $socket->write_settings		|| undef $socket;	# activate settings
      } else {
        #close ($socket) if (defined($socket));
        undef $socket;
      }
    }
    last if ($socket);
    $retry++;
    if ($retry > 5) {
      logmsg 1, "Couldn't connect to $LinkDev after 5 retries: $!";
      last;
    }

    logmsg 4, "Couldn't connect to $LinkDev $!, retrying... (attempt $retry)";
    sleep 1;
  }
  if ($SlowDown) {
    logmsg 5, "Connected to $LinkDev" if ($socket);
  } else {
    logmsg 3, "Connected to $LinkDev" if ($socket);
  }
  return $socket;
}

sub LinkData {
  my $send = shift;
  logmsg 6, "--> '$send'";
  my $returned = '';
  my $tmp;
  if ($main::MasterType eq 'LinkHubE') {
    if (defined($main::socket)) {
      $main::socket->send($send);
      sleep $SleepTime;
      if ($main::select->can_read(1)) {			# 1 second timeout waiting for any data
        while ($main::select->can_read(0.100)) {	# 100ms timeout waiting for extra data
          $main::socket->recv($tmp,128);
          $returned .= $tmp;
          last if ($returned =~ m/.[\r\n\0]/);
        }
      } else {
        logmsg 1, "ERROR on $main::LinkDev: Couldn't read data. Closing connection.";
        close ($main::socket) if (defined($main::socket));
        $main::socket = undef;
      }
    }
  } elsif ($main::MasterType eq 'LinkSerial') {
    if (defined($main::socket)) {
      $main::socket->write($send);
      sleep $SleepTime;
      ($tmp,$returned) = $main::socket->read(1023);
      if (!defined($returned)) {
        logmsg 1, "ERROR on $main::LinkDev: Couldn't read data. Closing connection.";
        close ($main::socket) if (defined($main::socket));
        $main::socket = undef;
      }
    }
  }
  if (defined($returned)) {
    logmsg 6, "<-- '$returned'";
    $returned =~ s/[?\r\n\0]*$//;
    $returned =~ s/^[?\r\n\0]*//;
    $returned =~ s/\xff\xfa\x2c\x6a\x60\xff\xf0//;
  } else {
    $returned = '';
  }
  return $returned;
}

sub Reset {
  my $returned = LinkData("r\n");			# issue a 1-wire reset
  if ( (! CheckData($returned)) or ($returned eq '') ) {
    logmsg 2, "Reset on $main::LinkDev returned '$returned' (expected 'P' or 'N')";
    return 0;
  }
  return 1;
}

sub CheckData {
  my $returned = shift;
  if (defined($returned)) {
    if ($returned eq 'N') {
      logmsg 2, "WARNING on $main::LinkDev: reported that it has no devices.";
      $MastersData{$main::LinkDev}{DataError} = 0;
      return 1;		# This means that there aren't any devices on the bus which is not a data error
    }
    if ($returned eq 'S') {
      logmsg 1, "ERROR on $main::LinkDev: reported a short on the bus.";
      $MastersData{$main::LinkDev}{DataError}++;
      return 0;
    }
    if ($returned eq 'E') {
      logmsg 1, "ERROR on $main::LinkDev: reported an error processing the command.";
      $MastersData{$main::LinkDev}{DataError}++;
      return 0;
    }
    if ($returned eq '') {
      logmsg 1, "ERROR on $main::LinkDev: no data returned.";
      $MastersData{$main::LinkDev}{DataError}++;
      return 0;
    }
    if ($returned eq 'P') {
      $MastersData{$main::LinkDev}{DataError} = 0;
      return 1;		# This should only be after a reset but any other time the returned data will be checked outside this subroutine anyway
    }
  } else {
    $MastersData{$main::LinkDev}{DataError}++;
    return 0;
  }
  $MastersData{$main::LinkDev}{DataError} = 0;
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
      logmsg 2, "ERROR on $main::LinkDev:$name: Failed to read multisensor type.";
      return 0;
    }

    next if (! Reset());
    # byte mode, match rom, address, recall memory page 03 to scratch pad
    $returned = LinkData("b55${address}B803\n");
    if (! CheckData($returned)) {
      logmsg 2, "ERROR on $main::LinkDev:$name: Error requesting recall memory page 03 to scratch pad.";
      next;
    }
    if ($returned ne "55${address}B803") {
      logmsg 3, "ERROR on $main::LinkDev:$name: Sent b55${address}B803 command; got: $returned";
      next;
    }

    next if (! Reset());
    # byte mode, match rom, address, read scratch pad for memory page 03
    $returned = LinkData("b55${address}BE03FFFFFFFFFFFFFFFFFF\n");
    if (! CheckData($returned)) {
      logmsg 2, "ERROR on $main::LinkDev:$name: Error requesting reading scratch pad for memory page 03.";
      next;
    }

    if ( (length($returned) != 40) || (! ($returned =~ s/^55${address}BE03([A-F0-9]{18})$/$1/)) ) {
      logmsg 3, "ERROR on $main::LinkDev:$name: Query of MS type returned: $returned";
      next;
    }
    if ( $returned =~ m/^F{18}$/ ) {
      logmsg 4, "ERROR on $main::LinkDev:$name: Got only F's on query of MS type.";
      next;
    }
    if (! CRCow($returned) ) {
      logmsg 1, "ERROR on $main::LinkDev:$name: CRC failed on query of MS type.";
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
      logmsg 2, "INFO: $name found to be type $returned (" . $data{$address}{mstype} . ")";
    }
    return 1;
  }
}

sub QueryArduinoType {
  my $address = shift;
  my $returned;

  my $name = $data{$address}{name};

  my $retry = 0;

  while (1) {
    $retry++;
    if ($retry > 5) {
      logmsg 2, "ERROR on $main::LinkDev:$name: Failed to read Arduino type.";
      return 0;
    }

    next if (! Reset());
    # byte mode, match rom, address, read memory page 03 (like a DS2438)
    $returned = LinkData("b55${address}BAFFFF\n");
    if (! CheckData($returned)) {
      logmsg 2, "ERROR on $main::LinkDev:$name: Error requesting reading scratch pad for memory page 03.";
      next;
    }

    if ( (length($returned) != 24) || (! ($returned =~ s/^55${address}BA([A-F0-9]{4})$/$1/)) ) {
      logmsg 3, "ERROR on $main::LinkDev:$name: Query of Arduino type returned: $returned";
      next;
    }
    if ( $returned =~ m/^F{4}$/ ) {
      logmsg 4, "ERROR on $main::LinkDev:$name: Got only F's on query of Arduino type.";
      next;
    }
    if (! CRCow($returned) ) {
      logmsg 1, "ERROR on $main::LinkDev:$name: CRC failed on query of Arduino type.";
      next;
    }
    $returned =~ s/^([0-9A-F]{2}).*/$1/;		# we only need the first byte (2 chars)

    if ( defined($mstype{$returned}) ) {
      $data{$address}{arduino} = $mstype{$returned};
    } else {
      $data{$address}{arduino} = 'unknown';
    }
    if ($data{$address}{type} eq 'query') {
      logmsg 2, "INFO: $name reports as Arduino type $returned (" . $data{$address}{arduino} . ")";
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
    logmsg 2, "INFO: Attempting to change $name multisensor type to ".$data{$address}{type}.".";

    my $retry = 0;

    while (1) {
      $retry++;
      if ($retry > 5) {
        logmsg 1, "ERROR on $main::LinkDev:$name: Failed to change multisensor type.";
        return 0;
      }

      next if (! Reset());

      $returned = LinkData("b55${address}4E03${type}\n");	# byte mode, match rom, address, write scratch 4E, register 03, value $type
      if (! CheckData($returned)) {
        logmsg 2, "ERROR on $main::LinkDev:$name: Error requesting writing scratch pad for memory page 03.";
        next;
      }
      sleep 0.01;						# wait 10ms
      next if (! Reset());

      $returned = LinkData("b55${address}BE03FF\n");		# byte mode, match rom, address, read scratch BE, register 03
      if (! CheckData($returned)) {
        logmsg 2, "ERROR on $main::LinkDev:$name: Error requesting reading scratch pad for memory page 03.";
        next;
      }
      next if ($returned ne "55${address}BE03${type}");
      sleep 0.01;						# wait 10ms
      next if (! Reset());
      $returned = LinkData("b55${address}4803\n");		# byte mode, match rom, address, copy scratch 48, register 03
      if (! CheckData($returned)) {
        logmsg 2, "ERROR on $main::LinkDev:$name: Error requesting copying scratch pad for memory page 03.";
        next;
      }
      sleep 0.01;						# wait 10ms
      next if (! Reset());

      $returned = LinkData("b55${address}B803\n");		# byte mode, match rom, address, recall memory page 03 to scratch pad
      if (! CheckData($returned)) {
        logmsg 2, "ERROR on $main::LinkDev:$name: Error requesting recalling memory page 03 to scratch pad.";
        next;
      }
      if ($returned ne "55${address}B803") {
        logmsg 3, "ERROR on $main::LinkDev:$name: Sent b55${address}B803 command; got: $returned";
        next;
      }
      sleep 0.01;						# wait 10ms
      next if (! Reset());

      logmsg 1, "INFO: Changed $name multisensor type from " . $data{$address}{mstype} . " to " . $data{$address}{type};
      return 1;
    }
  } else {
    logmsg 1, "ERROR for $name: Unkown multisensor type $data{$address}{type}. Cannot update.";
    return 0;
  }
}

sub CRCow {
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

sub CRC16 {
  my $data = shift;

  $data =~ s/(.{2})(.{2})$//;	# grab the last two bytes for the CRC and remove them from the data to be checked
  my $crc = "$2$1";		# reverse the bytes
  $crc = hex $crc;		# convert it to a number
  $crc = 65535 - $crc;		# invert the bits

  my $bytes = pack "H*", $data;	# converts the hex data into a long set of bits

  if ( $crc == crc16($bytes) ) {
    return 1;
  } else {
    return 0;
  }
}


sub cleanshutdown {
  logmsg(1, "SHUTDOWN: Shutting down.");
  foreach my $thread (keys(%threads)) {
    logmsg(3, "SHUTDOWN: Active thread $thread ".$threads{$thread}->tid() );
  }

  $threads{threadstatus}->kill('KILL')->join();	# Get rid of this first so it doesn't report uselessly
  delete $threads{threadstatus};

  if ($UseRRDs) {
    $threads{RRDthread}->kill('KILL')->detach();	# This could be in a long sleep so just detach
    delete $threads{RRDthread};
  }

  foreach my $thread (threads->list()) {
    $thread->kill('KILL');
  }
  sleep 1;

  while (keys(%threads)) {
    foreach my $thread (keys(%threads)) {
      if ($threads{$thread}->is_running() ) {
        logmsg(1, "SHUTDOWN: thread $thread ".$threads{$thread}->tid()." still running");
      } else {
        logmsg(1, "SHUTDOWN: thread $thread finished.");
        $threads{$thread}->join();
        delete $threads{$thread};
      }
    }
    sleep 1;
  }

  logmsg 3, "SHUTDOWN: Closing socket $ListenPort";
  close ($server_sock);
  if (! ($ListenPort =~ m/^\d+$/)) {
    logmsg(3, "SHUTDOWN: Removing socket on $ListenPort");
    unlink "$ListenPort";
  }
  if ($PidFile) {
    unlink $PidFile;
  }
  exit 0;
}

