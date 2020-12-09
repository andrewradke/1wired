# 1wired

A daemon, and related tools, for communicating with 1wire devices.

Since extended to talk to MQTT using the homie standard. https://github.com/homieiot/convention

## Getting Started

All the code is in Perl and tested under Debian and Ubuntu Linux. There should be no dependencies that limit the distriibution it runs on. It's possible it may run on Windows though it may need some minor patches.

### Prerequisites

Some extra libraries are required:

* libdevice-serialport-perl
* libdigest-crc-perl
* libproc-daemon-perl
* librrds-perl (for RRD support, will be removed in the future)
* libwww-perl (for InfluxDB support)

### Installing

There is currently no install script so a few things need creating manually.

A likely set of commands on a Debian like system is:
```
apt install libdigest-crc-perl libproc-daemon-perl librrds-perl libdevice-serialport-perl libwww-perl
cp onewired.service /lib/systemd/system/
chown root:root onewired.service /lib/systemd/system/
cp onewired /usr/local/sbin/onewired
chown root:root /usr/local/sbin/onewired
chmod 755 /usr/local/sbin/onewired
cp onewire-* /usr/local/bin/
chown root:root /usr/local/bin/onewire-*
chmod 755 /usr/local/bin/onewire-*
systemctl daemon-reload
mkdir /run/onewired
useradd -r -d /run/onewired -s /usr/sbin/nologin onewired
mkdir /etc/onewired
vi /etc/onewired/onewired.conf
vi /etc/onewired/devices
vi /etc/onewired/state
vi /etc/onewired/low_state
systemctl enable onewired.service
systemctl start onewired.service
```

## Future planned development

* Homie v3 support
* Possible rewrite in Python with native MQTT support

## Contributing

All contributions, ideas and criticism is welcome. :-)

## Authors

* **Andrew Radke**

## License

This project is licensed under the GNU General Public License Version 3 available at https://www.gnu.org/licenses/gpl-3.0.en.html

## Acknowledgements

* Marvin Roger for his work on The Homie Convention (https://github.com/homieiot/convention)
