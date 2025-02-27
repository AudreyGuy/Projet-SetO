# LAN3v2 Connection Guide via a terminal (PowerShell and Linux Command line)

This guide acts as an introduction to shell navigation and useful commands for the LAN3v2.

Note : This guide is only a reminder of the commands that can be incountered during a data collection session. Therefore, I highly recommend getting familiar with shell navigation, network protocols and bash scripting.

## Using Secure Shell (SSH) to connect to the LAN3v2

Secure Shell (SSH) is a network protocol that will enable us to access the LAN3v2 on another computer (without needing to plug in a screen).

Before starting, make sure that the device that will be used to access the LAN3v2 is connected to the hotspot (ssid : lan3v3 , key : lan3v2wifi)

Here is the default command : `ssh [command parameters (options)] username@ip-address`

Default username : sand \
Default IP address for the LAN3v2 : 192.168.5.1

In most cases, as the ssh port is different from the default port 22, we have to use the `-p` parameter to specify the desired port as so `-p 2023`.

Here is an example with user : sand, ip address : 192.168.5.1 and SSH port 2024 : `ssh -p 2024 sand@192.168.5.1`

Once entered in command line, enter user password, as you would on a login page.

Default LAN3v2 password : lumin007

## Useful commands when connected to LAN3v2

To change date and time, with root access, use the date command with the `-s` flag to specify a time and date (string format) as so : `sudo date -s "YYYY-MM-DD HH:mm:ss"`

To access the data files (stored in csv format), we use the `cd` command which is short for change directory, to ironicaly change directories. With that in mind, we can now access the data files : `cd /var/www/html/data/`

To list the current contents of the directory, use `ls` to list its contents.

To print on the screen the contents of a whole file, use the `cat` command with the filename specified : `cat filename.txt`

To get the last 10 lines of a file, use the `tail` command with the filename specified : `tail filename.txt`

To exit the terminal when finished on the LAN3v2 : `exit` or `ctrl+D`