grabOnce
===========

A tool to download files from your seedbox.

Why?
-----
Most people drop torrents on their seedbox and use the default filenames
for all the files in the torrents. They then retrieve them to a local machine,
and rename or reorganize them in some way. This may be done automatically
or by hand.

For example, we download an ISO of our favorite linux distribution: Ubuntu.
The file you get is named:

     Ubuntu-12.04-AMD64.iso
     
But you don't like dashes, so you rename the file to

     Ubuntu_12.04_AMD64.iso
     
If you download files from your seedbox using rsync between the two
directories, it will re download the original file because it cannot find it.

The solution to this is to keep a local list of files downloaded, and check
it before downloading any file. This assumes files on the server are not
renamed, but this is usually the case.

Solution
---
Using an SQL database, we can simply store a reference to each file
as it is retrieved. Since we don't want to have a to run a full SQL server
just to store one small database, sqlite is used. Before downloading 
any file, the database is queried to see if it already has been downloaded.

Configuration
---
grabOnce requires you to set up configuration for each host you want to sync 
with. The first step is setting up your ssh configuration file to use a 
private key for your seedbox's SSH login. A good example of how to do that
can be found [here](https://help.ubuntu.com/community/SSH/OpenSSH/Keys). Next
you must setup a JSON configuration file describing each host and the local
directory to sync it to. An example is provided in sync.json. By default
this file is searched for at `~/.grabOnce.py/sync.json` when the script is invoked.
The host names used in this file must match the hostnames used in your SSH
configuration file.

Running it
---
Once your configuration is setup, running it is easy 

    python grabOnce.py remoteHost
    
This will create the sqlite database if needed, and sync remoteHost as described in
your `sync.json` file.

# Interactive mode

If launched with the `--interactive` command line switch, grabOnce prompts before each
file download. The options for each file are

* Yes - download the file
* No - don't ever download file 
* Skip - skip this file, ask about it again next time or download it if not in interactive mode

If you already have a number of files on your remote machine which you may not need to download,
you can run in interactive mode the first time. Any file you have already downloaded you can 
answer no, and for all other files you can just skip them. Then run grabOnce again without
interactive mode specified.

# Show downloaded files

You can run the script with the `--downloaded` command line switch to show all files
downloaded from the remote host and exit.

Dependencies
---

# Python Modules

* sqlite3 - for manipulating the local database of downloaded file
* paramiko - for listing files on the remote server and downloading small files

# Command line executables

* rsync - for downloading files from the remote server

