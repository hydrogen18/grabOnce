import paramiko
import sys
import argparse
import logging
import os
import getpass
import json
import stat
import sqlite3
import time
import subprocess

class FileHistory(object):
    
    def __init__(self,filename):
        self.db = sqlite3.connect(filename)
        self.db.execute("CREATE TABLE IF NOT EXISTS fileHistory ( host text NOT NULL, path text  NOT NULL ,PRIMARY KEY(host,path) )");
        
    def hasFile(self,host,file):
        cur = self.db.execute("Select 1 FROM fileHistory where host=? and path=?",(host,file))
        
        return cur.fetchone() != None
        
    def recordFile(self,host,file):
        self.db.execute("INSERT INTO fileHistory (host,path) VALUES (?,?)",(host,file))
        self.db.commit()
        pass

class BandwidthMeter(object):
    
    def __init__(self):
        self.startTime = time.time()
        self.bytes = 0
        
    def addBytes(self,bytes):
        self.bytes += bytes
        
    def getRate(self):
        return float(self.bytes)/(time.time() - self.startTime)

def prompt(prompt,default,permitted):
    if default not in permitted:
        raise ValueError("Default not in permitted")
    
    permitted[permitted.index(default)] = default.upper()
    
    prompt = prompt + ' [' + ','.join(permitted) + ']:'
        
    retval = None
    while retval == None:
        uin = raw_input(prompt)
        if len(uin) > 1:
            continue
        elif len(uin) == 0:
            retval = default
        elif len(uin) == 1:
            i = uin[0].lower()
            if i in map(lambda x : x.lower(),permitted):
                retval = i
        
    return retval

def main():
    parser = argparse.ArgumentParser(description='Retrieve files from an sftp server, but only once.')
    
    #Grab the username
    username = os.environ['USER']
    
    #Argument for the remote host
    parser.add_argument(dest='remoteHost',action='store',metavar='R',help='The remote host to synchronize with')
    
    #Argument for the default config file, use the most probable location
    parser.add_argument('--configFile',dest='configFile',action='store',metavar='F',help='OpenSSH style config file to use',default='~/.ssh/config',nargs=1)
    
    #Argument for the remote user, default to current username
    parser.add_argument('--remoteUser',dest='remoteUser',action='store',metavar='U',help='User to authenticate as',default=username,nargs=1)
    
    #Argument for the sync file with a default
    parser.add_argument('--syncfile',dest='syncFile',action='store',metavar='S',help='Synchronization configuration file',default='~/.' + os.path.basename(__file__) + '/sync.json',nargs=1)
    
    #Argument for file history database with a default
    parser.add_argument('--filehistory',dest='filehistory',action='store',metavar='I',help='SQLite database of file history',default='~/.' + os.path.basename(__file__) + '/history.sqlite',nargs=1)
    
    #Argument for interactive mode
    parser.add_argument('--interactive',dest='interactive',action='store_true',help='Prompt for each file before downloading', default=False)
    
    #Argument to specify rsync exectuable
    parser.add_argument('--with-rsync',dest='rsync',action='store',help='Path to rsync executable',default = '/usr/bin/rsync',nargs=1)
    
    #Argument for read buffer size
    parser.add_argument('--buffersize',dest='buffersize',action='store',type=int,metavar='R',help="Number of bytes to read at a time from the remote server",default=65536,nargs=1)

    
    args = parser.parse_args()
    
    args.syncFile = os.path.expanduser(args.syncFile)
    if not os.path.exists(args.syncFile):
        sys.stderr.write("Can't find synchronization file: " + args.syncFile + '\n')
        sys.exit(1)
        
    try:
        syncConfig = json.load(open(args.syncFile,'r'))
    except:
        sys.stderr.write("Can't parse synchronization file: " + args.syncFile + '\n')
        raise
    
    if not syncConfig.has_key("hosts"):
        sys.stderr.write("Synchronziation file missing hosts entry")
        sys.exit(1)
        
    if not isinstance(syncConfig['hosts'],type(dict())):
        sys.stderr.write("Synchronization file hosts entry should be a dictionary")
        sys.exit(1)
        
    args.filehistory = os.path.expanduser(args.filehistory)
    
    try:
        fh = FileHistory(args.filehistory)
    except:
        sys.stderr.write("Error opening sqlite database: " + args.filehistory + '\n')
        raise
        
    
    config = paramiko.SSHConfig()
    
    #If a config file exists, parse it
    args.configFile = os.path.expanduser(args.configFile)
    if os.path.exists(args.configFile):
        config.parse(open(args.configFile,'r'))
    
    #Set some sensible defaults for connection settings
    remoteHost = args.remoteHost
    remoteHostKey = None
    remoteHostPort = 22
    remoteUser = args.remoteUser
    
    #If a configuration file entry present for this host, then use it
    if  len(config.lookup(remoteHost))!=0:
        hostDict = config.lookup(remoteHost)
        
        #Get the actual hostname
        remoteHost = hostDict['hostname']
        
        #Use the specified values if present
        if hostDict.has_key('port'):
            remoteHostPort = int(hostDict['port'].strip())
            
        if hostDict.has_key('user'):
            remoteUser = hostDict['user'].strip()
            
        if hostDict.has_key('identityfile'):
            remoteHostKey = os.path.expanduser(hostDict['identityfile']).strip()
            
            if not os.path.exists(remoteHostKey):
                sys.stderr.write("Can't find key file " + remoteHostKey + " for host " + remoteHost + '\n')
                sys.exit(1)
            remoteHostKey = paramiko.RSAKey.from_private_key_file(remoteHostKey)
            #TODO - DSA Key?
    
    #Make sure the sync file has an entry for this host
    if not syncConfig['hosts'].has_key(args.remoteHost):
        sys.stderr.write("Synchronization file does not have a host entry for: " + args.remoteHost +'\n')
        sys.exit(1)
    
    if not syncConfig['hosts'][args.remoteHost].has_key('remote'):
        sys.stderr.write("Synchronization file does not specify remote directory for: " + args.remoteHost + '\n')
        sys.exit(1)
        
    remoteDir = syncConfig['hosts'][args.remoteHost]['remote']
    
    if syncConfig['hosts'][args.remoteHost].has_key('local'):
        localDir = syncConfig['hosts'][args.remoteHost]['local']
    else:
        localDir = remoteDir
    
    #Create the transport layer, this does not connection
    tport = paramiko.Transport((remoteHost,remoteHostPort))
    
    #Connect and authenticate
    if remoteHostKey:
        tport.connect(username = remoteUser, pkey = remoteHostKey)
    else:
        password = getpass.getpass("Password:")
        tport.connect(username = remoteUser, password = password)
    
    sftp = paramiko.SFTPClient.from_transport(tport)
            
    def sync(sftp,remoteDir,localDir):
        try:
            entries = sftp.listdir_attr(remoteDir)
        except:
            sys.stderr.write("Failed to retrieve remote directory listing\n")
            raise
            
        for entry in entries:
            if stat.S_ISDIR(entry.st_mode):
                newRemote = os.path.join(remoteDir,entry.filename)
                newLocal = os.path.join(localDir,entry.filename)
                for x in sync(sftp,newRemote,newLocal):
                    yield x
            elif stat.S_ISREG(entry.st_mode):
                remoteFile = os.path.join(remoteDir,entry.filename)
                localFile = os.path.join(localDir,entry.filename)
                
                yield remoteFile,localFile
                    
    
    for remoteFile, localFile in sync(sftp,remoteDir,localDir):
        if not fh.hasFile(remoteHost,remoteFile):   
            sys.stdout.write (remoteFile + ' ->' + localFile +'\n')

            download = True
            record = True
            
            if args.interactive:
                selection = prompt("Download?",'y',['y','n','s'])
                
                if selection == 'n':
                    download = False
                elif selection == 's':
                    download = False
                    record = False
                
            if download:     
                bwm = BandwidthMeter()
                
                containingDirectory,_ = os.path.split(localFile)
                
                if not os.path.exists(containingDirectory):
                    os.makedirs(containingDirectory)
                
                if os.path.exists(localFile):
                    sys.stdout.write("Local file already exists, skipping\n")
                    continue
                
                fin = sftp.open(remoteFile,'r',args.buffersize)
                filesize = fin.stat().st_size
                
                #For small files use paramiko
                if filesize < 4096:
                    try:
                        with open(localFile,'w+') as fout:
                            while True:
                                dataIn = fin.read(args.buffersize)
                                
                                if len(dataIn) == 0:
                                    break
                                    
                                fout.write(dataIn)
                                bwm.addBytes(len(dataIn))
                                sys.stdout.write("Download Rate: " + str(bwm.getRate()) +' B/s \r')
                                sys.stdout.write('\n')
                    except paramiko.SFTPError:
                        sys.stderr.write("Failed while downloading: " + remoteFile + '\n')
                        os.unlink(localFile)
                        raise
                
                #Otherwise invoke rsync
                else:                    
                    cmd = [args.rsync,'--archive','--verbose','--progress',remoteUser+'@'+args.remoteHost+':' + remoteFile,localFile]
                    sys.stdout.write('Executing ' + ' '.join(cmd) + '\n')
                    try:
                        proc = subprocess.Popen(cmd)
                    except OSError:
                        sys.stderr.write("Failed to rsync: " + remoteFile + '\n')
                        raise
                    
                    if 0 != proc.wait():
                        sys.stderr.write("Rsync failed\n")
                        
                        
                  
            if record:
                fh.recordFile(remoteHost,remoteFile)
        
if __name__ == "__main__":
    main()