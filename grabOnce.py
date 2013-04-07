import sys
import argparse
import logging
import os
import json
import stat
import sqlite3
import time
import subprocess
import itertools

class FileHistory(object):
    
    def __init__(self,filename,host):
        self.db = sqlite3.connect(filename)
        self.db.execute("CREATE TABLE IF NOT EXISTS fileHistory ( host text NOT NULL, path text  NOT NULL ,PRIMARY KEY(host,path) )");
        
        self.host = host
        
    def hasFile(self,file):
        cur = self.db.execute("Select 1 FROM fileHistory where host=? and path=?",(self.host,file))
        
        return cur.fetchone() != None
        
    def recordFile(self,file):
        self.db.execute("INSERT INTO fileHistory (host,path) VALUES (?,?)",(self.host,file))
        self.db.commit()
        
    def __iter__(self):
		
		cur = self.db.execute("Select path FROM fileHistory where host=:host",{"host":self.host})
		
		class CursorWrapper(object):
			def __init__(self,cursor):
				self.cursor = cursor
				
			def next(self):
				result = self.cursor.fetchone()
				if result == None:
					raise StopIteration()
					
				path, = result
				return path
			def __del__(self):
				self.cursor.close()
		
		return CursorWrapper(cur)
		
    def __del__(self):
        self.db.close()

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

def rsync(localFile,remoteFile):
    r = args.remoteHost+':'+remoteFile

    cmd = [args.rsync,'-s','--partial','--archive','--verbose','--progress', r ,  localFile  ]
    sys.stdout.write('Executing ' + ' '.join(cmd) + '\n')
    try:
        proc = subprocess.Popen(cmd)
    except OSError:
        sys.stderr.write("Failed to rsync: " + remoteFile + '\n')
        raise
    
    if 0 != proc.wait():
        raise Exception("Rsync failed\n")


def main():
    parser = argparse.ArgumentParser(description='Retrieve files from an sftp server, but only once.')
    
    #Grab the username
    username = os.environ['USER']
    
    #Argument for the remote host
    parser.add_argument(dest='remoteHost',action='store',metavar='R',help='The remote host to synchronize with')
    
    #Argument for the default config file, use the most probable location
    parser.add_argument('--configFile',dest='configFile',action='store',metavar='F',help='OpenSSH style config file to use',default='~/.ssh/config',nargs=1)
    
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

    #Add a command for listing downloaded files
    parser.add_argument('--downloaded',dest='showDownloaded',action='store_true',help='Show all downloaded files for the specified host and exit',default=False)
    
    global args
    args = parser.parse_args()

    args.filehistory = os.path.expanduser(args.filehistory)
 
    #Open the file history for this host
    try:
        fh = FileHistory(args.filehistory,args.remoteHost)
    except:
        sys.stderr.write("Error opening sqlite database: " + args.filehistory + '\n')
        raise
        
    #Check to see if the user just wanted to see downloaded files
    if args.showDownloaded:
		for file in fh:
			print file
		sys.exit(0)
	
	#load the sync configuration file
    args.syncFile = os.path.expanduser(args.syncFile)
    #Check for existence, fail otherwise
    if not os.path.exists(args.syncFile):
        sys.stderr.write("Can't find synchronization file: " + args.syncFile + '\n')
        sys.exit(1)

    try:
        syncConfig = json.load(open(args.syncFile,'r'))
    except:
        sys.stderr.write("Can't parse synchronization file: " + args.syncFile + '\n')
        raise
    
    #check to make sure the required key is present 
    if not syncConfig.has_key("hosts"):
        sys.stderr.write("Synchronziation file missing hosts entry")
        sys.exit(1)
    
    #Make sure it is the correct type    
    if not isinstance(syncConfig['hosts'],type(dict())):
        sys.stderr.write("Synchronization file hosts entry should be a dictionary")
        sys.exit(1)
    
    if not syncConfig['hosts'].has_key(args.remoteHost):
        sys.stderr.write("Synchronization file does not have a host entry for: " + args.remoteHost +'\n')
        sys.exit(1)
    
    if not syncConfig['hosts'][args.remoteHost].has_key('remote'):
        sys.stderr.write("Synchronization file does not specify remote directory for: " + args.remoteHost + '\n')
        sys.exit(1)
        
    remoteDir = os.path.normpath(syncConfig['hosts'][args.remoteHost]['remote'])
    
    #If a local directory is not specified, use the same as the remote
    #directory
    if syncConfig['hosts'][args.remoteHost].has_key('local'):
        localDir = syncConfig['hosts'][args.remoteHost]['local']
    else:
        localDir = remoteDir
    
    localDir = os.path.normpath(localDir)
    
    cmd = ['ssh', args.remoteHost,"find " + remoteDir + " -type f -print0"]
    try:
        proc = subprocess.Popen(cmd,stdout=subprocess.PIPE,bufsize=-1)
        stdout,_ = proc.communicate()
    except OSError:
        sys.stderr.write("Failed to retrieve remote host file listing\n")
        raise
		
    if proc.returncode != 0:
        sys.stderr.write("Failed to retrieve remote host file listing\n")
        sys.exit(1)

    remoteFiles = stdout.split('\0')
    
    codec = 'utf-8'

    #eliminate empty lines, happens at the end
    remoteFiles = [ remoteFile.decode(codec) for remoteFile in remoteFiles if len(remoteFile) > 0 ]

    localFiles = [ os.path.join(localDir,remoteFile.replace(remoteDir + '/','')) for remoteFile in remoteFiles]

    
    for localFile,remoteFile in itertools.izip(localFiles,remoteFiles):
        if not fh.hasFile(remoteFile):   
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
                containingDir, _ = os.path.split(localFile)
                if not os.path.exists(containingDir):
                    os.makedirs(containingDir)

                rsync(localFile,remoteFile)
      
            if record:
                fh.recordFile(remoteFile)
        
if __name__ == "__main__":
    main()
