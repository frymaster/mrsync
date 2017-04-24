#!/usr/bin/env python

import optparse
import os
import Queue
import signal
import subprocess
import sys
import threading
import time

class Scanner (threading.Thread):
  def __init__(self, sources, filecap, sizecap, workdir, sourceQueue, baseDir):
    threading.Thread.__init__(self)
    self.daemon = True
    self.sources = sources
    self.filecap = filecap
    self.sizecap = sizecap
    self.workdir = workdir
    self.sourceQueue = sourceQueue
    self.baseDir = baseDir
    self.status = "Created"
    self.filesAdded = 0
    self.bytesAdded = 0
    self.jobsSubmitted = 0
    self.curFileCount = 0
    self.curByteCount = 0
    self.current = []

  def run(self):
    self.status = "Scanning"
    for path in self.sources:
      for root,dirs,files in os.walk(os.path.join(self.baseDir,path)):
        for f in files:
          target = os.path.join(root,f)
          entry = os.path.relpath(target,self.baseDir)
          try:
            size=os.lstat(target).st_size
          except OSError:
            pass
          self.addToList(entry+"\n",size) #Add newline here to make it easy to write out
        #If a naked dir, add the dir
        if len(dirs) + len(files) == 0:
          #Nominal size for a dir
          self.addToList(os.path.relpath(root,self.baseDir)+"\n",10)
    #Send anything left over
    if len(self.current)>0: self.submitList(self.current)
    self.status = "Completed"
    #This should wait until the queue is empty
    self.sourceQueue.join()

  def addToList(self,entry,size):
    self.filesAdded += 1
    self.bytesAdded += size
    #File is, in and of itself, bigger than the cap - enqueue on its own
    if size > self.sizecap:
      self.submitList([entry])
      return
    self.curFileCount += 1
    self.curByteCount += size
    self.current.append(entry)
    if self.curFileCount > self.filecap or self.curByteCount > self.sizecap:
      self.submitList(self.current)
      self.curFileCount = 0
      self.curByteCount = 0
      self.current = []

  def submitList(self,entries):
    self.status = "Writing job file"
    jobFileName = "mrsync-" + str(os.getpid()) + "-" + str(self.jobsSubmitted)
    jobPath = os.path.join(self.workdir,jobFileName)
    with open(jobPath,"w") as f:
      f.writelines(entries)
    self.status = "Submitting job file to queue"
    self.sourceQueue.put(jobPath)
    self.jobsSubmitted += 1
    self.status = "Scanning"

class Worker (threading.Thread):
  def __init__(self, workdir, sourceQueue,source,destination):
    threading.Thread.__init__(self)
    self.workdir = workdir
    self.sourceQueue = sourceQueue
    self.source = source
    self.destination = destination
    self.status = "-"
    self.daemon = True

  def run(self):
    while True:
      self.status = "-"
      jobList = self.sourceQueue.get()
      self.status = "o"
      logfile = jobList + "-rsync-log.txt"
      args=["rsync","--files-from=" + jobList, "-lotgoDR","--log-file=" + logfile, self.source, self.destination]
      subprocess.call(args)
      os.remove(jobList)
      self.sourceQueue.task_done()

class Status (threading.Thread):
  def __init__(self, scanner, workers):
    threading.Thread.__init__(self)
    self.scanner = scanner
    self.workers = workers

  def run(self):
    while True:
      statusline=["Workers ["]
      for worker in self.workers:
        statusline.append(worker.status)
      statusline.extend(["] Scanner status: ",self.scanner.status])
      statusline.extend(["  ",str(self.scanner.filesAdded)," files, ",str(self.scanner.bytesAdded)," bytes, ",str(scanner.jobsSubmitted)," jobs submitted.  Queue size: ",str(scanner.sourceQueue.qsize())])
      print "".join(statusline)
      if not self.scanner.isAlive(): return
      time.sleep(0.1)

#When waiting on threads we need to explicitly catch interrupts
def signal_handler(signal, frame):
  print "\nCaught signal - exiting"
  sys.exit(1)

if __name__ == "__main__":
  parser = optparse.OptionParser(version="%prog 1.0",usage="usage: %prog [options] [source paths] [[user@]host:]destination")
  parser.add_option("-b","--base-dir",action="store",type="string",dest="base_dir",default="/",help="Base directory for sources")
#  parser.add_option("--exclude",action="append",type="string",dest="exclude_pattern",help="Patterns to exclude. Scanner does not support wildcards. Passed through to rsync")
#  parser.add_option("--exclude-from",action="append",type="string",dest="exclude_file",help="File of patterns to exclude. Scaner does NOT support wildcards. Passed through to rsync")
  parser.add_option("--files-from",action="append",type="string",dest="include_file",default=[],help="File of source paths to include, one per line. Does not support wildcards")
  parser.add_option("--workers","-w",action="store",type="int",dest="workers",default=4,help="Number of rsync workers (4)")
  parser.add_option("--size","-s",action="store",type="int",dest="size",default=1024,help="Send to rsync worker after size reaches this limit in megabytes")
  parser.add_option("--filenumber","-f",action="store",type="int",dest="number",default=10000,help="Send to rsync worker after this many files reached")
  parser.add_option("--queue","-q",action="store",type="int",dest="queue_size",default=0,help="Only create this many rsync input files in advance.  Default is infinite")
  
  (options,args)=parser.parse_args()

  #Check option sanity
  if len(options.include_file) + len(args) < 2:
    parser.print_help()
    print "\nError: You must specify at least one source path - or at least one file of source paths - and one destination"
    sys.exit(254)
  if options.workers < 1 or options.number < 1 or options.size < 1 or options.queue_size < 0:
    parser.print_help()
    sys.exit(254)

  #Add paths from files to paths from command-line
  paths=args[:-1]
  for filename in options.include_file:
    try:
      f = open(filename,"r")
    except IOError:
      print "Unexpected error reading",filename,"- aborting"
      sys.exit(1)
    with f:
      for line in f:
        paths.append(line.strip())
  if len(paths) < 1:
    parser.print_help()
    print "\nError: You have specified a file of source paths but it is empty" 
    sys.exit(254)

  #Create work directory if it does not exist
  workdir=os.path.expanduser(os.path.join("~",".mrsync"))
  if not os.path.isdir(workdir):
    try:
      os.mkdir(workdir)
    except:
      print "Couldn't create work directory",workdir,"- aborting"
      sys.exit(1)

  signal.signal(signal.SIGINT, signal_handler)
  sourceQueue = Queue.Queue(maxsize=options.queue_size)
  scanner = Scanner(paths, options.number, options.size * 1048576, workdir, sourceQueue, options.base_dir)
  scanner.start()
  workers = []
  for i in range(0,options.workers):
    worker = Worker(workdir, sourceQueue,options.base_dir,args[-1])
    worker.start()
    workers.append(worker)
  status = Status(scanner,workers)
  status.start()
  while scanner.isAlive():
    scanner.join(5)
