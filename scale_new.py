import threading
import subprocess
import uuid
import os
from shutil import rmtree
from time import sleep

#PREFIX is the working directory where runs will be generated, e.g. /share2/jmhite
PREFIX = '/dev/shm/scale/'

class scaleTask(threading.Thread):
    """Prepare and run a parallel SCALE task on an IPython cluster.

    Positional arguments:
    opts:
      A dict with the following entries
          cmd (str): the command to run SCALE in batch mode (e.g. 'batch6.1').
          filename (str): the input file name. Relative, will have the working directory prepended.
          opts (list[str]) : Extra options to pass to SCALE. MUST be a list, even for one entry.
                             DO NOT include the '-T' option, it is automatically added

    inputs:
      Inputs for SCALE. This is up to you to use and will be available as self.inputs.

    cluster:
      The IPython cluster to run on. Must be a load_balanced_view.

    Keyword arguments:
    clean (True)
      Whether or not to delete the folder after the task completes.

    Note that self.folder will refer to the path of the working directory WITHOUT a trailing slash. 
    Use this to copy files into and out of the directory.

    Method execution order:
      setup() : Copy files into directory, prepare inputs etc.
      --self.run() is executed--
      gather() : Collect results, e.g. copy outputs

    Please overwrite setup and gather to use, then manually call .run().
    """

    def __init__(self, opts, inputs, cluster, clean=True):
        threading.Thread.__init__(self)
        self.idt = uuid.uuid4().hex
        self.folder = PREFIX + self.idt
        os.mkdir(self.folder)
        self.lview = cluster
        self.opts = opts
        self.inputs = inputs

        self.setup()
        self.extraPrep()

    def setup(self):
        pass

    def run(self):
        try:
            self.retval = self.lview.apply_async(subprocess.check_call, self.runstr)
            #This might not work, threads may block?
            while not self.retval.ready():
                sleep(5)
            self.retval = self.retval.get()

            self.gather()

        except:
            print "Error in task", self.idt

        if self.clean:
            self._clean()
        

    def gather(self): 
        pass

    def _genRunstr(self):
        #Need a helper function to generate the run string
        self.runstr = [self.opts['cmd']] + [self.folder + '/' + self.opts['filename']] + opts['opts'] + ['-T', self.folder + '/']

    def _clean(self):
        rmtree(self.folder)


