import numpy as np
import threading
import subprocess
import uuid
import os
from shutil import rmtree, copyfile
import tempfile
from time import sleep

#PREFIX is the working directory where runs will be generated, e.g. /share2/jmhite
PREFIX = '/dev/shm/scale/'

class scaleTask(threading.Thread):
    def __init__(self, opts, inputs, cluster, clean=True):
        threading.Thread.__init__(self)
        self.idt = uuid.uuid4().hex
        self.folder = PREFIX + self.idt
        os.mkdir(self.folder)
        self.lview = cluster
        self.cmd = opts

        self.setup()
        self.extraPrep()

    def setup(self):
        pass

    def extraPrep(self):
        pass

    def run(self):
        try:
            self.retval = self.lview.apply_async(subprocess.check_call, self.runstr)
            while not self.retval.ready():
                sleep(5)
            self.retval = self.retval.get()

            self.gather()

        except:
            print "Error in task", str(self.idt)

        if self.clean:
            self._clean()
        

    def gather(self): 
        pass

    def _genRunstr(self):
        #Need a helper function to generate the run string
        self.runstr = [self.opts['cmd']] + [self.folder + '/' + self.opts['filename']] + opts['opts'] + ['-T', self.folder + '/']

    def _clean(self):
        rmtree(self.folder)


