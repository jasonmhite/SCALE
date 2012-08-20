import numpy as np
import threading
import subprocess
import uuid
import os
from shutil import rmtree, copyfile
import tempfile

#PREFIX is the working directory where runs will be generated, e.g. /share2/jmhite
PREFIX = '/dev/shm/scale/'

class scaleTask(threading.Thread):
    def __init__(self, xsr, iso_ids, clean=True):
        threading.Thread.__init__(self)

        #Generate the folder for the run
        self.idt = uuid.uuid4().hex
        self.folder = PREFIX + self.idt
        os.mkdir(self.folder)
        self.xsr = xsr[1]
        self.runid = xsr[0]
        self.iso_ids = iso_ids

        #Assemble the readparam files from the inputs
        self._preparePerturbations(self.iso_ids)
        self._copyFiles()
        self.clean = clean

    def run(self):
        try:
            self.value = subprocess.check_output(['batch6.1', self.folder + '/input_scale', '-T ' + self.folder + '/'])
        except subprocess.CalledProcessError as cpe:
            print cpe
        
        self._gatherResults()

        if self.clean:
            self._clean()

    def _clean(self):
        rmtree(self.folder)

    def _preparePerturbations(self, iso_ids):
        for isotope in iso_ids:
            data = np.array([self.xsr[isotope + '_' + str(group)] for group in range(1,45)])
            thefile = self.folder + '/' + 'readparam_' + isotope
            np.savetxt(thefile, data.reshape(1, -1), delimiter=' ')
            self._fixPerturbations(thefile)

    def _fixPerturbations(self, thefile):
        with open(thefile, 'r') as tf:
            text = tf.read()
        os.unlink(thefile)
        with open(thefile, 'w') as tf:
            tf.write('1 44 ')
            tf.write(text)

    def _copyFiles(self):
        copyfile('./Fill/bonamist.sen', self.folder + '/bonamist.sen')
        copyfile('./Fill/ft42f001', self.folder + '/ft42f001')
        copyfile('./Fill/ft92f001', self.folder + '/ft92f001')
        copyfile('./Fill/input_scale', self.folder + '/input_scale')
        copyfile('./Fill/i_worker0002', self.folder + '/i_worker0002')
        copyfile('./Fill/senlib.sen', self.folder + '/senlib.sen')
        copyfile('./Fill/worf', self.folder + '/worf')

    def _gatherResults(self):
        copyfile(self.folder + '/input_scale.out', './Results/' + str(self.runid) + '.out')
        copyfile(self.folder + '/input_scale.sdf', './Results/' + str(self.runid) + '.sdf')

        


