from scale_new import scaleTask
import os

class myScaleTask(scaleTask):
    def setup(self):
        

    def gather(self):
        with open(self.folder + '/test.a', 'w') as thefile:
            thefile.write('test2')


options = {'cmd' : 'batch6.1', 'filename' : 'input_scale', 'opts' : []}
st = myScaleTask()
