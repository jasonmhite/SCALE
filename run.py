import pandas as pd
import numpy as np
import re
from time import sleep
from itertools import islice
from IPython.parallel import Client
import os

try:
    os.mkdir('/dev/shm/scale')
except OSError:
    pass

#Parse in column names
txt = """param_92234_0018
 param_92234_0101
 param_92234_0002
 param_92235_0018
 param_92235_0101
 param_92235_0002
 param_92238_0018
 param_92238_0101
 param_92238_0002"""

#Below - Magic
pars = re.compile('_(\d{5})_(\d{4})')
ids = map(lambda x: x[0] + '_' + x[1], pars.findall(txt))
c_names = [i + '_' + j for i in ids for j in map(str, range(1, 45))]

# Subtract one because readparam automatically adds 1
samples = pd.read_csv('design-pred.txt', sep='\s*', header=False, names=c_names) - 1.

cl = Client(profile='ssh')
dview = cl[:]
lview = cl.load_balanced_view()

with dview.sync_imports():
    import scale

dview.push(dict(IDS=ids))

@lview.parallel()
def runtasks(samples):
    task = scale.scaleTask(samples, IDS)
    task.run()
    return task.value

job = runtasks.map(list(samples.iterrows()))
while not job.ready():
    sleep(5)
results = job.get()
print "JOB DONE"


# dummytasks = list(islice(samples.iterrows(), 0, 5))
# job = runtasks.map(dummytasks)

# while not job.ready():
#     sleep(1)
# results = job.get()

#print results



