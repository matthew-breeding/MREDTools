#!/labs/isde/mred/mred-20181015/bin/python
"""
Author: Matthew Breeding
Email: 	matthew.l.breeding@vanderbilt.edu
Created: Nov 2019; most recent update: March 15, 2020

This executable python program combines HDF5 files from parallelized runs into one single HDF5 file.
    The files are selected using the command line option flag '--files' which defaults to all 
    HDF5 files in the current directory ("*.hdf5"). User may specify the output directory, or the 
    default ("combinedHDF5output") is created and used.
    Automatically calculates the new nIons flag, assigns the rest of the file attributes based on whichever file is 
    first in the files list. 
"""
# 	NOTE: Currently only works for log-spaced histograms ... need to figure out why the logflag wasn't properly importing from the saved files

from mredHdf5 import smartAccumulate, getAttrDict, checkAttrs
import numpy as np
import parsers, glob, mred_hdf5, os

parsers.defaultOptions()
parsers.addStrOption('files', '*.hdf5')
# Where to save the total hdf5 file. NOTE: don't use the same directory as files
parsers.addStrOption('saveName', '')
# checks to make sure the gun fluence unit matches between runs by default; can turn off with this flag
parsers.addBoolOption('skipGFU', False)
options, rem = parsers.updateOptions()

if os.path.relpath(options.saveDir) == 'outputData':
    options.saveDir = 'combinedHDF5output'
    if not os.path.exists(options.saveDir):
	os.makedirs(options.saveDir)
    if len(os.listdir('outputData')) == 0:
	os.rmdir('outputData')

d = mred.setDevice()
d.register()
mred.init()


histograms = smartAccumulate(options.files, (not options.skipGFU))

for h in histograms:
    hnew = mred.getNewHistogram(h.name, h.low, h.high, h.nbins, True)
    hnew.merge(h)


# selects the first file in the hdf5 directory to pull file attributes from  
if type(options.files) == str:
    fileNames = glob.glob(options.files)
elif type(options.files) == list:
    fileNames=options.files
attrs = getAttrDict(fileNames[0])

if not options.saveName:
    try:
	baseNames =  list(set([os.path.basename('_'.join(fname.split("_")[:-1]))[:-3] for fname in fileNames]))
	if len(baseNames) == 1:
	    options.saveName = baseNames[0] + "TOTAL"
	else:
	    options.saveName = "defaultSaveName"
    except:
	print("ERROR when setting the default save name...not sure why though, make sure that the files are selected properly at rum time?")

mred.autogenerate_histograms=False
mred.hdf5.file_path=options.saveDir
mred.hdf5.file_name=options.saveName+'.hdf5'
mred.hdf5.include_histograms=True
mred.hdf5.write_output_files=True

mred.beamOn(0)


# then sets the new file attributes using these. 
for k in attrs:
    try:
	mred.hdf5.setFileAttribute(k,attrs[k].item())
    except Exception, e:
	print e
	print "ERROR adding the file attribute: %s"%k
	
# Finally, nIons is updated by summing over all nIons flags in the run files 
nIons= np.sum(checkAttrs(options.files, 'nIons', None))
mred.hdf5.setFileAttribute('nIons', nIons)

