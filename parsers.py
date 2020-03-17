"""
Author: Matthew Breeding
E-mail: matthew.l.breeding@vanderbilt.edu

Provides a few methods for instantiating a command line options parser when submitting simulations using MRED;
establishes default options which are expected in other helper modules (i.e. hdf5RunManager) and are 
most generally useful when running MRED.
"""
import sys as _sys
import os as _os
import argparse as _argparse
from datetime import datetime

_parser=_argparse.ArgumentParser()

#  updates the options and remaining namespaces with flags called from the command line.
#          Returns a tuple with each respective namespace. Allows for the creation of more options
#        beyond the default ones, then sorting them into the appropriate namespace
#        Checks that the save directory 
def updateOptions():
    options,remaining = _parser.parse_known_args(_sys.argv[1:])
    allowed = ['-i', '--init']
    for a in allowed:
        if a in remaining:
            remaining.remove(a)
    for r in remaining:
        if len(r) > 2:
            if r[-3:] == '.py':
                remaining.remove(r)

    
    if 'saveDir' in options:
        if options.saveDir[-1] != '/':
            options.saveDir += '/'

        if not _os.path.isdir(options.saveDir):
            print("Save directory given at startup is not a valid path...attempting to create path: {}".format((options.saveDir)))
            try:
                _os.makedirs(options.saveDir)
            except Exception:
                print("ERROR MAKING SAVE DIRECTORY PATH FROM options.saveDir. Defaulting the working directory output folder that called the script")
                options.saveDir = _os.getcwd()+'/outputData/'
                _os.makedirs(options.saveDir)

    print( "*"*15)
    print( 'run options: ')
    print( options)
    print( '\n'*4)
    return (options,remaining)


#         @param name String input of the parser option name
#        This checks to make sure the input name is a string, and that it has the two hyphen flag prepended ('--')
def _nameCheck(name):
    if type(name) != str:
        print("ERROR! Must use a string as the name of the parser option")
        return False
    else:
        if name[0] != '-':
            name = '-' + name
            if name[1] !='-':
                name = '-' + name
        return name

### TODO: add this complete wrapper
#def add_argument(name, **kwargs):
#    try:
#	_parser.add_argument(name, help, **kwargs)
#    except:
#	print("Option {} already created".format(name))

#        used for multiple arguments passed to the same option tag
#        e.g. --deviceDimensions 10 10 10 
#        Note: the argument must be all of the same type, and that type must be specified 
#        explicitly with the @param argumentType, and the @param defaultValue must be an list or tuple
def addArrayOption(name, argumentType, defaultValue, helpString=''):
    name = _nameCheck(name) 

    try:
        _parser.add_argument(name, nargs="+", type = argumentType, default = defaultValue, help = helpString)
        print('Adding parse option %s with default value %s'%(name, str(defaultValue)))
    except Exception:
        print( 'option {} already created'.format(name))
        

def addBoolOption(name, defaultValue=False, helpString=''):
    name = _nameCheck(name)
    try:
        _parser.add_argument(name, action = "store_true", default = defaultValue, help = helpString)
        print( 'Adding parse option %s with default value %s'%(name, str(defaultValue)))
    except Exception:
        print( "option {} already created".format(name))

def addStrOption(name, defaultValue='', helpString=''):
    name = _nameCheck(name)
    if not name:
        return False
    try:
        _parser.add_argument(name, type = str, default = defaultValue, help = helpString)
        print( 'Adding parse option %s with default value %s'%(name, str(defaultValue)))
    except Exception: 
        print( 'option {} already created'.format(name))

def addFloatOption(name, defaultValue, helpString=''):
    name = _nameCheck(name)
    if not name:
        return False
    try:
        _parser.add_argument(name, type = float, default = defaultValue, help = helpString)
        print( 'Adding parse option %s with default value %s'%(name, str(defaultValue)))
    except Exception: 
        print( 'option {} already created'.format(name))

def addIntOption(name, defaultValue, helpString=''):
    name = _nameCheck(name)
    if not name:
        return False
    try:
        _parser.add_argument(name, type = int, default = defaultValue, help = helpString)
        print( 'Adding parse option %s with default value %s'%(name, str(defaultValue)))
    except Exception: 
        print( 'option {} already created'.format(name))

def addOption(name, defaultValue , optionType=str, helpString = ''):
    name = _nameCheck(name)
    try:
        _parser.add_argument(name, type = optionType, default = defaultValue, help = helpString)
    except Exception:
        print( 'option {} already created'.format(name))

    

#   Loads the default option parsers (most commonly used)
#        This is done automatically upon loading the parsers module. Of course, individual method loading
#        provides a way around this auto load feature if only certain methods here are desired, but I can't 
#        think of any applications where that would be preferrable...
#        includes: 
#        particle, beamDir, waferMat, runName, saveDir, 
#        beamE, beamA, beamZ, beamTilt, beamRoll, index, sBias, maxSteps, 
#        saveAsText, dx, suv
def defaultOptions():
    
    print( "\n"*2)
    print( 'Loading default options paraser from .py helper functions....')
    print( "\n"*2)
    
    now = datetime.now()
    addStrOption('--particle', 'neutron', 'defines the particle beam species' )
    addStrOption('--beamType', 'directionalFlux', 'defines the particle beam flux option. Options include "directionalFlux", "pointSource", "isotropicFlux", and others' )
    addStrOption('--waferMat', 'silicon', 'defines the material of the wafer (defaults to silicon)' )
    addStrOption('--runName', 'defaultRunName{:04}{:02}{:02}.{:02}{:02}'.format(now.year, now.month, now.day, now.hour, now.minute), 'defines the run name' )
    addStrOption('--saveDir','%s/outputData/'%(_os.getcwd()),'the directory path for saving output data from the run')#### default to the run name directory

    addFloatOption('--beamE', 1., 'defines the particle beam energy in MeV. Defaults to 1 MeV')
    addFloatOption('--beamTilt', 0., 'defines the particle beam tilt (polar angle, in degrees)')
    addFloatOption('--beamRoll', 0., 'defines the particle beam roll (azimuthal angle, in degrees)')
    addFloatOption("--rangeCuts", 1.0 , "range cuts (in um). Defines the minimum threshold energy (using range) for secondary production")

    addIntOption('--beamA', 1, 'defines the particle beam atomic mass')
    addIntOption('--beamZ', 1, 'defines the particle beam atomic number')
    addIntOption('--nIons', 100, 'sets the total number of particles to run')
    addIntOption("--index", 0 ,"index off set value, used to increment the batch index when saving files")
    addIntOption("--maxSteps", 20000 , "maximum number of steps in a given event")
    addIntOption("--sBias", 500 , "Hadronic cross section biasing factor")
    addIntOption("--nSaves", 10 , "number of incremental progress saves")

    addBoolOption("--retainAll" , helpString="Keep all of the files generated as incremental saves. Useful for retaining the steps/tracks in HDF5")#this will be deprecated by the saver module once that is complete. 
    addBoolOption("--dx" , helpString="Use the OpenDx view for event-by-event viewing")
    addBoolOption("-f" , helpString="include copies of the scripts in output files")
    addBoolOption("--suv" , helpString="Use the Geant4 OpenGL Viewer")

    options, remaining = updateOptions()
    return options, remaining

def slurmOptions():
    addStrOption("name", "slurmJobName")
    addStrOption("errorFile", "")
    addStrOption("logFile", "")
    addIntOption("nodes", 1)
    addIntOption("memoryLimit", 1)
    addIntOption("runTime", 1)
    addIntOption("tasks", 1)
    
    options, remaining = updateOptions()
    return options, remaining


