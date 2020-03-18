#!/usr/bin/env python
"""run_batch.py is designed to facilitate batch execution of mred simulations. 
It takes care of passing the arguments to splitrun, and (if needed) scheduling a cleanup job at the end.


ADDED on 20200303 -MLB: 
    - changed default memoryLimit options flag to 16Gb; changed units to Gb
    - the --runName flag is now set automatically for MRED when called using the batch script; 
	    does NOT override (i.e. you can still select a unique runName by passing the flag after mred --init)
    - changed default folder creation behavior. now:
	* killFiles is created and populated automatically with the scancel scripts
	* logFiles is created to store all of the output .txt log files; logFiles/ is populated with 
	    directories for each of the runNames, with each directory (run) having its own log file.
	* disabled the hdf5_output directory creation
    - enable saving in /tmp during run. To be used in conjunction with hdf5RunManager method.
	* directory created on local disk at /tmp/mred_runName
	* automatically removed at the end of each run, or if job cancelled/killed
	* saves .hdf5 output files incrementally, converts to .tar.gz, then sends
	    to the --saveDir passed when running mred. (default is '.')
"""
version_string="$Id: mredBatch.py 2020-03-05 14:40:45Z breediml $"

def submit(parameterDictList=None):
	print "\n\n",version_string,"\n\n"

	import sys
	import os
	import string
	import math
	from splitrun import splitrun_batch
	from optparse import OptionParser
	####################ADDED MLB 20200303
	if not os.path.exists("logFiles"):
	    os.mkdir("logFiles")
	######################################

	# These variable names are important to splitrun_batch since they are used
	# explicitly. Defaults are for 15 minutes, 1 copy and output into a directory 'foo'. 

	parser=OptionParser(usage="run_batch [batch options] mred [mred specific options] <script_file> [script specific options]")
	parser.disable_interspersed_args()
	parser.add_option("", "--runName", action='store', dest="runName", type="str", default="foo",
										help="Run name")
	parser.add_option("", "--runTime", action='store', dest="runTime", type="int", default=int(900),
										help="Requested run time (sec)")
	parser.add_option("", "--nCopies", action='store', dest="nCopies", type="int", default=1,
										help="Number of batch jobs")
	parser.add_option("", "--memoryLimit", action='store', dest="memoryLimit", type="int", default=16,
										help="memory limit in Gb")
#the ppc nodes cannot run any mred stuff now.  no cpu type permitted
	parser.add_option("", "--cpuType", action='store', dest="cpuType", type="str", default="x86",
										help="Allowed CPU Type (x86 only, now!)")
	parser.add_option("", "--ulimit_f", action='store', dest="ulimit_f", type="int", default=10000,
										help="Log size limit")

	parser.add_option("", "--cleanupScriptFile", action='store', dest="cleanupScriptFile", type="str", default="",
										help="Module name in which to find cleanup function (without .py)")
	parser.add_option("", "--cleanupScriptFunction", action='store', dest="cleanupScriptFunction", type="str", default="",
										help="Function inside cleanupScriptFile to execute")

	parser.add_option("", "--configName", action='store', dest="configName", type="str", default="current",
										help="name of config for launched jobs to use")

	(options, args) = parser.parse_args(sys.argv[1:])

	del parser #clean up our namespace

	if options.runTime < 86400:
		cpuinfo="##PBS --nodelist=nehalem:x86"
	else:
		cpuinfo="##PBS --nodelist=x86"
	
	# This is the SLURM template. The only documentation carried out here, and what
	# will be put into the RUN.$(isotime)s.%(index)03d.txt (based on code in
	# splitrun), is a list of the environment variables before the first mred8 code
	# gets executed.

	# The real magic is the bit of code after the program name, namely:
	# "%(variablesDict)s". This dictionary contains every variable created before
	# the run gets launched. It is handed into mred9 as the switch --batch-variables, 
	# and from there is parsed into an object 'batch_vars' which has as its attributes all of the variables for the run.
	# this allows the user to look at all the variables which have been created to set up the run, in case there
	# is information in them which may be used to modify the behavior of the code if it is running in a batch.
	#
	# In particular, if this 'submit' function is called externally with a non-None parameterDictList,
	# which is a list of dictionaries, 
	# each running member of the batch will have one element of that list inserted into the attributes of batch_vars, 
	# so that each member can be doing a different calculation.
	# (for example, running a different ion).

	template="""#!/bin/bash
#SBATCH --mem=%(memory_limit_mb)dG
#SBATCH --nodes=1
%(cpuinfo)s
#SBATCH --time=%(wallTime)d
##PBS -l cput=%(runTime)d
# All output goes to the same file
##PBS -j oe

###########################################3############################################################################
############# Added 20203002 MLB 
localdir=/tmp/mred_%(runName)s%(index)03d
tmp_cleaner()
{
    rm -rf ${localdir}
    exit -1
}
trap 'tmp_cleaner' TERM
mkdir ${localdir} # create unique directory on compute node
echo "CREATING THE FOLLOWING TEMP DIRECTORY: "
echo "${localdir}"
###########################################3############################################################################


#cd $PBS_O_WORKDIR
eval $(/usr/local/radeffects/config/%(configName)s bash)
env

# ulimit -f %(ulimit_f)d
#echo --batch-variables="%(variablesDict)s" `python -c "import cPickle, base64; ss=%(variablesDict)s; print ' '.join(ss['script_options'])"` &> test.txt
%(script_name)s --batch-variables="%(variablesDict)s" `python -c "import cPickle, base64; ss=%(variablesDict)s; print ' '.join(ss['script_options'])"` &> logFiles/%(runName)s/output.%(isotime)s.%(index)03d.txt


###########################################3############################################################################
##############################################   ALSO ADDED MLB 
echo "Removing the following temp directory:"
ls ${localdir}
echo "${localdir}"
rm -rf ${localdir}
###########################################3############################################################################
"""

	if options.cleanupScriptFile:
		finalTask="""#!/bin/bash
#SBATCH --nodes=1
#SBATCH --time=15:00
##PBS -l cput=15:00
#SBATCH --nodes=1
##PBS --nodelist=nehalem:x86
# All output goes to the same file
##PBS -j oe
#cd $PBS_O_WORKDIR
eval $(/usr/local/radeffects/config/%(configName)s bash)
$RADEFFECTS_BASE_DIR/bin/python $PYTHON_LIB_DIR/$MRED_VERS/splitrun.py general_summarize %(runVarsString)s &> logFiles/%(runName)s/output.%(isotime)s.END.txt
"""
	else:
		finalTask=None #don't pass a template for the final run if it doesn't know what to do
		
	################
	## Batch runs ##
	################


	if os.path.exists("logFiles/" + options.runName):
		if not os.path.isdir("logFiles/" + options.runName):
			raise RuntimeError, "Cannot create directory for run data... name exists and is a file"
	else:
		os.mkdir("logFiles/" +options.runName, 0750) #create directory group readable, world invisible

	# This statement copies all the local variables and creates a dictionary called
	# passed_variables that gets fed to splitrun_batch. 
	passed_variables=locals().copy()

	##### MLB added 20200303 -- automatically appends the run name to the mred argument list
	if '--runName' not in [x.split("=")[0] for x in passed_variables['args']]:
	    passed_variables['args'] += ['--runName={}'.format(options.runName)]

	# We pass 'wallTime' explicitly since it is a derived from 'runTime'. We add 4
	# minutes to 'wallTime' to avoid killing jobs because of CPU lags. If you start
	# swapping a lot, the additional time will have to be increased.
	splitrun_batch(wallTime=math.ceil(options.runTime/60.+4), runTime=math.ceil(options.runTime/60.), script_options=args[1:],
			 runName=options.runName, ulimit_f=options.ulimit_f, memory_limit_mb=options.memoryLimit,
			 nCopies=options.nCopies, script_name=args[0], 
			 configName=options.configName, 
			 summarize_module=options.cleanupScriptFile, summarize_function=options.cleanupScriptFunction,
			 **passed_variables)
	
	print "mred ", options, args
	print '\n\n< Program Finished Successfully >\n\n'

	#### ADDED 202020303 MLB
	if not os.path.exists("killFiles"):
	    os.mkdir("killFiles")
	os.system("mv killbatch* killFiles")
	
if __name__ == "__main__":
	submit()
