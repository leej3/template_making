#!/usr/bin/env python
# -*- coding: utf-8 -*-

# way to run script example
# python make_template_dask.py \
#  -dsets /dsets/*.HEAD -init_base ~/abin/MNI_2009c.nii.gz -ex_mode dry_run
#

import sys
sys.path.append('/data/NIMH_SSCC/template_making/scripts')
import copy
from time import asctime

# AFNI modules
from afni_base import *
from afni_util import *
from option_list import *
from db_mod import *
import ask_me

# parallelization library
from dask_jobqueue import SLURMCluster
from dask.distributed import Client
from dask import delayed
#def delayed(fn):
#  return fn

g_help_string = """
===========================================================================
make_template_dask.py    make a template from a bunch of datasets

This Python script iteratively aligns datasets initially to an example base dataset
and then to each other to make a new common template.

---------------------------------------------
REQUIRED OPTIONS:

-dsets   : names of input datasets
-init_base   : initial base template, mostly for AC-PC or similar rigid registration
-template_name : name of new template dataset

MAJOR OPTIONS:
-help       : this help message
-outdir ssss: put all output into a specific, new directory (default is iterative_template_dir)
-overwrite  : overwrite and replace existing datasets
-restart    : skip over preexisting results to continue as quickly as possibly with a restart
"""
## BEGIN common functions across scripts (loosely of course)
class RegWrap:
  def __init__(self, label):
  self.make_template_version = "0.01" # software version (update for changes)
  self.output_dir = 'iterative_template_dir' # user assigned path for anat and EPI

  self.label = label
  self.valid_opts = None
  self.user_opts = None
  self.verb = 1    # a little talkative by default
  self.save_script = '' # save completed script into given file
  self.rewrite = 0 #Do not recreate existing volumes
  self.oexec = "" #dry_run is an option
  self.rmrm = 1   # remove temporary files
  self.prep_only = 0  # do preprocessing only
  self.odir = os.getcwd()

  return

  def init_opts(self):

    self.valid_opts = OptionList('init_opts')

  # input datasets
  self.valid_opts.add_opt('-dsets', -1,[],\
   helpstr="Names of datasets")
  self.valid_opts.add_opt('-init_base', 1,[],\
   helpstr="Name of initial base dataset")

  self.valid_opts.add_opt('-keep_rm_files', 0, [], \
   helpstr="Don't delete any of the temporary files created here")
  self.valid_opts.add_opt('-prep_only', 0, [], \
   helpstr="Do preprocessing steps only without alignment")
  self.valid_opts.add_opt('-help', 0, [], \
   helpstr="The main help describing this program with options")
  self.valid_opts.add_opt('-limited_help', 0, [], \
   helpstr="The main help without all available options")
  self.valid_opts.add_opt('-option_help', 0, [], \
   helpstr="Help for all available options")
  self.valid_opts.add_opt('-version', 0, [], \
   helpstr="Show version number and exit")
  self.valid_opts.add_opt('-ver', 0, [], \
   helpstr="Show version number and exit")
  self.valid_opts.add_opt('-verb', 1, [], \
   helpstr="Be verbose in messages and options" )
  self.valid_opts.add_opt('-save_script', 1, [], \
   helpstr="save executed script in given file" )

  self.valid_opts.add_opt('-align_centers', 1, ['no'], ['yes', 'no', 'on', 'off'],  \
   helpstr="align centers of datasets based on spatial\n"      \
   "extents of the original volume")
  self.valid_opts.add_opt('-strip_dsets', 1, ['None'],           \
    ['3dSkullStrip', '3dAutomask', 'None'],      \
    helpstr="Remove skull/outside head or neither")
  self.valid_opts.add_opt('-overwrite', 0, [],\
   helpstr="Overwrite existing files")


  def dry_run(self):
    if self.oexec != "dry_run":
     return 0
   else:
     return 1

     def apply_initial_opts(self, opt_list):
  opt1 = opt_list.find_opt('-version') # user only wants version
  opt2 = opt_list.find_opt('-ver')
  if ((opt1 != None) or (opt2 != None)):
     # ps.version()
     ps.ciao(0)   # terminate
  opt = opt_list.find_opt('-verb')    # set and use verb
  if opt != None: self.verb = int(opt.parlist[0])

  opt = opt_list.find_opt('-save_script') # save executed script
  if opt != None: self.save_script = opt.parlist[0]

  # user says it's okay to overwrite existing files
  opt = self.user_opts.find_opt('-overwrite')
  if opt != None:
   print("setting option to rewrite")
   ps.rewrite = 1

  opt = opt_list.find_opt('-ex_mode')    # set execute mode
  if opt != None: self.oexec = opt.parlist[0]

  opt = opt_list.find_opt('-keep_rm_files')    # keep temp files
  if opt != None: self.rmrm = 0

  opt = opt_list.find_opt('-prep_only')    # preprocessing only
  if opt != None: self.prep_only = 1

  opt = opt_list.find_opt('-help')    # does the user want help?
  if opt != None:
     ps.self_help(2)   # always give full help now by default
     ps.ciao(0)  # terminate

  opt = opt_list.find_opt('-limited_help')  # less help?
  if opt != None:
   ps.self_help()
     ps.ciao(0)  # terminate

  opt = opt_list.find_opt('-option_help')  # help for options only
  if opt != None:
   ps.self_help(1)
     ps.ciao(0)  # terminate

     opt = opt_list.find_opt('-suffix')
     if opt != None:
      self.suffix = opt.parlist[0]
      if((opt=="") or (opt==" ")) :
        self.error_msg("Cannot have blank suffix")
        ps.ciao(1);

        def get_user_opts(self):
  self.valid_opts.check_special_opts(sys.argv) #ZSS March 2014
  self.user_opts = read_options(sys.argv, self.valid_opts)
  if self.user_opts == None: return 1 #bad
  # no options: apply -help
  if ( len(self.user_opts.olist) == 0 or \
   len(sys.argv) <= 1 ) :
  ps.self_help()
     ps.ciao(0)  # terminate
     if self.user_opts.trailers:
       opt = self.user_opts.find_opt('trailers')
       if not opt:
         print( "** ERROR: seem to have trailers, but cannot find them!")
       else:
         print( "** ERROR: have invalid trailing args: %s", opt.show())
     return 1  # failure

  # apply the user options
  if self.apply_initial_opts(self.user_opts): return 1

  if self.verb > 3:
   self.show('------ found options ------ ')

   return

   def show(self, mesg=""):
    print('%s: %s' % (mesg, self.label))
    if self.verb > 2: self.valid_opts.show('valid_opts: ')
    self.user_opts.show('user_opts: ')

    def info_msg(self, mesg=""):
     if(self.verb >= 1) :
      print("#++ %s" % mesg)
      def error_msg(self, mesg=""):
       print("#**ERROR %s" % mesg)

       def exists_msg(self, dsetname=""):
         print("** Dataset: %s already exists" % dsetname)
         print("** Not overwriting.")
         if(not ps.dry_run()):
           self.ciao(1)

           def ciao(self, i):
            if i > 0:
             print( "** ERROR - script failed")
           elif i==0:
             print("")

             os.chdir(self.odir)

             if self.save_script:
               write_afni_com_history(self.save_script)

  # return status code
  sys.exit(i)

# save the script command arguments to the dataset history
def save_history(self, dset, exec_mode):
  self.info_msg("Saving history")  # sounds dramatic, doesn't it?
  cmdline = args_as_command(sys.argv, \
   '3dNotes -h "', '" %s' % dset.input())
  com = shell_com(  "%s\n" % cmdline, exec_mode)
  com.run()

# show help
# if help_level is 1, then show options help only
# if help_level is 2, then show main help and options help
def self_help(self, help_level=0):
  if(help_level!=1) :
   print( g_help_string )
   if(help_level):
     print("A full list of options for %s:\n" % ps.label)
     for opt in self.valid_opts.olist:
      print("   %-20s" % (opt.name ))
      if (opt.helpstr != ''):
       print( "   %-20s   %s" % \
        ("   use:", opt.helpstr.replace("\n","\n   %-20s   "%' ')))
       if (opt.acceptlist):
         print( "   %-20s   %s" % \
          ("   allowed:" , str.join(', ',opt.acceptlist)))
         if (opt.deflist):
           print( "   %-20s   %s" % \
            ("   default:",str.join(' ',opt.deflist)))
           return 1

           def version(self):
            self.info_msg("make_template_dask: %s" % self.make_template_version)

# copy dataset 1 to dataset 2
# show message and check if dset1 is the same as dset2
# return non-zero error if can not copy
def copy_dset(self, dset1, dset2, message, exec_mode):
  self.info_msg(message)
  if(dset1.input()==dset2.input()):
   print( "# copy is not necessary")
   return 0
#      if((os.path.islink(dset1.p())) or (os.path.islink(dset2.p()))):
if(dset1.real_input() == dset2.real_input()):
  print( "# copy is not necessary")
  return 0
  ds1 = dset1.real_input()
  ds2 = dset2.real_input()
  ds1s = ds1.replace('/./','/')
  ds2s = ds2.replace('/./','/')
  if(ds1s == ds2s):
    print( "# copy is not necessary - both paths are same" )
    return 0
    print("copying from dataset %s to %s" % (dset1.input(), dset2.input()))
    dset2.delete(exec_mode)
    com = shell_com(  \
      "3dcopy %s %s" % (dset1.input(), dset2.out_prefix()), exec_mode)
    com.run()
    if ((not dset2.exist())and (exec_mode!='dry_run')):
      print( "** ERROR: Could not rename %s\n" % dset1.input())
      return 1
      return 0

## BEGIN script specific functions
def process_input(self):
  #Do the default test on all options entered.
  #NOTE that default options that take no parameters will not go
  #through test, but that is no big deal
  for opt in self.user_opts.olist:
   if (opt.test() == None): ps.ciao(1)

  # skull stripping is off by default
  opt = self.user_opts.find_opt('-strip_dsets')
  ps.skullstrip = 0
  if opt != None :
    ps.skullstrip_method = 'None'
    ps.skullstrip_method = opt.parlist[0]
    if(ps.skullstrip_method=='None'):
     ps.skullstrip = 0
   else:
     ps.skullstrip = 1
     opt = self.user_opts.find_opt('-dsets')
     if opt != None:
       opt = self.user_opts.find_opt('-dsets')
       if opt == None:
        print( "** ERROR: Must use -dsets option to specify input datasets\n")
        ps.ciao(1)
        ps.dsets = self.user_opts.find_opt('-dsets')
        for dset_name in ps.dsets.parlist:
         check_dset = afni_name(dset_name)
         if not check_dset.exist():
          self.error_msg("Could not find dset\n %s "
           % check_dset.input())
        else:
          self.info_msg(
           "Found dset %s\n" % check_dset.input())

          if opt != None:
           opt = self.user_opts.find_opt('-init_base')
           if opt == None:
            print( "** ERROR: Must use -init_base option to specify an initial base\n")
            ps.ciao(1)

            ps.basedset = afni_name(opt.parlist[0])
            if not ps.basedset.exist():
             self.error_msg("Could not find initial base dataset\n %s "
              % ps.basedset.input())
           else:
             self.info_msg(
              "Found initial base dset %s\n" % ps.basedset.input())


# align the center of a dataset to the center of another dataset like a template
#   @delayed
def align_centers(self,dset=None,base=None,suffix="_ac"):
  print("align centers of %s to %s" % (dset.out_prefix(), base.out_prefix()) )

  if(dset.type == 'NIFTI'):
     #copy original to a temporary file
     print("dataset input name is %s" % dset.input())
     ao = strip_extension(dset.input(), ['.nii','nii.gz'])
     print("new AFNI name is %s" % ao[0])
     aao = afni_name("%s"  % (ao[0]))
     aao.to_afni(new_view="+orig")
     o = afni_name("%s%s%s" % (aao.out_prefix(), suffix, aao.view))
   else:
     o = dset.new("%s%s" % (dset.out_prefix(), suffix))

  # use shift transformation of centers between grids as initial
  # transformation. @Align_Centers (3drefit)
  copy_cmd = "3dcopy %s %s" % (dset.input(), o.ppv())
  cmd_str = "%s; @Align_Centers -base %s -dset %s -no_cp" %     \
  (copy_cmd, base.input(), o.input())
  print("executing:\n %s" % cmd_str)
  if (not o.exist() or ps.rewrite or ps.dry_run()):
   o.delete(ps.oexec)
   com = shell_com( cmd_str, ps.oexec)
   com.run();
   if (not o.exist() and not ps.dry_run()):
    print( "** ERROR: Could not align centers using \n  %s\n" % cmd_str)
    return None
  else:
   self.exists_msg(o.input())

   return o

# automask - make simple mask
#   @delayed
def automask(self,dset=None,suffix="_am"):
  print("automask %s" % dset.out_prefix() )

  if(dset.type == 'NIFTI'):
     #copy original to a temporary file
     print("dataset input name is %s" % dset.input())
     ao = strip_extension(dset.input(), ['.nii','nii.gz'])
     print("new AFNI name is %s" % ao[0])
     aao = afni_name("%s"  % (ao[0]))
     aao.to_afni(new_view="+orig")
     o = afni_name("%s%s%s" % (aao.out_prefix(), suffix, aao.view))
   else:
     o = dset.new("%s%s" % (dset.out_prefix(), suffix))
     cmd_str = "3dAutomask -apply_prefix %s %s" %     \
     (o.out_prefix(), dset.input())
     print("executing:\n %s" % cmd_str)
     if (not o.exist() or ps.rewrite or ps.dry_run()):
       o.delete(ps.oexec)
       com = shell_com( cmd_str, ps.oexec)
       com.run();
       if (not o.exist() and not ps.dry_run()):
        print( "** ERROR: Could not unifize using \n  %s\n" % cmd_str)
        return None
      else:
       self.exists_msg(o.input())

       return o
# unifize - bias-correct a dataset
#   @delayed
def unifize(self,dset=None,suffix="_un"):
  print("unifize %s" % dset.out_prefix() )

  if(dset.type == 'NIFTI'):
     #copy original to a temporary file
     print("dataset input name is %s" % dset.input())
     ao = strip_extension(dset.input(), ['.nii','nii.gz'])
     print("new AFNI name is %s" % ao[0])
     aao = afni_name("%s"  % (ao[0]))
     aao.to_afni(new_view="+orig")
     o = afni_name("%s%s%s" % (aao.out_prefix(), suffix, aao.view))
   else:
     o = dset.new("%s%s" % (dset.out_prefix(), suffix))
     cmd_str = "3dUnifize -gm -prefix %s -input %s" %     \
     (o.out_prefix(), dset.input())
     print("executing:\n %s" % cmd_str)
     if (not o.exist() or ps.rewrite or ps.dry_run()):
       o.delete(ps.oexec)
       com = shell_com( cmd_str, ps.oexec)
       com.run();
       if (not o.exist() and not ps.dry_run()):
        print( "** ERROR: Could not unifize using \n  %s\n" % cmd_str)
        return None
      else:
       self.exists_msg(o.input())

       return o


# Main:
if __name__ == '__main__':

  ps = RegWrap('make_template_dask.py')

  ps.init_opts()
  ps.version()
  rv = ps.get_user_opts()
  if (rv != None): ps.ciao(1)

#process and check input params
ps.process_input()
#if(not (ps.process_input())):
#   ps.ciao(1)

# get rid of any previous temporary data
# ps.cleanup()
# setup a scheduler. the cluster object will manage this
# It's important to constrain the workers to ones that  default to the same networking.
# We can't mix infiniband with 10g ethernet nodes.
# The constraint argument to sbatch allows us to specify the ethernet nodes
cluster = SLURMCluster(
 queue='quick',
 memory =  "8g",
 processes=1,
 threads = 4,
 job_extra = ['--constraint=10g'] )
#  interface = 'ib0',

print("starting %d workers!" % len(ps.dsets.parlist))
# start workers for each and every subject
cluster.start_workers(len(ps.dsets.parlist))


# Create a Client object to use the cluster we set up
c = Client(cluster)

# coopt align_centers for delayed
align_centers = delayed(ps.align_centers)
automask = delayed(ps.automask)
unifize = delayed(ps.unifize)

alldnames = []
# Lets use the cluster we setup using dask delayed
# from dask import delayed
for dset_name in ps.dsets.parlist:
  start_dset = afni_name(dset_name)
  # start off just aligning the centers of the datasets
  aname = ps.align_centers(dset=start_dset, base=ps.basedset)
  amname = ps.automask(dset=aname)
  dname = ps.unifize(dset=amname)

  alldnames.append(dname)

  print("Configured first processing loop")

# The following command executes the task graph that
# alldnames represents. This is non-blocking. We can continue
# our python session. Whenever we query the affine object
# we will be informed of its status.
affine = c.compute(alldnames)

# This is a blocking call and will return the results.
# We could run this immediately or wait until affine shows
# that the computation is finished.
c.gather(affine)

ps.ciao(0)

