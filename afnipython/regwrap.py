import sys
import os

# sys.path.append('/data/NIMH_SSCC/template_making/scripts')

# AFNI modules
import afni_base as ab
import afni_util as au
from option_list import OptionList, read_options
class RegWrap:
    def __init__(self, label):
        # software version (update for changes)
        self.make_template_version = "0.01"
        # user assigned path for anat and EPI
        self.output_dir = 'iterative_template_dir'

        self.label = label
        self.valid_opts = None
        self.user_opts = None
        self.verb = 1    # a little talkative by default
        self.save_script = ''  # save completed script into given file
        self.rewrite = 0  # Do not recreate existing volumes
        self.oexec = ""  # dry_run is an option
        self.rmrm = 1   # remove temporary files
        self.prep_only = 0  # do preprocessing only
        self.odir = os.getcwd()

        return

    def init_opts(self):

        self.valid_opts = OptionList('init_opts')

        # input datasets
        self.valid_opts.add_opt('-dsets', -1, [],
                                helpstr="Names of datasets")
        self.valid_opts.add_opt('-init_base', 1, [],
                                helpstr="Name of initial base dataset")

        self.valid_opts.add_opt('-keep_rm_files', 0, [],
                                helpstr="Don't delete any of the temporary files created here")
        self.valid_opts.add_opt('-prep_only', 0, [],
                                helpstr="Do preprocessing steps only without alignment")
        self.valid_opts.add_opt('-help', 0, [],
                                helpstr="The main help describing this program with options")
        self.valid_opts.add_opt('-limited_help', 0, [],
                                helpstr="The main help without all available options")
        self.valid_opts.add_opt('-option_help', 0, [],
                                helpstr="Help for all available options")
        self.valid_opts.add_opt('-version', 0, [],
                                helpstr="Show version number and exit")
        self.valid_opts.add_opt('-ver', 0, [],
                                helpstr="Show version number and exit")
        self.valid_opts.add_opt('-verb', 1, [],
                                helpstr="Be verbose in messages and options")
        self.valid_opts.add_opt('-save_script', 1, [],
                                helpstr="save executed script in given file")

        self.valid_opts.add_opt('-align_centers', 1, ['no'], ['yes', 'no', 'on', 'off'],
                                helpstr="align centers of datasets based on spatial\n"
                                "extents of the original volume")
        self.valid_opts.add_opt('-strip_dsets', 1, ['None'],
                                ['3dSkullStrip', '3dAutomask', 'None'],
                                helpstr="Remove skull/outside head or neither")
        self.valid_opts.add_opt('-overwrite', 0, [],
                                helpstr="Overwrite existing files")

    def dry_run(self):
        if self.oexec != "dry_run":
            return 0
        else:
            return 1

    def apply_initial_opts(self, opt_list):
        opt1 = opt_list.find_opt('-version')  # user only wants version
        opt2 = opt_list.find_opt('-ver')
        if ((opt1 != None) or (opt2 != None)):
            # self.version()
            self.ciao(0)   # terminate
        opt = opt_list.find_opt('-verb')    # set and use verb
        if opt != None:
            self.verb = int(opt.parlist[0])

        opt = opt_list.find_opt('-save_script')  # save executed script
        if opt != None:
            self.save_script = opt.parlist[0]

        # user says it's okay to overwrite existing files
        opt = self.user_opts.find_opt('-overwrite')
        if opt != None:
            print("setting option to rewrite")
            self.rewrite = 1

        opt = opt_list.find_opt('-ex_mode')    # set execute mode
        if opt != None:
            self.oexec = opt.parlist[0]

        opt = opt_list.find_opt('-keep_rm_files')    # keep temp files
        if opt != None:
            self.rmrm = 0

        opt = opt_list.find_opt('-prep_only')    # preprocessing only
        if opt != None:
            self.prep_only = 1

        opt = opt_list.find_opt('-help')    # does the user want help?
        if opt != None:
            self.self_help(2)   # always give full help now by default
            self.ciao(0)  # terminate

        opt = opt_list.find_opt('-limited_help')  # less help?
        if opt != None:
            self.self_help()
            self.ciao(0)  # terminate

        opt = opt_list.find_opt('-option_help')  # help for options only
        if opt != None:
            self.self_help(1)
            self.ciao(0)  # terminate

        opt = opt_list.find_opt('-suffix')
        if opt != None:
            self.suffix = opt.parlist[0]
            if((opt == "") or (opt == " ")):
                self.error_msg("Cannot have blank suffix")
                self.ciao(1)

    def get_user_opts(self):
        self.valid_opts.check_special_opts(sys.argv)  # ZSS March 2014
        self.user_opts = read_options(sys.argv, self.valid_opts)
        if self.user_opts == None:
            return 1  # bad
        # no options: apply -help
        if (len(self.user_opts.olist) == 0 or len(sys.argv) <= 1):
            self.self_help()
            self.ciao(0)  # terminate
        if self.user_opts.trailers:
            opt = self.user_opts.find_opt('trailers')
            if not opt:
                print("** ERROR: seem to have trailers, but cannot find them!")
            else:
                print("** ERROR: have invalid trailing args: %s", opt.show())
            return 1  # failure

        # apply the user options
        if self.apply_initial_opts(self.user_opts):
            return 1

        if self.verb > 3:
            self.show('------ found options ------ ')

        return

    def show(self, mesg=""):
        print('%s: %s' % (mesg, self.label))
        if self.verb > 2:
            self.valid_opts.show('valid_opts: ')
        self.user_opts.show('user_opts: ')

    def info_msg(self, mesg=""):
        if(self.verb >= 1):
            print("#++ %s" % mesg)

    def error_msg(self, mesg=""):
        print("#**ERROR %s" % mesg)

    def exists_msg(self, dsetname=""):
        print("** Dataset: %s already exists" % dsetname)
        print("** Not overwriting.")
        if(not self.dry_run()):
            self.ciao(1)

    def ciao(self, i):
        if i > 0:
            print("** ERROR - script failed")
        elif i == 0:
            print("")

        os.chdir(self.odir)

        if self.save_script:
            au.write_afni_com_history(self.save_script)

        # return status code
        sys.exit(i)

        # save the script command arguments to the dataset history
    def save_history(self, dset, exec_mode):
        self.info_msg("Saving history")  # sounds dramatic, doesn't it?
        cmdline = au.args_as_command(sys.argv,
                                  '3dNotes -h "', '" %s' % dset.input())
        com = ab.shell_com("%s\n" % cmdline, exec_mode)
        com.run()

        # show help
        # if help_level is 1, then show options help only
        # if help_level is 2, then show main help and options help
    def self_help(self, help_level=0):
        if(help_level != 1):
            g_help_string = "What is dis??"
            print(g_help_string)
        if(help_level):
            print("A full list of options for %s:\n" % self.label)
            for opt in self.valid_opts.olist:
                print("   %-20s" % (opt.name))
                if (opt.helpstr != ''):
                    print("   %-20s   %s" %
                          ("   use:", opt.helpstr.replace("\n", "\n   %-20s   " % ' ')))
                if (opt.acceptlist):
                    print("   %-20s   %s" %
                          ("   allowed:", str.join(', ', opt.acceptlist)))
                if (opt.deflist):
                    print("   %-20s   %s" %
                          ("   default:", str.join(' ', opt.deflist)))
        return 1

    def version(self):
        self.info_msg("make_template_dask: %s" % self.make_template_version)

        # copy dataset 1 to dataset 2
        # show message and check if dset1 is the same as dset2
        # return non-zero error if can not copy
    def copy_dset(self, dset1, dset2, message, exec_mode):
        self.info_msg(message)
        if(dset1.input() == dset2.input()):
            print("# copy is not necessary")
            return 0
        #      if((os.path.islink(dset1.p())) or (os.path.islink(dset2.p()))):
        if(dset1.real_input() == dset2.real_input()):
            print("# copy is not necessary")
            return 0
        ds1 = dset1.real_input()
        ds2 = dset2.real_input()
        ds1s = ds1.replace('/./', '/')
        ds2s = ds2.replace('/./', '/')
        if(ds1s == ds2s):
            print("# copy is not necessary - both paths are same")
            return 0
        print("copying from dataset %s to %s" % (dset1.input(), dset2.input()))
        dset2.delete(exec_mode)
        com = ab.shell_com(
            "3dcopy %s %s" % (dset1.input(), dset2.out_prefix()), exec_mode)
        com.run()
        if ((not dset2.exist())and (exec_mode != 'dry_run')):
            print("** ERROR: Could not rename %s\n" % dset1.input())
            return 1
        return 0

        # BEGIN script specific functions
    def process_input(self):
        # Do the default test on all options entered.
        # NOTE that default options that take no parameters will not go
        # through test, but that is no big deal
        for opt in self.user_opts.olist:
            if (opt.test() == None):
                self.ciao(1)

        # skull stripping is off by default
        opt = self.user_opts.find_opt('-strip_dsets')
        self.skullstrip = 0
        if opt != None:
            self.skullstrip_method = 'None'
            self.skullstrip_method = opt.parlist[0]
            if(self.skullstrip_method == 'None'):
                self.skullstrip = 0
            else:
                self.skullstrip = 1
        opt = self.user_opts.find_opt('-dsets')
        if opt != None:
            opt = self.user_opts.find_opt('-dsets')
            if opt == None:
                print("** ERROR: Must use -dsets option to specify input datasets\n")
                self.ciao(1)
        self.dsets = self.user_opts.find_opt('-dsets')
        for dset_name in self.dsets.parlist:
            check_dset = ab.afni_name(dset_name)
            if not check_dset.exist():
                self.error_msg("Could not find dset\n %s "
                               % check_dset.input())
            else:
                self.info_msg(
                    "Found dset %s\n" % check_dset.input())

        if opt != None:
            opt = self.user_opts.find_opt('-init_base')
            if opt == None:
                print(
                    "** ERROR: Must use -init_base option to specify an initial base\n")
                self.ciao(1)

            self.basedset = ab.afni_name(opt.parlist[0])
            if not self.basedset.exist():
                self.error_msg("Could not find initial base dataset\n %s "
                               % self.basedset.input())
            else:
                self.info_msg(
                    "Found initial base dset %s\n" % self.basedset.input())

