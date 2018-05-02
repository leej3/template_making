#!/usr/bin/env python
# -*- coding: utf-8 -*-

# way to run script example
# python make_template_dask.py \
#  -dsets /dsets/*.HEAD -init_base ~/abin/MNI_2009c.nii.gz -ex_mode dry_run
#
# command working for john:
# python make_template_dask.py -dsets /data/DSST/template_making/testdata/*.nii.gz -init_base /usr/local/apps/afni/current/linux_openmp_64/MNI152_2009_template.nii.gz
import sys
import socket
from shutil import which
# AFNI modules
import afni_base as ab
import afni_util as au
from regwrap import RegWrap

# parallelization library
try:
    from dask_jobqueue import SLURMCluster
    from dask import delayed
    from dask.distributed import Client
except:
    print("Can't find dask stuff. Going to run in 'excruciatingly-slow-mode'")
    def delayed(fn):
        return fn

sys.path.append('/data/DSST/template_making/scripts')


if 'felix.nimh.nih.gov' == socket.gethostname():
    raise EnvironmentError("Need to run from cluster node. Not Felix")

if not which('3dinfo'):
    except EnvironmentError("Is AFNI on your path?")

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
# BEGIN common functions across scripts (loosely of course)

class RegWrapTemplate(RegWrap):

        # align the center of a dataset to the center of another dataset like a template
    @delayed
    def align_centers(self, dset=None, base=None, suffix="_ac"):
        print("align centers of %s to %s" %
              (dset.out_prefix(), base.out_prefix()))

        if(dset.type == 'NIFTI'):
            # copy original to a temporary file
            print("dataset input name is %s" % dset.input())
            ao = ab.strip_extension(dset.input(), ['.nii', 'nii.gz'])
            print("new AFNI name is %s" % ao[0])
            aao = ab.afni_name("%s" % (ao[0]))
            aao.to_afni(new_view="+orig")
            o = ab.afni_name("%s%s%s" % (aao.out_prefix(), suffix, aao.view))
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
            com = ab.shell_com(cmd_str, ps.oexec)
            com.run()
            if (not o.exist() and not ps.dry_run()):
                print("** ERROR: Could not align centers using \n  %s\n" % cmd_str)
                return None
        else:
            self.exists_msg(o.input())

        return o

        # automask - make simple mask
    @delayed
    def automask(self, dset=None, suffix="_am"):
        print("automask %s" % dset.out_prefix())

        if(dset.type == 'NIFTI'):
            # copy original to a temporary file
            print("dataset input name is %s" % dset.input())
            ao = ab.strip_extension(dset.input(), ['.nii', 'nii.gz'])
            print("new AFNI name is %s" % ao[0])
            aao = ab.afni_name("%s" % (ao[0]))
            aao.to_afni(new_view="+orig")
            o = ab.afni_name("%s%s%s" % (aao.out_prefix(), suffix, aao.view))
        else:
            o = dset.new("%s%s" % (dset.out_prefix(), suffix))
        cmd_str = "3dAutomask -apply_prefix %s %s" %     \
            (o.out_prefix(), dset.input())
        print("executing:\n %s" % cmd_str)
        if (not o.exist() or ps.rewrite or ps.dry_run()):
            o.delete(ps.oexec)
            com = ab.shell_com(cmd_str, ps.oexec)
            com.run()
            if (not o.exist() and not ps.dry_run()):
                print("** ERROR: Could not unifize using \n  %s\n" % cmd_str)
                return None
        else:
            self.exists_msg(o.input())

        return o
        # unifize - bias-correct a dataset
    @delayed
    def unifize(self, dset=None, suffix="_un"):
        print("unifize %s" % dset.out_prefix())

        if(dset.type == 'NIFTI'):
            # copy original to a temporary file
            print("dataset input name is %s" % dset.input())
            ao = ab.strip_extension(dset.input(), ['.nii', 'nii.gz'])
            print("new AFNI name is %s" % ao[0])
            aao = ab.afni_name("%s" % (ao[0]))
            aao.to_afni(new_view="+orig")
            o = ab.afni_name("%s%s%s" % (aao.out_prefix(), suffix, aao.view))
        else:
            o = dset.new("%s%s" % (dset.out_prefix(), suffix))
        cmd_str = "3dUnifize -gm -prefix %s -input %s" %     \
            (o.out_prefix(), dset.input())
        print("executing:\n %s" % cmd_str)
        if (not o.exist() or ps.rewrite or ps.dry_run()):
            o.delete(ps.oexec)
            com = ab.shell_com(cmd_str, ps.oexec)
            com.run()
            if (not o.exist() and not ps.dry_run()):
                print("** ERROR: Could not unifize using \n  %s\n" % cmd_str)
                return None
        else:
            self.exists_msg(o.input())

        return o


# Main:
if __name__ == '__main__':

    ps = RegWrapTemplate('make_template_dask.py')

    ps.init_opts()
    ps.version()
    rv = ps.get_user_opts()
    if rv is not None: ps.ciao(1)

    # process and check input params
    ps.process_input()
    # if(not (ps.process_input())): ps.ciao(1)

    # setup a scheduler. the cluster object will manage this
    # It's important to constrain the workers to ones that
    # default to the same networking.
    # We can't mix infiniband with 10g ethernet nodes.
    cluster = SLURMCluster(
        queue='quick',
        memory =  "8g",
        processes=1,
        threads = 4,
        job_extra = ['--constraint=10g'] )
    # n_workers = ps.dsets.parlist
    n_workers = 2
    print("starting %d workers!" % n_workers)
    cluster.start_workers(n_workers)

    client = Client(cluster)
    # coopt align_centers for delayed
    # align_centers = delayed(ps.align_centers)
    # automask = delayed(ps.automask)
    # unifize = delayed(ps.unifize)

    alldnames = []
    # from dask import delayedsetup using dask delayed
    for dset_name in ps.dsets.parlist:
        start_dset = ab.afni_name(dset_name)
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
    affine = client.compute(alldnames)
    # This is a blocking call and will return the results.
    # We could run this immediately or wait until affine shows
    # that the computation is finished.
    import pdb;pdb.set_trace()
    client.gather(affine)

    ps.ciao(0)
