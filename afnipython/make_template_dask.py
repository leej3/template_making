#!/usr/bin/env python
# -*- coding: utf-8 -*-

# way to run script example
# python make_template_dask.py \
#  -dsets /dsets/*.HEAD -init_base ~/abin/MNI_2009c.nii.gz -ex_mode dry_run
#
# command working for john:
# python make_template_dask.py -cluster "SlurmCluster" -dsets /data/DSST/template_making/testdata/*.nii.gz -init_base /usr/local/apps/afni/current/linux_openmp_64/MNI152_2009_template.nii.gz
bad_host_strings = ['felix','helix','biowulf']
import socket
from shutil import which
if any(pattern in socket.gethostname() for pattern in bad_host_strings):
    raise EnvironmentError("Need to run from a cluster node.")

if not which('3dinfo'):
    raise EnvironmentError("Is AFNI on your path?")


from afnipython.regwrap import RegWrap
ps = RegWrap('make_template_dask.py')
ps.init_opts()
ps.version()
rv = ps.get_user_opts()
ps.process_input()
if rv is not None: ps.ciao(1)
n_workers = len(ps.dsets.parlist)

from dask import delayed
# AFNI modules
from afnipython.construct_template_graph import get_task_graph
# parallelization library
try:
    # TODO: generalize to other clusters
    from dask_jobqueue import SLURMCluster
    from dask.distributed import Client
    cluster = SLURMCluster(
        queue='nimh',
        memory =  "8g",
        processes=1,
        threads = 4,
        job_extra = ['--constraint=10g'] )
    print("starting %d workers!" % n_workers)
    cluster.start_workers(n_workers)
    client = Client(cluster)
    using_cluster = True
except ImportError as err:
    try:
        from distributed import Client, LocalCluster
        client = Client()
        using_cluster = False
    except ImportError as e:
        print("Import error: {0}".format(err))

    else: raise(err)

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

# Main:
if __name__ == '__main__':

    task_graph = get_task_graph(ps,delayed,client)

    # The following command executes the task graph that
    import pdb;pdb.set_trace()
    affine = client.compute(task_graph)

    # This is a blocking call and will return the results.
    # We could run this immediately or wait until affine shows
    # that the computation is finished.
    result = client.gather(affine)

    ps.ciao(0)
