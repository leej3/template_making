import afnipython.afni_base as ab

def rigid_align(dset,base,suffix="_a4rigid"):

    o = dset.new("%s_temp%s" % (dset.out_prefix(), suffix))

    cmd_str = """\
    align_epi_anat.py -dset1 {dset.input()} -dset2 {base.input()} \
    -dset1_strip None -dset2_strip None \
    -giant_move -suffix _temp_{suffix}; \
    cat_matvec strip_alaea1_mat.aff12.1D \
    -P >> strip_alaea1_mat_rigid.aff12.1D; \
    3dAllineate -1Dmatrix_apply strip_alaea1_mat_rigid.aff12.1D \
    -master ~/TT_N27+tlrc -prefix rigid_01 -input strip+orig.
    """
    cmd_str = cmd_str.format(**locals())
    print("executing:\n %s" % cmd_str)
    
    if (not o.exist() or regwrap.rewrite or regwrap.dry_run()):
        o.delete(regwrap.oexec)
        com = ab.shell_com(cmd_str, regwrap.oexec)
        com.run()
        if (not o.exist() and not regwrap.dry_run()):
            print("** ERROR: Could not align centers using \n  %s\n" % cmd_str)
            return None
    else:
        regwrap.exists_msg(o.input())

    align_epi_anat.py -dset1 strip+orig. -dset2 ~/abin/TT_N27+tlrc. -dset1_strip None -dset2_strip None -suffix _alaea1
    cat_matvec strip_alaea1_mat.aff12.1D -P >> strip_alaea1_mat_rigid.aff12.1D
    3dAllineate -1Dmatrix_apply strip_alaea1_mat_rigid.aff12.1D -master ~/TT_N27+tlrc -prefix rigid_01 -input strip+orig.


def align_centers(regwrap, dset=None, base=None, suffix="_ac"):
    # align the center of a dataset to the center of another
    # dataset like a template
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
    if (not o.exist() or regwrap.rewrite or regwrap.dry_run()):
        o.delete(regwrap.oexec)
        com = ab.shell_com(cmd_str, regwrap.oexec)
        com.run()
        if (not o.exist() and not regwrap.dry_run()):
            print("** ERROR: Could not align centers using \n  %s\n" % cmd_str)
            return None
    else:
        regwrap.exists_msg(o.input())

    return o


def automask(regwrap, dset=None, suffix="_am"):
    # automask - make simple mask
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
    if (not o.exist() or regwrap.rewrite or regwrap.dry_run()):
        o.delete(regwrap.oexec)
        com = ab.shell_com(cmd_str, regwrap.oexec)
        com.run()
        if (not o.exist() and not regwrap.dry_run()):
            print("** ERROR: Could not unifize using \n  %s\n" % cmd_str)
            return None
    else:
        regwrap.exists_msg(o.input())

    return o


def unifize(regwrap, dset=None, suffix="_un"):
    # unifize - bias-correct a dataset
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
    if (not o.exist() or regwrap.rewrite or regwrap.dry_run()):
        o.delete(regwrap.oexec)
        com = ab.shell_com(cmd_str, regwrap.oexec)
        com.run()
        if (not o.exist() and not regwrap.dry_run()):
            print("** ERROR: Could not unifize using \n  %s\n" % cmd_str)
            return None
    else:
        regwrap.exists_msg(o.input())

    return o


def get_task_graph(ps,delayed,client):

    # process and check input params
    ps.process_input()

    task_graph = []
    # from dask import delayedsetup using dask delayed
    for dset_name in ps.dsets.parlist:
        start_dset = ab.afni_name(dset_name)
        # start off just aligning the centers of the datasets
        aname = delayed(align_centers)(ps,dset=start_dset, base=ps.basedset)
        amname = delayed(automask)(ps,dset=aname)
        dname = delayed(unifize)(ps,dset=amname)


        # alldnames represents. This is non-blocking. We can continue
        # our python session. Whenever we query the affine object
        # we will be informed of its status.
        task_graph.append(dname)

    print("Configured first processing loop")
    return task_graph