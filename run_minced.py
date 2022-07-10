#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import logging
import argparse

sys.path.append("/project/hgptemp/pipeline/v1.1.1/")
from dagflow import DAG, Task, ParallelTask, do_dag
from ngsmetavirus.common import mkdir, check_paths
LOG = logging.getLogger(__name__)
__version__ = "1.0.0"
__author__ = ("Xingguo Zhang",)
__email__ = "invicoun@foxmail.com"
__all__ = []

QUEUE = ""
#QUEUE = "-q all.q"
MINCRED_BIN = "/project/hgptemp/software/minced/v0.4.2/"
GFFVERT = "/project/hgptemp/script/gffvert"

def create_minced_tasks(genomes, job_type, work_dir="", out_dir=""):

    prefixs = [os.path.basename(i) for i in genomes]

    id = "minced"
    tasks = ParallelTask(
        id=id,
        work_dir=work_dir,
        type=job_type,
        option="-pe smp 2 %s" % QUEUE,
        script="""
export PATH={minced_bin}:$PATH
#minced {{genomes}} {{prefixs}}.crisprs {{prefixs}}.gff
row=`grep -v "#" {{prefixs}}.gff|wc -l`
if [ $row -gt 0 ];then
   {gffvert} get_seq {{prefixs}}.gff -g {{genomes}} >{{prefixs}}.crisprs.fsta
   cp {{genomes}} {{prefixs}}.crisprs.fsta {{prefixs}}.gff {out_dir}
fi
""".format(minced_bin=MINCRED_BIN,
            gffvert=GFFVERT,
            out_dir=out_dir
            ),
        genomes=genomes,
        prefixs=prefixs,
    )

    return tasks


def run_minced(genomes, job_type, work_dir, out_dir,
              concurrent=10, refresh=30):

    genomes = check_paths(genomes)
    work_dir = mkdir(work_dir)
    out_dir = mkdir(out_dir)

    dag = DAG("run_minced")

    tasks = create_minced_tasks(
        genomes=genomes,
        job_type=job_type,
        work_dir=work_dir,
        out_dir=out_dir
    )
    dag.add_task(*tasks)

    do_dag(dag, concurrent_tasks=concurrent, refresh_time=refresh)

    return 0


def add_hlep_args(parser):

    parser.add_argument("input", nargs="+", metavar="FILE", type=str,
        help="Input the original fasta file.")
    parser.add_argument("--concurrent", metavar="INT", type=int, default=10,
        help="Maximum number of jobs concurrent  (default: 10)")
    parser.add_argument("--refresh", metavar="INT", type=int, default=30,
        help="Refresh time of log in seconds  (default: 30)")
    parser.add_argument("--job_type", choices=["sge", "local"], default="local",
        help="Jobs run on [sge, local]  (default: local)")
    parser.add_argument("--work_dir", metavar="DIR", type=str, default="work",
        help="Work directory (default: current directory)")
    parser.add_argument("--out_dir", metavar="DIR", type=str, default="./",
        help="Output directory (default: current directory)")

    return parser


def main():

    logging.basicConfig(
        stream=sys.stderr,
        level=logging.INFO,
        format="[%(levelname)s] %(message)s"
    )
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
    description='''
URL: https://github.com/zxgsy520/gfungi
name:
    bioyimamas.py: Microbiology and Metabolism Association Analysis

attention:
    bioyimamas.py

version: %s
contact:  %s <%s>\
        ''' % (__version__, ' '.join(__author__), __email__))

    args = add_hlep_args(parser).parse_args()

    run_minced(args.input, args.job_type, args.work_dir, args.out_dir,
        args.concurrent, args.refresh)


if __name__ == "__main__":

    main()
