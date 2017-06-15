import os
import click
import itertools
import functools

SLURM_SCRIPT = '''
#!/bin/bash
# Job name:
#SBATCH --job-name=tasmax_test
#
# Partition:
#SBATCH --partition=savio2_bigmem
#
# Account:
#SBATCH --account=co_laika
#
# QoS:
#SBATCH --qos=savio_lowprio
#
#SBATCH --nodes=1
#
#SBATCH --ntasks-per-node=24
#
#SBATCH  --cpus-per-task=1
#
#SBATCH --requeue
#
# Wall clock limit:
#SBATCH --time=72:00:00
#
#SBATCH --array=0-{numjobs}

## Run command
python {filepath} --job_id ${{SLURM_ARRAY_TASK_ID}}
'''.strip()


def _product(values):
    '''
    Examples
    --------

    .. code-block:: python

        >>> _product([3, 4, 5])
        60

    '''
    return functools.reduce(lambda x, y: x*y, values, 1)


def _unpack_job(specs):
    job = {}
    for spec in specs:
        job.update(spec)
    return job


def generate_jobs(job_spec):
    for specs in itertools.product(*job_spec):
        yield _unpack_job(specs)


def _prep_slurm(filepath, job_spec):
    with open('do_job.sh', 'w+') as f:
        f.write(SLURM_SCRIPT.format(
            numjobs = len(list(generate_jobs(job_spec)))-1,
            filepath = filepath))


def run_slurm(filepath, job_spec):
    _prep_slurm(filepath, job_spec)
    os.system('sbatch do_job.sh')


def get_job_by_index(job_spec, index):
    '''
    Examples
    --------

    .. code-block:: python

        >>> get_job_by_index(
        ...    (('a', 'b', 'c'), (1, 2, 3), ('do', 'rey', 'mi')), 5)
        ('a', 2, 'mi')

        >>> get_job_by_index(map(tuple, ['hi', 'hello', 'bye']), 10)
        ('h', 'l', 'y')


    '''

    return _unpack_job([
        job_spec[i][
            (index//(_product(map(len, job_spec[i+1:])))%len(job_spec[i]))]
        for i in range(len(job_spec))])
