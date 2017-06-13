import os
import click
import itertools
import functools
import subprocess
import re

SLURM_SCRIPT = '''
#!/bin/bash
# Job name:
#SBATCH --job-name=resid
#
# Partition:
#SBATCH --partition=savio2
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
# Wall clock limit:
#SBATCH --time=72:00:00
#
#SBATCH --array=0-{numjobs}
#
#SBATCH --requeue
{dependencies}

## Run command
python {filepath} {flags} --job_id ${{SLURM_ARRAY_TASK_ID}}
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


def _prep_slurm(filepath, job_spec, dependencies=None, flags=None):
    if dependencies:
        depstr = '\n'.join([
            '#',
            '#SBATCH --dependency=afterok:{}'.format(
                ','.join(map(str, dependencies)))])

    else:
        depstr = ''

    if flags:
        flagstr = ' '.join(map(str, flags))
    else:
        flagstr = ''

    with open('do_job.sh', 'w+') as f:
        f.write(SLURM_SCRIPT.format(
            numjobs = len(list(generate_jobs(job_spec))),
            filepath = filepath,
            dependencies = depstr,
            flags = flagstr))


def run_slurm(filepath, job_spec, dependencies=None, flags=None):
    _prep_slurm(filepath, job_spec, dependencies)

    job_command = ['sbatch', 'do_job.sh']

    proc = subprocess.Popen(
        job_command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)

    out, err = proc.communicate()

    matcher = re.search(r'^\s*Submitted batch job (?P<run_id>[0-9]+)\s*$', out)

    if matcher:
        run_id = int(matcher.group('run_id'))
    else:
        run_id = None

    if err:
        raise OSError('Error encountered submitting job: {}'.format(err))

    return run_id


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
