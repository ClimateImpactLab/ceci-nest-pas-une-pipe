import os
import click
import itertools
import functools

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
    if dependencies is None:
        depstr = ''

    else:
        depstr = '\n'.join([
            '#',
            '#SBATCH --dependency=afterok:{}'.format(
                ','.join(map(str, dependencies)))])

    if flags is None:
        flagstr = ''
    else:
        flagstr = ' '.join(map(str, flags))

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

    out, err = subprocess.communicate()

    print(out)
    print(err)


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
