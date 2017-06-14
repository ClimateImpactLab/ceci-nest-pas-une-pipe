import os
import click
import itertools
import functools
import subprocess
import re

SLURM_SCRIPT = '''
#!/bin/bash
# Job name:
#SBATCH --job-name={jobname}
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
#SBATCH --requeue
{jobs}
{dependencies}

## Run command
python {filepath} {flags}
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


def _prep_slurm(filepath, jobname='slurm_job', job_spec=None, dependencies=None, flags=None):
    depstr = ''

    if (dependencies is not None) and (len(dependencies) > 1):
        status, deps = dependencies

        if len(deps) > 0:

            depstr += '\n'.join([
                '#',
                '#SBATCH --dependency=afterok:{}'.format(
                    ','.join(map(str, deps)))])

    if flags:
        flagstr = ' '.join(map(str, flags))
    else:
        flagstr = ''

    if job_spec:
        jobstr = '\n'.join([
            '#',
            '#SBATCH --array=0-{}'.format(
                len(list(generate_jobs(job_spec))))])
        
        flagstr = flagstr + ' --job_id ${SLURM_ARRAY_TASK_ID}'

    else:
        jobstr = ''

    with open('do_job.sh', 'w+') as f:
        f.write(SLURM_SCRIPT.format(
            jobname = jobname,
            jobs = jobstr,
            filepath = filepath,
            dependencies = depstr,
            flags = flagstr))


def run_slurm(filepath, jobname='slurm_job', job_spec=None, dependencies=None, flags=None):
    _prep_slurm(filepath, jobname, job_spec, dependencies, flags)

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


def slurm_runner(job_spec, run_job, onfinish, additional_metadata=None):

    @click.group()
    def slurm():
        pass

    @slurm.command()
    @click.option('--dependency', '-d', type=int, multiple=True)
    def prep(dependency=False):
        _prep_slurm(
            filepath=__file__,
            job_spec=job_spec,
            dependencies=('afterany', list(dependency)),
            flags=['do_job'])

    @slurm.command()
    @click.option('--jobname', default='slurm_job', help='name of the job')
    @click.option('--dependency', '-d', type=int, multiple=True)
    def run(jobname='slurm_job', dependency=None):
        slurm_id = run_slurm(
            filepath=__file__,
            jobname=jobname,
            job_spec=job_spec,
            dependencies=('afterany', list(dependency)),
            flags=['do_job'])

        finish_id = run_slurm(
            filepath=__file__,
            jobname=jobname+'_finish',
            dependencies=('afterany', [slurm_id]),
            flags=['cleanup', slurm_id])

        print('run job: {}\non-finish job: {}'.format(slurm_id, finish_id))


    @slurm.command()
    @click.argument('slurm_id')
    def cleanup(slurm_id):
        proc = subprocess.Popen(
            ['sacct', '-j', slurm_id, '--format=JobID,JobName,MaxRSS,Elapsed,State'],
            stdout = subprocess.PIPE,
            stderr = subprocess.PIPE)

        out, err = proc.communicate()

        print(out)
        onfinish()


    @slurm.command()
    @click.option('--job_id', required=True, type=int)
    def do_job(job_id=None):

        job = get_job_by_index(job_spec, job_id)
        
        metadata = {}
        
        if additional_metadata is not None:
            metadata.update(
                {k: str(v) for k, v in additional_metadata.items()})

        metadata.update({k: str(v) for k, v in job.items()})

        run_job(metadata=metadata, **job)

    return slurm
