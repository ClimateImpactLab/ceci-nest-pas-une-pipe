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
#SBATCH --partition={partition}
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
{output}

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


def _prep_slurm(
        filepath,
        jobname='slurm_job',
        partition='savio2',
        job_spec=None,
        num_jobs=None,
        dependencies=None,
        flags=None):

    depstr = ''

    if (dependencies is not None) and (len(dependencies) > 1):
        status, deps = dependencies

        if len(deps) > 0:

            depstr += (
                '#\n#SBATCH --dependency={}:{}'
                    .format(status, ','.join(map(str, deps))))

    if flags:
        flagstr = ' '.join(map(str, flags))
    else:
        flagstr = ''

    if job_spec:
        n = len(list(generate_jobs(job_spec)))

        if num_jobs is not None:
            n = min(num_jobs, n)

        jobstr = '#\n#SBATCH --array=0-{}'.format(n)
        
        flagstr = flagstr + ' --job_id ${SLURM_ARRAY_TASK_ID}'
        output = ('#\n#SBATCH --output log/slurm-{jobname}-%A_%a.out'
                    .format(jobname=jobname))

    else:
        jobstr = ''
        output = ('#\n#SBATCH --output log/slurm-{jobname}-%A.out'
                    .format(jobname=jobname))

    with open('run-slurm.sh', 'w+') as f:
        f.write(SLURM_SCRIPT.format(
            jobname=jobname,
            jobs=jobstr,
            partition=partition,
            filepath=filepath.replace(os.sep, '/'),
            dependencies=depstr,
            flags=flagstr,
            output=output))


def run_slurm(
        filepath,
        jobname='slurm_job',
        partition='savio2',
        job_spec=None,
        num_jobs=None,
        dependencies=None,
        flags=None):

    _prep_slurm(filepath, jobname, partition, job_spec, num_jobs, dependencies, flags)

    job_command = ['sbatch', 'run-slurm.sh']

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


def slurm_runner(filepath, job_spec, run_job, onfinish=None, test_job=None, additional_metadata=None):

    @click.group()
    def slurm():
        if not os.path.isdir('log'):
            os.makedirs('log')

    @slurm.command()
    @click.option('--dependency', '-d', type=int, multiple=True)
    def prep(dependency=False):
        _prep_slurm(
            filepath=filepath,
            job_spec=job_spec,
            dependencies=('afterany', list(dependency)),
            flags=['do_job'])

    @slurm.command()
    @click.option('--num_jobs', '-n', type=int, required=False, default=None, help='Number of iterations to run')
    @click.option('--jobname', '-j', default='test', help='name of the job')
    @click.option('--partition', '-p', default='savio2', help='resource on which to run')
    @click.option('--dependency', '-d', type=int, multiple=True)
    def run(num_jobs=None, jobname='slurm_job', dependency=None, partition='savio2'):
        slurm_id = run_slurm(
            filepath=filepath,
            jobname=jobname,
            partition=partition,
            job_spec=job_spec,
            num_jobs=num_jobs,
            dependencies=('afterany', list(dependency)),
            flags=['do_job'])

        finish_id = run_slurm(
            filepath=filepath,
            jobname=jobname+'_finish',
            partition=partition,
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

        if onfinish:
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

    @slurm.command()
    @click.option('--num_jobs', '-n', type=int, required=False, default=None, help='Number of iterations to run')
    @click.option('--jobname', '-j', default='test', help='name of the job')
    @click.option('--partition', '-p', default='savio2', help='resource on which to run')
    @click.option('--dependency', '-d', type=int, multiple=True)
    def test(num_jobs=None, jobname='slurm_job', dependency=None, partition='savio2'):
        slurm_id = run_slurm(
            filepath=filepath,
            jobname=jobname,
            partition=partition,
            job_spec=job_spec,
            num_jobs=num_jobs,
            dependencies=('afterany', list(dependency)),
            flags=['do_test'])

        print('test job: {}'.format(slurm_id))

    @slurm.command()
    @click.option('--job_id', required=True, type=int)
    def do_test(job_id=None):

        job = get_job_by_index(job_spec, job_id)
        
        metadata = {}
        
        if additional_metadata is not None:
            metadata.update(
                {k: str(v) for k, v in additional_metadata.items()})

        metadata.update({k: str(v) for k, v in job.items()})

        test_job(metadata=metadata, **job)

    return slurm
