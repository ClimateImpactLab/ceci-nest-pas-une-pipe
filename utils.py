import re
import os
import time
import click
import pprint
import logging
import itertools
import functools
import subprocess

from _compat import exclusive_open

FORMAT = '%(asctime)-15s %(message)s'

logger = logging.getLogger('uploader')
logger.setLevel('DEBUG')

formatter = logging.Formatter(FORMAT)

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
# Wall clock limit:
#SBATCH --time=72:00:00
#
#SBATCH --requeue
{dependencies}
{output}
'''.strip()

SLURM_MULTI_SCRIPT = SLURM_SCRIPT + '''
#
#SBATCH --array=0-{maxnodes}

# set up directories
mkdir -p {logdir}
mkdir -p locks

## Run command

for i in {{1..{jobs_per_node}}}
do
    nohup python {filepath} do_job --job_name {jobname} \
--job_id {uniqueid} --num_jobs {numjobs} {flags} \
> {logdir}/nohup-{jobname}-{uniqueid}-${{SLURM_ARRAY_TASK_ID}}-$i.out &
done

python {filepath} wait --job_name {jobname} \
--job_id {uniqueid} --num_jobs {numjobs} {flags}
'''

SLURM_SINGLE_SCRIPT = SLURM_SCRIPT + '''

## Run command
python {filepath} {flags}
'''


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
        limit=None,
        uniqueid='"${SLURM_ARRAY_JOB_ID}"',
        jobs_per_node=24,
        maxnodes=100,
        dependencies=None,
        logdir='log',
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

        if limit is not None:
            n = min(limit, n)

        numjobs = n

        output = ('#\n#SBATCH --output {logdir}/slurm-{jobname}-%A_%a.out'
                    .format(jobname=jobname, logdir=logdir))

        template = SLURM_MULTI_SCRIPT

    else:
        numjobs = 1
        output = ('#\n#SBATCH --output {logdir}/slurm-{jobname}-%A.out'
                    .format(jobname=jobname, logdir=logdir))

        template = SLURM_SINGLE_SCRIPT

    with open('run-slurm.sh', 'w+') as f:
        f.write(template.format(
            jobname=jobname,
            partition=partition,
            numjobs=numjobs,
            jobs_per_node=jobs_per_node,
            maxnodes=maxnodes,
            uniqueid=uniqueid,
            filepath=filepath.replace(os.sep, '/'),
            dependencies=depstr,
            flags=flagstr,
            logdir=logdir,
            output=output))


def run_slurm(
        filepath,
        jobname='slurm_job',
        partition='savio2',
        job_spec=None,
        limit=None,
        uniqueid='"${SLURM_ARRAY_JOB_ID}"',
        jobs_per_node=24,
        maxnodes=100,
        dependencies=None,
        logdir='log',
        flags=None):

    _prep_slurm(
        filepath=filepath,
        jobname=jobname,
        partition=partition,
        job_spec=job_spec,
        limit=limit,
        uniqueid=uniqueid,
        jobs_per_node=jobs_per_node,
        maxnodes=maxnodes,
        dependencies=dependencies,
        logdir=logdir,
        flags=flags)

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


def slurm_runner(filepath, job_spec, run_job, onfinish=None):

    @slurm.command()
    @click.option(
        '--limit', '-l', type=int, required=False, default=None,
        help='Number of iterations to run')
    @click.option(
        '--jobs_per_node', '-n', type=int, required=False, default=24,
        help='Number of jobs to run per node')
    @click.option(
        '--maxnodes', '-x', type=int, required=False, default=100,
        help='Number of nodes to request for this job')
    @click.option(
        '--jobname', '-j', default='test', help='name of the job')
    @click.option(
        '--partition', '-p', default='savio2', help='resource on which to run')
    @click.option('--dependency', '-d', type=int, multiple=True)
    @click.option(
        '--logdir', '-L', defualt='log', help='Directory to write log files')
    @click.option(
        '--uniqueid', '-u', default='"${SLURM_ARRAY_JOB_ID}"',
        help='Unique job pool id')
    def prep(
            limit=None,
            jobs_per_node=24,
            jobname='slurm_job',
            dependency=None,
            partition='savio2',
            maxnodes=100,
            logdir='log',
            uniqueid='"${SLURM_ARRAY_JOB_ID}"'):

        _prep_slurm(
            filepath=filepath,
            jobname=jobname,
            partition=partition,
            job_spec=job_spec,
            jobs_per_node=jobs_per_node,
            maxnodes=maxnodes,
            limit=limit,
            uniqueid=uniqueid,
            logdir=logdir,
            dependencies=('afterany', list(dependency)))

    @slurm.command()
    @click.option('--limit', '-l', type=int, required=False, default=None, help='Number of iterations to run')
    @click.option('--jobs_per_node', '-n', type=int, required=False, default=24, help='Number of jobs to run per node')
    @click.option('--maxnodes', '-x', type=int, required=False, default=100, help='Number of nodes to request for this job')
    @click.option('--jobname', '-j', default='test', help='name of the job')
    @click.option('--partition', '-p', default='savio2', help='resource on which to run')
    @click.option('--logdir', '-L', defualt='log', help='Directory to write log files')
    @click.option('--dependency', '-d', type=int, multiple=True)
    @click.option('--logdir', '-L', defualt='log', help='Directory to write log files')
    @click.option('--uniqueid', '-u', default='"${SLURM_ARRAY_JOB_ID}"', help='Unique job pool id')
    def run(
            limit=None,
            jobs_per_node=24,
            jobname='slurm_job',
            dependency=None,
            partition='savio2',
            maxnodes=100,
            logdir='log',
            uniqueid='"${SLURM_ARRAY_JOB_ID}"'):

        slurm_id = run_slurm(
            filepath=filepath,
            jobname=jobname,
            partition=partition,
            job_spec=job_spec,
            jobs_per_node=jobs_per_node,
            maxnodes=maxnodes,
            limit=limit,
            uniqueid=uniqueid,
            logdir=logdir,
            dependencies=('afterany', list(dependency)))

        finish_id = run_slurm(
            filepath=filepath,
            jobname=jobname+'_finish',
            partition=partition,
            dependencies=('afterany', [slurm_id]),
            logdir=logdir,
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
    @click.option('--job_name', required=True)
    @click.option('--job_id', required=True)
    @click.option('--num_jobs', required=True, type=int)
    def do_job(job_name, job_id, num_jobs=None):

        if not os.path.isdir('locks'):
            os.makedirs('locks')

        for task_id in range(num_jobs):

            if os.path.exists('locks/{}-{}-{}.done'.format(job_name, job_id, task_id)):
                print('{} already done. skipping'.format(task_id))
                continue

            try:
                with exclusive_open(
                        'locks/{}-{}-{}.lck'.format(job_name, job_id, task_id)) as f:
                    pass

            except OSError:
                print('{} already in progress. skipping'.format(task_id))
                continue

            handler = logging.FileHandler(
                'log/run-{}-{}-{}.log'.format(job_name, job_id, task_id))
            handler.setFormatter(formatter)
            handler.setLevel(logging.DEBUG)

            logger.addHandler(handler)

            try:

                job = get_job_by_index(job_spec, task_id)

                metadata = {}
                metadata.update({k: str(v) for k, v in job.items()})

                logger.debug('Beginning job\nkwargs:\t{}'.format(
                    pprint.pformat(metadata, indent=2)))

                run_job(metadata=metadata, **job)

            except (KeyboardInterrupt, SystemExit):
                raise

            except Exception as e:
                logger.error(
                    'Error encountered in job {} {} {}'
                        .format(job_name, job_id, task_id),
                    exc_info=e)

            finally:
                if os.path.exists('locks/{}-{}-{}.lck'.format(job_name, job_id, task_id)):
                    os.remove('locks/{}-{}-{}.lck'.format(job_name, job_id, task_id))

                with open('locks/{}-{}-{}.done'.format(job_name, job_id, task_id), 'w+') as f:
                    pass

            logger.removeHandler(handler)


    @slurm.command()
    @click.option('--job_name', required=True)
    @click.option('--job_id', required=True)
    @click.option('--num_jobs', required=True, type=int)
    def wait(job_name, job_id, num_jobs=None):

        for task_id in range(num_jobs):
            while not os.path.exists(
                        'locks/{}-{}-{}.done'.format(job_name, job_id, task_id)):
                time.sleep(10)

    return slurm
