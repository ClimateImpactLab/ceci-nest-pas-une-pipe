import re
import os
import time
import math
import click
import pprint
import logging
import itertools
import functools
import subprocess

from _compat import exclusive_open

from locking import JobLocker, JobSkipped

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

for i in {{1..{tasks_per_node}}}
do
    nohup python {filepath} do_job --jobname {jobname} \
--uniqueid {uniqueid} --num_tasks {num_tasks} --logdir "{logdir}" {flags} \
> {logdir}/nohup-{jobname}-{uniqueid}-${{SLURM_ARRAY_TASK_ID}}-$i.out &
done

python {filepath} wait --jobname {jobname} \
--uniqueid {uniqueid} --num_tasks {num_tasks} {flags}
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


def count_jobs(job_spec):
    return _product(map(len, job_spec))


def _prep_slurm(
        filepath,
        jobname='slurm_job',
        partition='savio2',
        job_spec=None,
        limit=None,
        uniqueid='"${SLURM_ARRAY_JOB_ID}"',
        tasks_per_node=24,
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
        n = count_jobs(job_spec)

        if limit is not None:
            n = min(limit, n)

        num_tasks = n

        output = (
                '#\n#SBATCH --output {logdir}/slurm-{jobname}-%A_%a.out'
                .format(jobname=jobname, logdir=logdir))

        template = SLURM_MULTI_SCRIPT

    else:
        num_tasks = 1
        output = (
                '#\n#SBATCH --output {logdir}/slurm-{jobname}-%A.out'
                .format(jobname=jobname, logdir=logdir))

        template = SLURM_SINGLE_SCRIPT

    with open('run-slurm.sh', 'w+') as f:
        f.write(template.format(
            jobname=jobname,
            partition=partition,
            num_tasks=num_tasks,
            tasks_per_node=tasks_per_node,
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
        tasks_per_node=24,
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
        tasks_per_node=tasks_per_node,
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
            (index//(_product(map(len, job_spec[i+1:]))) % len(job_spec[i]))]
        for i in range(len(job_spec))])


def slurm_runner(filepath, job_spec, run_job, onfinish=None):

    @click.group()
    def slurm():
        pass

    @slurm.command()
    @click.option(
        '--limit', '-l', type=int, required=False, default=None,
        help='Number of iterations to run')
    @click.option(
        '--tasks_per_node', '-n', type=int, required=False, default=24,
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
        '--logdir', '-L', default='log', help='Directory to write log files')
    @click.option(
        '--uniqueid', '-u', default='"${SLURM_ARRAY_JOB_ID}"',
        help='Unique job pool id')
    def prep(
            limit=None,
            tasks_per_node=24,
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
            tasks_per_node=tasks_per_node,
            maxnodes=maxnodes,
            limit=limit,
            uniqueid=uniqueid,
            logdir=logdir,
            dependencies=('afterany', list(dependency)))

    @slurm.command()
    @click.option(
        '--limit', '-l', type=int, required=False, default=None,
        help='Number of iterations to run')
    @click.option(
        '--tasks_per_node', '-n', type=int, required=False, default=24,
        help='Number of jobs to run per node')
    @click.option(
        '--maxnodes', '-x', type=int, required=False, default=100,
        help='Number of nodes to request for this job')
    @click.option(
        '--jobname', '-j', default='slurm_job', help='name of the job')
    @click.option(
        '--partition', '-p', default='savio2', help='resource on which to run')
    @click.option(
        '--dependency', '-d', type=int, multiple=True)
    @click.option(
        '--logdir', '-L', default='log', help='Directory to write log files')
    @click.option(
        '--uniqueid', '-u', default='"${SLURM_ARRAY_JOB_ID}"',
        help='Unique job pool id')
    def run(
            limit=None,
            tasks_per_node=24,
            jobname='slurm_job',
            dependency=None,
            partition='savio2',
            maxnodes=100,
            logdir='log',
            uniqueid='"${SLURM_ARRAY_JOB_ID}"'):

        '''
        Dispatch a multi-node x multi-core run with SLURM

        This function requests nodes from SLURM. Each node will provision
        multiple workers which will be charged with completing individual
        tasks.

        To add multiple partition types to the same task pool, specify
        identical ``jobname`` and ``uniqueid`` parameters.

        .. warning::

            Each worker reads the calling file at runtime. Changes to this file
            will therefore be reflected in subsequent runs without warning.

        Parameters
        ----------
        limit : int, optional
            Maximum number of tasks to run. Useful for debugging - should not
            be used if all tasks are desired. Default behavior is to run all
            tasks.

        tasks_per_node : int, optional
            Number of worker threads to create on each node. This value should
            be less than or equal to the number of cores on each machine
            divided by the number of cores used by each task. Default 24.

        jobname : str, optional
            Name of the job. This value is used for log file naming and for
            task completion tracking. Specify the same name and ``uniqueid``
            across ``run`` calls to provision multiple worker pools with the
            same set of tasks (e.g. to utilize multiple partition types for the
            same job). Default ``slurm_job``.

        dependency : int, optional
            ``SLURM_JOB_ID`` job. Dependency is interpreted as an ``afterany``
            dependency, so inputs/job completion should be checked before
            continuing an operation. (default None)

        partition : str, optional
            Node partition to request from SLURM (default ``savio2``)

        maxnodes : int, optional
            Maximum number of nodes to request (default 100)

        logdir : str, optional
            Logging directory. Default is to create a directory ``log`` at the
            current location. Note that if the ``logdir`` is in the home
            directory, this could create a storage allowance problem with
            significant logging or a large number of tasks.

        uniqueid : str, optional
            Unique job ID to use (alongside the ``jobname``) to create a worker
            pool. If creating a pool of workers with multiple SLURM tasks for
            any reason (e.g. using multiple partition types) specify the same
            ``jobname`` and ``uniqueid`` to instruct worker processes to share
            tasks.

        '''

        if not os.path.isdir(logdir):
            os.makedirs(logdir)

        slurm_id = run_slurm(
            filepath=filepath,
            jobname=jobname,
            partition=partition,
            job_spec=job_spec,
            tasks_per_node=tasks_per_node,
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
        '''
        On-finish "worker" process
        '''

        proc = subprocess.Popen(
            [
                'sacct', '-j', slurm_id,
                '--format=JobID,JobName,MaxRSS,Elapsed,State'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)

        out, err = proc.communicate()

        print(out)

        if onfinish:
            onfinish()

    @slurm.command()
    @click.option('--jobname', required=True)
    @click.option('--uniqueid', required=True)
    @click.option('--num_tasks', required=True, type=int)
    @click.option(
        '--logdir', '-L', default='log', help='Directory to write log files')
    def do_job(jobname, uniqueid, num_tasks=None, logdir='log'):
        '''
        Worker process - do not call directly

        This option should be called by SLURM nodes as part of a ``run`` job.

        Iterates through tasks, finding tasks in need of completion. For each
        job, creates a logging file handler, prepares metadata, and calls
        ``run_job(logger, metadata, **job)``.

        Job dispatch is controlled by :py:class:`locking.JobLocker` objects.
        '''

        locker = JobLocker(jobname, uniqueid)

        if not os.path.isdir(logdir):
            os.makedirs(logdir)

        for task_id in range(num_tasks):

            try:
                locker.acquire(task_id)

            except JobSkipped as e:
                print(e)
                continue

            else:

                handler = logging.FileHandler(os.path.join(
                    logdir,
                    'run-{}-{}-{}.log'.format(jobname, uniqueid, task_id)))
                handler.setFormatter(formatter)
                handler.setLevel(logging.DEBUG)

                logger.addHandler(handler)

                try:

                    job = get_job_by_index(job_spec, task_id)

                    metadata = {}
                    metadata.update({k: str(v) for k, v in job.items()})

                    logger.debug('Beginning job\nkwargs:\t{}'.format(
                        pprint.pformat(metadata, indent=2)))

                    run_job(logger=logger, metadata=metadata, **job)

                except (KeyboardInterrupt, SystemExit):
                    raise

                except Exception as e:

                    # report error
                    logger.error(
                        'Error encountered in job {} {} {}'
                        .format(jobname, uniqueid, task_id),
                        exc_info=e)

                    locker.error(task_id)

                else:

                    # report successful job completion
                    locker.done(task_id)

            finally:

                logger.removeHandler(handler)
                locker.release(task_id)

    @slurm.command()
    @click.option('--jobname', '-j', required=True)
    @click.option('--uniqueid', '-u', required=True)
    def status(jobname, uniqueid):
        '''
        Get the status of a running job

        Parameters
        ----------

        jobname : str
            Name of the job to check status. If no jobname was provided at run
            creation, this is ``slurm_job``

        uniqueid : str
            Unique ID of the job to check status. If no uniqueid was provided
            at run creation, this is the ``SLURM_JOB_ID``.

        '''

        n = count_jobs(job_spec)
        locks = os.listdir('locks')

        count = int(math.log10(n)//1 + 1)

        locked = len([
            i for i in range(n)
            if '{}-{}-{}.lck'.format(jobname, uniqueid, i) in locks])

        done = len([
            i for i in range(n)
            if '{}-{}-{}.done'.format(jobname, uniqueid, i) in locks])

        print(
            ("\n".join(["{{:<15}}{{:{}d}}".format(count) for _ in range(3)]))
            .format('jobs:', n, 'done:', done, 'in progress:', locked))

    @slurm.command()
    @click.option('--jobname', required=True)
    @click.option('--uniqueid', required=True)
    @click.option('--num_tasks', required=True, type=int)
    def wait(jobname, uniqueid, num_tasks=None):
        '''
        Worker process - do not call directly

        This option should be called by SLURM nodes as part of a ``run`` job.

        Waits for all tasks to complete. This process blocks a node from
        exiting until all tasks have either errored or completed.

        '''

        locker = JobLocker(jobname, uniqueid)

        once_more = True

        while True:

            tasks_in_progress = False
            for task_id in range(num_tasks):
                status = locker.get_status(task_id)
                if (status == 'locked') or (status is None):
                    time.sleep(10)
                    tasks_in_progress = True
                    once_more = True
                    break

            if not tasks_in_progress:

                if once_more:
                    time.sleep(10)
                    once_more = False
                    continue

                return

    return slurm
