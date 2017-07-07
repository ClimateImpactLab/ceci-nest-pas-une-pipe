
import os
from _compat import exclusive_open, exclusive_read

class JobSkipped(IOError):
    MSG = 'Job {} skipped'

    def __init__(self, job_id, *args, **kwargs):
        super(JobSkipped, self).__init__(
            self.MSG.format(job_id), *args, **kwargs)


class JobInProgress(JobSkipped):
    MSG = 'Job {} skipped - already in progress'


class JobAlreadyDone(JobSkipped):
    MSG = 'Job {} skipped - already done'


class JobPreviouslyErrored(JobSkipped):
    MSG = 'Job {} skipped - previously errored'


class JobLocker(object):
    def __init__(self, job_name, job_id, lock_dir='locks'):
        self.job_name = job_name
        self.job_id = job_id
        self.lock_dir = lock_dir

        self.lock_file = (os.path.join(
            self.lock_dir,
            '{}-{}-{{}}.{{}}'.format(self.job_name, self.job_id)))

    def initialize(self):

        if not os.path.isdir(self.lock_dir):
            os.makedirs(self.lock_dir)

    def acquire(self, task_id):

        if os.path.exists(self.lock_file.format(task_id, 'done')):
            raise JobAlreadyDone(task_id)
            
        elif os.path.exists(self.lock_file.format(task_id, 'err')):
            raise JobPreviouslyErrored(task_id)

        try:
            with exclusive_open(self.lock_file.format(task_id, 'lck')):
                pass

            # Check for race conditions
            if os.path.exists(self.lock_file.format(task_id, 'done')):
                if os.path.exists(self.lock_file.format(task_id, 'lck')):
                    os.remove(self.lock_file.format(task_id, 'lck'))
                raise JobAlreadyDone(task_id)
            
            elif os.path.exists(self.lock_file.format(task_id, 'err')):
                if os.path.exists(self.lock_file.format(task_id, 'lck')):
                    os.remove(self.lock_file.format(task_id, 'lck'))
                raise JobPreviouslyErrored(task_id)
        
        except IOError:
            raise JobInProgress(task_id)

    def error(self, task_id):

        with open(self.lock_file.format(task_id, 'err'), 'w+'):
            pass

    def done(self, task_id):

        with open(self.lock_file.format(task_id, 'done'), 'w+'):
            pass

    def release(self, task_id):
        if os.path.exists(self.lock_file.format(task_id, 'lck')):
            os.remove(self.lock_file.format(task_id, 'lck'))

    def is_locked(self, task_id):

        lock_file = (os.path.join(
            self.lock_dir,
            '{}-{}-{}.{{}}'.format(self.job_name, self.job_id, task_id)))

        try:
            with exclusive_read(self.lock_file.format(task_id, 'lck')):
                pass

            # We only get here if the run has been terminated abruptly
            # and has left its lock object "unlocked"
            try:
                os.remove(self.lock_file.format(task_id, 'lck'))
            except OSError:
                pass
                
            return False

        except IOError:
            # We get here if a run is in progress
            return True

        except OSError:
            # We get here if a run has not started or is complete
            return False

    def get_status(self, task_id):
        lock_file = (os.path.join(
            self.lock_dir,
            '{}-{}-{}.{{}}'.format(self.job_name, self.job_id, task_id)))

        if self.is_locked(task_id):
            return 'locked'

        if os.path.isfile(self.lock_file.format(task_id, 'done')):
            return 'done'

        if os.path.isfile(self.lock_file.format(task_id, 'err')):
            return 'err'

        else:
            return None
