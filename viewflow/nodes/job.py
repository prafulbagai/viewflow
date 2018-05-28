
from .. import Task, mixins
from ..fields import get_task_ref
from ..activation import AbstractJobActivation


class AbstractJob(mixins.TaskDescriptionMixin,
                  mixins.NextNodeMixin,
                  mixins.UndoViewMixin,
                  mixins.CancelViewMixin,
                  mixins.DetailViewMixin,
                  Task):
    """
    Base class for task that runs in background.

    Example::

        job = (
            flow.Job(task.job)
            .Next(this.end)
        )
    """

    task_type = 'JOB'

    def __init__(self, job, **kwargs):  # noqa D102
        super(AbstractJob, self).__init__(**kwargs)
        self._job = job

    @property
    def job(self):
        """Callable that should start the job in background."""
        return self._job


class CeleryJob(AbstractJob):
    """CeleryJob Node."""

    def __init__(self, *args, **kwargs):
        kwargs.update(activation_class=JobActivation)
        return super(CeleryJob, self).__init__(*args, **kwargs)


class JobActivation(AbstractJobActivation):
    def run_async(self):
        job = self.flow_task.job
        flow_task_strref = get_task_ref(self.flow_task)
        job.delay(flow_task_strref, self.process.pk, self.task.pk)
