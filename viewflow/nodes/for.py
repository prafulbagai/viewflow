
from django.utils.timezone import now

from .. import mixins, Event, signals
from ..activation import Activation, STATUS, all_leading_canceled


class ForActivation(Activation):

    def execute(self):
        iterator = self.flow_task.iterator
        sequence = self.flow_task.sequence
        for_execution = list(map(iterator, sequence))
        print(for_execution)

    @Activation.status.transition(source=STATUS.NEW)
    def perform(self):
        with self.exception_guard():
            self.task.started = now()

            signals.task_started.send(sender=self.flow_class,
                                      process=self.process, task=self.task)

            self.execute()

            self.task.finished = now()
            self.set_status(STATUS.DONE)
            self.task.save()

            signals.task_finished.send(sender=self.flow_class,
                                       process=self.process, task=self.task)

            self.activate_next()

    @Activation.status.transition(source=STATUS.DONE,
                                  conditions=[all_leading_canceled])
    def activate_next(self):
        """Activate all outgoing edges."""
        if self.flow_task._next:
            self.flow_task._next.activate(prev_activation=self,
                                          token=self.task.token)

    @classmethod
    def activate(cls, flow_task, prev_activation, token):
        """Instantiate new task."""
        task = flow_task.flow_class.task_class(
            process=prev_activation.process,
            flow_task=flow_task,
            token=token)

        task.save()
        task.previous.add(prev_activation.task)

        activation = cls()
        activation.initialize(flow_task, task)
        activation.perform()

        return activation


class For(mixins.TaskDescriptionMixin, mixins.NextNodeMixin, Event):
    """For Node."""

    task_type = 'FOR'
    activation_class = ForActivation

    def __init__(self, iterator, sequence, **kwargs):
        self.iterator = iterator
        self.sequence = sequence
        super(For, self).__init__(**kwargs)
