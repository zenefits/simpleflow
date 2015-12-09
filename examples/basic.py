import time

from simpleflow import (
    activity,
    Workflow,
    futures,
)


@activity.with_attributes(task_list='quickstart', version='example')
def increment(x):
    return x + 1


@activity.with_attributes(task_list='quickstart', version='example')
def double(x):
    return x * 2


# simpleflow activities can be classes ; in that case the class is instantiated
# with the params passed via submit, then the `execute()` method is called and
# the result is returned. This is what you will do if you plan to submit the
# work like this:
#
#   self.submit(Delay, foo, bar)
#
@activity.with_attributes(task_list='quickstart', version='example')
class Delay(object):
    def __init__(self, t, x):
        self.t = t
        self.x = x

    def execute(self):
        time.sleep(self.t)
        return self.x


# There's another option, if you want to submit object *instances*. In that
# case you will not use the `@activity.with_attributes()` decorator. You will
# have to subclass `InstanciatedActivity` instead. You'll also have to implement
# an `execute()` method like above. You can optionnally set the arguments you
# would have set in the "with_attributes()" decorator directly on the instance
# or on the class, as shown below.
#
# WARNING: if you use this form, simpleflow assumes you'll handle arguments
# resolution yourself ; so if you receive a future from an earlier "submit()",
# you *must* call ".result" on it yourself. Simpleflow won't resolve futures
# transparently if you use this form.
from simpleflow.activity import InstanciatedActivity
class Tetra(InstanciatedActivity):
    task_list = "quickstart"
    version = "example"

    def __init__(self, x):
        super(Tetra, self).__init__(self, x)
        self.x = x

    def execute(self):
        return self.x * 4


class BasicWorkflow(Workflow):
    name = 'basic'
    version = 'example'
    task_list = 'example'

    def run(self, x, t=30):
        y = self.submit(increment, x)
        yy = self.submit(Delay, t, y)
        z = self.submit(double, y)
        t = self.submit(Tetra(z.result))

        print '({x} + 1) * 2 * 4 = {result}'.format(
            x=x,
            result=t.result)
        futures.wait(yy, z)
        return z.result
