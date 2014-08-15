import os
import sys

lib_path = os.path.join(os.getcwd(), 'lib')
if lib_path not in sys.path:
    sys.path.insert(0, lib_path)


import logging
import uuid
import time

import webapp2
from webapp2_extras import jinja2

from google.appengine.ext import ndb
from google.appengine.api.taskqueue import UnknownQueueError

from furious import context
from furious.async import Async
from furious.handlers import webapp
furious_app = webapp.app


def jinja2_factory(app):
    j = jinja2.Jinja2(app)
    j.environment.globals.update({
        'uri_for': webapp2.uri_for
    })

    return j


class Job(ndb.Model):

    task_count = ndb.IntegerProperty(indexed=False)
    created = ndb.DateTimeProperty(auto_now_add=True)
    modified = ndb.DateTimeProperty(auto_now=True)
    finished_ids = ndb.IntegerProperty(repeated=True, indexed=False)
    error = ndb.TextProperty()
    queue = ndb.StringProperty(indexed=False)

    # TODO: Add stat rollups
    stats = ndb.JsonProperty()

    def to_dict(self):
        return {
            'task_count': self.task_count,
            'created': self.created,
            'modified': self.modified,
            'finished_ids': self.finished_ids,
            'finished': self.task_count == len(self.finished_ids),
            'stats': self.stats or {},
            'error': self.error,
            'queue': self.queue,
        }


class TaskInfo(ndb.Model):

    job_id = ndb.StringProperty(indexed=True)
    latency = ndb.FloatProperty(indexed=False)
    created = ndb.DateTimeProperty(auto_now_add=True)


class BaseHandler(webapp2.RequestHandler):

    def __init__(self, *args, **kwargs):
        super(BaseHandler, self).__init__(*args, **kwargs)

        #self._options = {}

    @webapp2.cached_property
    def jinja2(self):
        # Returns a Jinja2 renderer cached in the app registry.
        return jinja2.get_jinja2(factory=jinja2_factory)

    #@property
    #def options(self):
        #if not self._options:
            #self._options = self.app.config.get('havok.options', {})

        #return self._options

    def render_template(self, template, **context):
        rv = self.jinja2.render_template(template, **context)
        self.response.write(rv)


class HomeHandler(BaseHandler):

    def get(self):
        ctx = {"message": self.request.GET.get("message", "")}

        self.render_template('home.html', **ctx)


class TaskInsertHandler(BaseHandler):

    def post(self):
        task_count = self.request.POST.get("task_count")
        batch_size = int(self.request.POST.get("batch_size", 0) or 0)
        task_run_time = self.request.POST.get("task_run_time")
        use_batcher = self.request.POST.get("use_batcher")
        queue = self.request.POST.get("queue_name")

        if not task_count:
            return webapp2.redirect_to(
                'home', message="No tasks inserted.")

        task_count = int(task_count)

        job_id = uuid.uuid4().hex
        job = Job(id=job_id, task_count=task_count, queue=queue or "")
        job.put()

        Async(
            insert_tasks,
            args=(job_id, batch_size, task_count, task_run_time, use_batcher,
                  queue)
        ).start()

        return webapp2.redirect_to('job_detail', job_id=job_id)


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in xrange(0, len(l), n):
        yield l[i:i + n]


def insert_tasks(job_id, batch_size, task_count, task_run_time, use_batcher,
                 queue):

    kwargs = {}

    if queue:
        kwargs["queue"] = queue

    try:
        _insert_tasks(job_id, batch_size, task_count, task_run_time,
                      use_batcher, kwargs)
    except UnknownQueueError, e:
        _set_error_on_job(
            job_id, "Could find queue '%s', Message: %s" % (queue, e.message))
        logging.exception(e)
        return
    except Exception, e:
        _set_error_on_job(job_id, e.message)
        logging.exception(e)
        return

    Async(collect_job_data, args=(job_id,)).start()


def _set_error_on_job(job_id, error):
    job = Job.get_by_id(job_id)

    if not job:
        return

    job.error = error
    job.put()


def _insert_tasks(job_id, batch_size, task_count, task_run_time, use_batcher,
                  kwargs):
    if use_batcher:
        with context.new(batch_size=batch_size) as ctx:
            for count in xrange(task_count):
                ctx.add(run_task, args=(job_id, count, task_run_time),
                        **kwargs)
    elif batch_size > 1:
        for batch in chunks(range(task_count), batch_size):
            with context.new() as ctx:
                for count in batch:
                    ctx.add(run_task, args=(job_id, count, task_run_time),
                            **kwargs)
    else:
        with context.new() as ctx:
            for count in xrange(task_count):
                ctx.add(run_task, args=(job_id, count, task_run_time),
                        **kwargs)


class JobDetailHandler(BaseHandler):

    def get(self):
        job_id = self.request.GET.get("job_id")

        if not job_id:
            return

        ctx = {
            "job_id": job_id
        }

        job = Job.get_by_id(job_id)

        if job:
            ctx.update(job.to_dict())

        self.render_template('job_detail.html', **ctx)


def collect_job_data(job_id):

    job = Job.get_by_id(job_id)

    if not job:
        # Not good.
        return

    if job.finished_ids and len(job.finished_ids) == job.task_count:
        # No more work to do.
        return

    run_time = job.modified - job.created

    if run_time.days >= 0 and run_time.seconds > 600:
        logging.error("Took too long, bailing. %s", run_time.seconds)
        return

    if not job.stats:

        job.stats = {
            'two_minutes': [],
            'one_minute': [],
            'thirty_seconds': [],
            'ten_seconds': [],
            'five_seconds': [],
            'two_seconds': [],
        }

    # Get the task keys
    keys = []
    task_futures = []
    i = 0

    for count in xrange(job.task_count):

        if count not in job.finished_ids:
            # add
            keys.append(ndb.Key(TaskInfo, "%s:%s" % (job_id, count)))
            i += 1

            if i == 100:
                task_futures.extend(ndb.get_multi_async(keys))
                i = 0
                keys = []

    task_futures.extend(ndb.get_multi_async(keys))

    for task_future in task_futures:
        task = task_future.get_result()

        if not task:
            continue

        job_count = int(task.key.id().split(':')[1])
        job.finished_ids.append(job_count)

        key = None
        if task.latency > 120:
            key = 'two_minutes'
        elif task.latency > 60:
            key = 'one_minute'
        elif task.latency > 30:
            key = 'thirty_seconds'
        elif task.latency > 10:
            key = 'ten_seconds'
        elif task.latency > 5:
            key = 'five_seconds'
        elif task.latency > 2:
            key = 'two_seconds'

        if key:
            job.stats[key].append((job_count, task.latency))

    job.finished_ids = list(set(job.finished_ids))
    job.put()

    Async(collect_job_data, args=(job_id,), task_args={'countdown': 1}).start()


def run_task(job_id, count, task_run_time):
    try:
        latency = _get_latency()
    except Exception, e:
        logging.exception(e)
        latency = -1

    if task_run_time:
        try:
            time.sleep(int(task_run_time))
        except Exception, e:
            logging.exception(e)

    task_info = TaskInfo(id="%s:%s" % (job_id, count), job_id=job_id,
                         latency=latency)
    task_info.put()


def _get_latency():
    start_timestamp = time.time()
    task_eta = float(os.environ.get('HTTP_X_APPENGINE_TASKETA', 0.0))
    return start_timestamp - task_eta


config = {
    'webapp2_extras.jinja2': {
        'template_path': os.path.join(os.path.dirname(__file__), 'templates')
    }
}


app = webapp2.WSGIApplication([
    webapp2.Route("/", handler=HomeHandler, name="home"),
    webapp2.Route("/insert", handler=TaskInsertHandler, name="trigger_tasks"),
    webapp2.Route("/detail", handler=JobDetailHandler, name="job_detail"),
])

