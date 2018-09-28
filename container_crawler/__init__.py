import eventlet
import eventlet.pools
eventlet.patcher.monkey_patch(all=True)

import os.path
import time
import traceback

from swift.common.db import DatabaseConnectionError
from swift.common.ring import Ring
from swift.common.ring.utils import is_local_device
from swift.common.utils import whataremyips, hash_path, storage_directory, \
    config_true_value

from swift.container.backend import DATADIR, ContainerBroker

from .utils import create_internal_client


class RetryError(Exception):
    pass


class SkipContainer(Exception):
    '''
    Raised during initialization when the provided container (for
    whatever reason) *should not* be crawled.
    '''
    pass


class ContainerJob(object):
    PASS_SUCCEEDED = 1
    PASS_FAILED = 2

    def __init__(self):
        # The Queue is only used to mimic a condition variable, so that we can
        # block on tasks being completed.
        self._done = eventlet.queue.Queue()
        self._lock = eventlet.semaphore.Semaphore(1)
        self._reset()

    def _reset(self):
        self._outstanding = 0
        self._retry = False
        self._error = False

    def wait_all(self):
        '''Waits until all of the rows for this iteration of the container have
           been completed.
           NOTE: this only works for a single producer thread, since we are not
           holding the lock here, we are not guarding against another thread
           submitting tasks!
        '''
        self._done.get()
        # At some point we may want to differentiate RetryError vs all other
        # errors.
        ret = not self._retry and not self._error
        self._reset()
        return self.PASS_SUCCEEDED if ret else self.PASS_FAILED

    def submit_tasks(self, tasks, work_queue):
        with self._lock:
            self._outstanding += len(tasks)
        for task in tasks:
            work_queue.put((task, self))

    def complete_task(self, error=False, retry=False):
        with self._lock:
            self._outstanding -= 1
            if error and not self._error:
                self._error = error
            if retry and not self._retry:
                self._retry = retry
            if self._outstanding == 0:
                self._done.put(None)


class ContainerCrawler(object):
    def __init__(self, conf, handler_class, logger=None):
        if not handler_class:
            raise RuntimeError('Handler class must be defined')

        self.logger = logger
        self.conf = conf
        self.root = conf['devices']
        self.bulk = config_true_value(conf.get('bulk_process', False))
        self.interval = 10
        self.swift_dir = '/etc/swift'
        self.container_ring = Ring(self.swift_dir, ring_name='container')

        self.status_dir = conf['status_dir']
        self.myips = whataremyips('0.0.0.0')
        self.items_chunk = conf['items_chunk']
        self.poll_interval = conf.get('poll_interval', 5)
        self.handler_class = handler_class
        self._in_progress_containers = set()

        if self.bulk:
            self.workers = 1

        self._init_workers(conf)
        self._init_ic_pool(conf)

        self.log('debug', 'Created the Container Crawler instance')

    def _init_workers(self, conf):
        if not self.bulk:
            self.workers = conf.get('workers', 10)
            self.worker_pool = eventlet.GreenPool(self.workers)
            self.work_queue = eventlet.queue.Queue(self.workers * 2)

            for _ in xrange(self.workers):
                self.worker_pool.spawn_n(self._worker)

        self.enumerator_workers = conf.get('enumerator_workers', 10)
        self.enumerator_pool = eventlet.GreenPool(self.enumerator_workers)
        self.enumerator_queue = eventlet.queue.Queue(self.enumerator_workers)

        for _ in xrange(self.enumerator_workers):
            self.enumerator_pool.spawn_n(self._enumerator)

    def _init_ic_pool(self, conf):
        pool_size = self.workers
        self._swift_pool = eventlet.pools.Pool(
            create=lambda: create_internal_client(conf, self.swift_dir),
            min_size=pool_size,
            max_size=pool_size)

    def _worker(self):
        while 1:
            try:
                work = self.work_queue.get()
            except:
                self.log(
                    'error', 'Failed to fetch items from the queue: %s' %
                    traceback.format_exc())
                eventlet.sleep(100)
                continue

            try:
                if not work:
                    break

                (row, handler), container_job = work
                with self._swift_pool.item() as swift_client:
                    handler.handle(row, swift_client)
                container_job.complete_task()
            except RetryError:
                container_job.complete_task(retry=True)
            except:
                container_job.complete_task(error=True)
                self.log('error', u'Failed to handle row %s (%s): %r' % (
                    row['ROWID'], row['name'].decode('utf-8'),
                    traceback.format_exc()))
            finally:
                self.work_queue.task_done()

    def _enumerator(self):
        job = ContainerJob()
        while 1:
            try:
                work = self.enumerator_queue.get()
            except:
                self.log(
                    'error', 'Failed to fetch containers to enumerate %s' %
                    traceback.format_exc())
                eventlet.sleep(100)
                continue

            try:
                if not work:
                    break

                settings, per_account = work
                handler = self.handler_class(self.status_dir, settings,
                                             per_account=per_account)
                owned, verified, last_row, db_id = self.handle_container(
                    handler, job)
                if not owned and not verified:
                    continue

                if self.bulk or job.wait_all() == ContainerJob.PASS_SUCCEEDED:
                    handler.save_last_row(last_row, db_id)
                    self.log('info',
                             'Processed %d rows; verified %d rows; '
                             'last row: %d' % (owned, verified, last_row))
            except SkipContainer:
                self.log(
                    'info', "Skipping %(account)s/%(container)s" % settings)
            except RetryError:
                # Can appear from the bulk handling code.
                # TODO: we should do a better tying the bulk handling code into
                # this model.
                pass
            except:
                account = settings['account']
                container = settings['container']
                self.log('error', "Failed to process %s/%s with %s" % (
                    account, container,
                    self.handler_class.__name__))
                self.log('error', traceback.format_exc())
            finally:
                if work:
                    self._in_progress_containers.remove(
                        (work[0]['account'], work[0]['container']))
                self.enumerator_queue.task_done()

    def log(self, level, message):
        if not self.logger:
            return
        getattr(self.logger, level)(message)

    def get_broker(self, account, container, part, node):
        db_hash = hash_path(account.encode('utf-8'), container.encode('utf-8'))
        db_dir = storage_directory(DATADIR, part, db_hash)
        db_path = os.path.join(self.root, node['device'], db_dir,
                               db_hash + '.db')
        return ContainerBroker(db_path, account=account, container=container)

    def submit_items(self, handler, rows, job):
        if not rows:
            return

        if self.bulk:
            with self._swift_pool.item() as swift_client:
                handler.handle(rows, swift_client)
            return

        job.submit_tasks(map(lambda row: (row, handler), rows),
                         self.work_queue)

    def process_items(self, handler, rows, nodes_count, node_id, job):
        owned_rows = filter(
            lambda row: row['ROWID'] % nodes_count == node_id, rows)
        verified_rows = filter(
            lambda row: row['ROWID'] % nodes_count != node_id, rows)

        self.submit_items(handler, owned_rows, job)
        self.submit_items(handler, verified_rows, job)

        return len(owned_rows), len(verified_rows)

    def handle_container(self, handler, job):
        part, container_nodes = self.container_ring.get_nodes(
            handler._account.encode('utf-8'),
            handler._container.encode('utf-8'))
        nodes_count = len(container_nodes)

        for index, node in enumerate(container_nodes):
            if not is_local_device(self.myips, None, node['ip'],
                                   node['port']):
                continue
            broker = self.get_broker(handler._account,
                                     handler._container,
                                     part, node)
            if broker.is_deleted():
                continue
            broker_info = broker.get_info()
            last_row = handler.get_last_row(broker_info['id'])
            if not last_row:
                last_row = 0
            try:
                items = broker.get_items_since(last_row, self.items_chunk)
            except DatabaseConnectionError:
                continue

            if not items:
                return (0, 0, None, broker_info['id'])

            self.log('info',
                     'Processing %d rows since row %d for %s/%s' % (
                         len(items), last_row, handler._account,
                         handler._container))
            owned_count, verified_count = self.process_items(
                handler, items, nodes_count, index, job)

            return (owned_count,
                    verified_count,
                    items[-1]['ROWID'],
                    broker_info['id'])
        return (0, 0, None, None)

    def list_containers(self, account):
        # TODO: we should not have to retrieve all of the containers at once,
        # but it will require allocating a swift_client for this purpose from
        # the pool -- consider doing that at some point. However, as long as
        # there are fewer than a few million containers, getting all of them at
        # once should be cheap, paginating 10000 at a time.
        with self._swift_pool.item() as swift_client:
            return [c['name'] for c in swift_client.iter_containers(account)]

    def _is_processing(self, settings):
        # NOTE: if we allow more than one destination for (account, container),
        # we have to change the contents of this set
        key = (settings['account'], settings['container'])
        return key in self._in_progress_containers

    def _enqueue_container(self, settings, per_account=False):
        key = (settings['account'], settings['container'])
        self._in_progress_containers.add(key)
        self.enumerator_queue.put((settings, per_account))

    def _submit_containers(self):
        for container_settings in self.conf['containers']:
            # TODO: perform validation of the settings on startup
            if 'container' not in container_settings:
                self.log(
                    'error',
                    'Container name not specified in settings -- continue')
                continue
            if 'account' not in container_settings:
                self.log(
                    'error',
                    'Account not specified in settings -- continue')
                continue

            if container_settings['container'] == '/*':
                all_containers = self.list_containers(
                    container_settings['account'])
                for container in all_containers:
                    settings_copy = container_settings.copy()
                    settings_copy['container'] = container.decode('utf-8')
                    if not self._is_processing(settings_copy):
                        self._enqueue_container(
                            settings_copy, per_account=True)
                # After iterating over all of the containers, we prune any
                # entries from containers that may have been deleted (so as to
                # avoid missing data). There is still a chance where a
                # container is removed and created between the calls to
                # CloudSync, however there is nothing we can do about that.
                # TODO: keep track of container creation date to detect when
                # they are removed and then added.
                if not os.path.exists(os.path.join(
                        self.status_dir, container_settings['account'])):
                    continue
                tracked_containers = os.listdir(os.path.join(
                    self.status_dir, container_settings['account']))
                disappeared = set(tracked_containers) - set(all_containers)
                for container in disappeared:
                    try:
                        os.unlink(os.path.join(self.status_dir,
                                               container_settings['account'],
                                               container))
                    except Exception as e:
                        self.log(
                            'warning',
                            'Failed to remove the status file for %s: %s' % (
                                os.path.join(container_settings['account'],
                                             container), repr(e)))
            else:
                if not self._is_processing(container_settings):
                    self._enqueue_container(container_settings,
                                            per_account=False)

    def run_always(self):
        # Since we don't support reloading, the daemon should quit if there are
        # no containers configured
        if 'containers' not in self.conf or not self.conf['containers']:
            return
        self.log('debug', 'Entering the poll loop')
        while True:
            start = time.time()
            self._submit_containers()
            elapsed = time.time() - start
            if elapsed < self.poll_interval:
                eventlet.sleep(self.poll_interval - elapsed)

    def run_once(self):
        self._submit_containers()
        self.enumerator_queue.join()
