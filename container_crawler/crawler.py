import eventlet
import eventlet.pools
eventlet.patcher.monkey_patch(all=True)

import glob
import hashlib
import json
import os.path
import shutil
import time
import threading

from swift.common.ring import Ring
from swift.common.ring.utils import is_local_device
from swift.common.internal_client import UnexpectedResponse
from swift.common.http import HTTP_NOT_FOUND
from swift.common.utils import config_true_value, decode_timestamps,\
    hash_path, storage_directory, whataremyips

from swift.container.backend import DATADIR, ContainerBroker

from .exceptions import RetryError, SkipContainer
from .utils import create_internal_client


HEXDIGITS = 6  # Digits to use from hash of name for sharding


class ContainerJob(object):
    PASS_SUCCEEDED = 1
    PASS_FAILED = 2

    def __init__(self):
        # The Queue is only used to mimic a condition variable, so that we can
        # block on tasks being completed.
        self._lock = threading.Lock()
        self._done = threading.Condition(self._lock)
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
        with self._lock:
            while self._outstanding != 0:
                self._done.wait()
            # At some point we may want to differentiate RetryError vs all
            # other errors.
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
                self._done.notify()


def num_from_row(row):
    return int(hashlib.sha1(row['name']).hexdigest()[-HEXDIGITS:], 16)


def mapping_signature(mapping):
    return hashlib.md5(json.dumps(mapping, sort_keys=True)).hexdigest()


class Crawler(object):
    def __init__(self, conf, handler_factory, logger=None):
        if not handler_factory:
            raise RuntimeError('Handler class must be defined')

        self.logger = logger
        self.conf = conf
        self.root = conf['devices']
        self.bulk = config_true_value(conf.get('bulk_process', False))
        self.interval = 10
        self.swift_dir = '/etc/swift'
        self.container_ring = Ring(self.swift_dir, ring_name='container')

        self.status_dir = conf['status_dir']
        self.myips = whataremyips(conf.get('swift_bind_ip', '0.0.0.0'))
        self.items_chunk = conf['items_chunk']
        # Verification slack is specified in minutes.
        self._verification_slack = conf.get('verification_slack', 0) * 60
        self.poll_interval = conf.get('poll_interval', 5)
        self.handler_factory = handler_factory
        # NOTE: this structure is not protected. Since we use green threads, we
        # expect a context switch to only occur on blocking calls, so the set
        # operations should be safe in this context. This can lead to skipping
        # container cycles unnecessarily if the threading model changes.
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
            except Exception as e:
                self.log(
                    'error', 'Failed to fetch items from the queue: %r' % e)
                self.log('debug', 'Failed to fetch items from the queue: %s',
                         exc_info=True)
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
            except Exception as e:
                container_job.complete_task(error=True)
                self.log('error', u'Failed to handle row %s (%s): %r' % (
                    row['ROWID'], row['name'].decode('utf-8'), e))
                self.log('debug', u'Failed to handle row %s (%s)' % (
                    row['ROWID'], row['name'].decode('utf-8')), exc_info=True)
            finally:
                self.work_queue.task_done()

    def _get_new_rows(self, broker, start_row, nodes, node_id, verifying):
        rows = []
        if verifying:
            cutoff = time.time() - self._verification_slack
        for row in broker.get_items_since(start_row, self.items_chunk):
            hnum = num_from_row(row)
            if not verifying and hnum % nodes != node_id:
                continue
            ts = decode_timestamps(row['created_at'])[2].timestamp
            if verifying and ts > cutoff:
                break
            rows.append(row)
        return rows

    def _enumerator(self):
        job = ContainerJob()
        while 1:
            try:
                work = self.enumerator_queue.get()
            except:
                self.log(
                    'error', 'Failed to fetch containers to enumerate',
                    exc_info=True)
                eventlet.sleep(100)
                continue

            try:
                if not work:
                    break

                settings, per_account = work
                account = settings['internal_account']
                container = settings['internal_container']

                # Should we try caching the broker to avoid doing these
                # look ups every time?
                broker, nodes_count, node_id = self.get_broker(
                    account.encode('utf-8'), container.encode('utf-8'))
                if not broker:
                    continue

                if getattr(broker, 'is_sharded', lambda: False)():
                    self._enqueue_sharded_container(settings, per_account)

                broker_info = broker.get_info()
                broker_id = broker_info['id']
                handler = self.handler_factory.instance(
                    settings, per_account=per_account)

                last_primary_row = handler.get_last_processed_row(broker_id)
                if getattr(broker, 'is_root_container', lambda: True)():
                    handler.handle_container_info(broker_info, broker.metadata)
                primary_rows = self._get_new_rows(
                    broker, last_primary_row, nodes_count, node_id, False)
                if primary_rows:
                    self.log(
                        'info', 'Processing %d rows since row %d for %s/%s' % (
                            len(primary_rows), last_primary_row,
                            account, container))
                    primary_status = self.submit_items(
                        handler, primary_rows, job)
                    if ContainerJob.PASS_SUCCEEDED == primary_status:
                        handler.save_last_processed_row(
                            primary_rows[-1]['ROWID'], broker_id)
                        self.log(
                            'info',
                            'Processed %d rows; last row: %d; for %s/%s' % (
                                len(primary_rows), primary_rows[-1]['ROWID'],
                                account, container))

                last_verified_row = handler.get_last_verified_row(broker_id)
                verifying_rows = self._get_new_rows(
                    broker, last_verified_row, nodes_count, node_id, True)

                # Remove any ROWIDs that we uploaded
                uploaded_rows = set([row['ROWID'] for row in primary_rows])
                verifying_rows = filter(
                    lambda row: row['ROWID'] not in uploaded_rows,
                    verifying_rows)

                if verifying_rows:
                    self.log(
                        'info', 'Verifying %d rows since row %d for %s/%s' % (
                            len(verifying_rows), last_verified_row,
                            account, container))
                    verifying_status = self.submit_items(
                        handler, verifying_rows, job)
                    if ContainerJob.PASS_SUCCEEDED == verifying_status:
                        handler.save_last_verified_row(
                            verifying_rows[-1]['ROWID'], broker_id)
                        self.log('info',
                                 'Verified %d rows; last row: %d; '
                                 'for %s/%s' % (
                                     len(verifying_rows),
                                     verifying_rows[-1]['ROWID'],
                                     account, container))

            except SkipContainer:
                self.log(
                    'info', "Skipping %s/%s" % (account, container))
            except RetryError:
                # Can appear from the bulk handling code.
                # TODO: we should do a better job tying the bulk handling code
                # into this model.
                pass
            except:
                self.log('error', "Failed to process %s/%s with %s" % (
                    account, container, str(self.handler_factory)),
                    exc_info=True)
            finally:
                if work:
                    self._in_progress_containers.remove(
                        mapping_signature(work[0]))
                self.enumerator_queue.task_done()

    def log(self, level, message, **kwargs):
        if not self.logger:
            return
        getattr(self.logger, level)(message, **kwargs)

    def _get_db_info(self, account, container):
        """
        Returns the database path of the container

        :param account: UTF-8 encoded account name
        :param container: UTF-8 encoded container name
        :returns: a tuple of (db path, nodes count, index of replica)
        """

        part, container_nodes = self.container_ring.get_nodes(
            account, container)
        nodes_count = len(container_nodes)
        db_hash = hash_path(account, container)
        db_dir = storage_directory(DATADIR, part, db_hash)

        for index, node in enumerate(container_nodes):
            if not is_local_device(self.myips, None, node['ip'],
                                   node['port']):
                continue
            db_path = os.path.join(
                self.root, node['device'], db_dir, db_hash + '.db')
            return db_path, nodes_count, index
        return None, None, None

    def get_broker(self, account, container):
        """Instatiates a container database broker.

        :param account: UTF-8 encoded account name
        :param container: UTF-8 encoded container name
        :returns: a tuple of (ContainerBroker, nodes count, index of replica)
        """
        db_path, nodes_count, index = self._get_db_info(account, container)
        if db_path:
            broker = ContainerBroker(
                db_path, account=account, container=container)
            if broker.is_deleted():
                self.log('info', 'Database does not exist for %s/%s' %
                         (account, container))
            else:
                return broker, nodes_count, index
        return None, None, None

    def submit_items(self, handler, rows, job):
        if not rows:
            return ContainerJob.PASS_SUCCEEDED

        if self.bulk:
            with self._swift_pool.item() as swift_client:
                handler.handle(rows, swift_client)
            return ContainerJob.PASS_SUCCEEDED

        job.submit_tasks(map(lambda row: (row, handler), rows),
                         self.work_queue)
        return job.wait_all()

    def list_containers(self, account, prefix=''):
        # TODO: we should not have to retrieve all of the containers at once,
        # but it will require allocating a swift_client for this purpose from
        # the pool -- consider doing that at some point. However, as long as
        # there are fewer than a few million containers, getting all of them at
        # once should be cheap, paginating 10000 at a time.
        with self._swift_pool.item() as swift_client:
            return [c['name'] for c in swift_client.iter_containers(
                account, prefix=prefix)]

    def _prune_deleted_containers(self, account, containers, prefix=None):
        # After iterating over all of the containers, we prune any
        # entries from containers that may have been deleted (so as to
        # avoid missing data). There is still a chance where a
        # container is removed and created between the iterations, however
        # there is nothing we can do about that.
        # TODO: keep track of container creation date to detect when
        # they are removed and then added.
        account_status_dir = os.path.join(
            self.status_dir, account.encode('utf-8'))
        if not os.path.exists(account_status_dir):
            return

        if prefix:
            container_paths = glob.glob(
                os.path.join(account_status_dir, prefix + '*'))
            tracked_containers = [
                os.path.basename(path) for path in container_paths]
        else:
            tracked_containers = os.listdir(account_status_dir)

        disappeared = set(tracked_containers) - set(
            map(lambda container: container.encode('utf-8'), containers))
        for container in disappeared:
            try:
                os.unlink(os.path.join(account_status_dir, container))
            except Exception as e:
                self.log(
                    'warning',
                    'Failed to remove the status file for %s: %s' % (
                        os.path.join(account, container), repr(e)))

    def _prune_status_files(self):
        # Unlike _prune_deleted_containers, which prunes status files from the
        # per-account mappings, this prunes only unknown status files (i.e. the
        # mapping was removed).
        known_mappings = {mapping['account']: set()
                          for mapping in self.conf['containers']
                          if mapping.get('container')}
        for mapping in self.conf['containers']:
            if 'container' not in mapping:
                continue
            known_mappings[mapping['account']].add(mapping['container'])

        for account in os.listdir(unicode(self.status_dir)):
            account_path = os.path.join(self.status_dir, account)
            if not os.path.isdir(account_path):
                continue
            if account.startswith('.shards_'):
                # Sharded containers are handled separately
                continue
            if account not in known_mappings:
                try:
                    shutil.rmtree(account_path)
                except OSError as e:
                    self.log('warn', 'Failed to remove {}: {}'.format(
                        os.path.join(self.status_dir, account.encode('utf-8')),
                        e))
                continue
            if '/*' in known_mappings[account]:
                continue
            for container in os.listdir(account_path):
                if container not in known_mappings[account]:
                    try:
                        os.unlink(os.path.join(account_path, container))
                    except OSError as e:
                        self.log('warn', 'Failed to remove {}: {}'.format(
                            os.path.join(account_path.encode('utf-8'),
                                         unicode(container).encode('utf-8')),
                            e))

    def _is_container_sharded(self, account, container):
        """
        Retrieve container metadata with a HEAD request and
        find out if container is sharded.
        :returns: True if container is sharded. False otherwise.
        """
        with self._swift_pool.item() as swift_client:
            try:
                metadata = swift_client.get_container_metadata(
                    account, container)
            except UnexpectedResponse as err:
                if err.resp.status_int != HTTP_NOT_FOUND:
                    self.log(
                        'error',
                        'Failed to retrieve container metadata for %s: %s' % (
                            os.path.join(account, container), err.message))
                metadata = {}
            except Exception as err:
                self.log(
                    'error',
                    'Failed to retrieve container metadata for %s: %s' % (
                        os.path.join(account, container), err.message))
                metadata = {}

        return metadata.get('x-backend-sharding-state') == 'sharded'

    def _enqueue_sharded_container(self, settings, per_account=False):
        """
        Get list of shards for a given containers and add them to the
        work queue.
        """
        # TODO: look into saving the sharded state of the container
        sharded_account = '.shards_' + settings['account']
        sharded_container = settings['container']
        all_sharded_containers = self.list_containers(
            sharded_account, prefix=sharded_container)
        for container in all_sharded_containers:
            settings_copy = settings.copy()
            settings_copy['internal_account'] = sharded_account
            settings_copy['internal_container'] = container
            self._enqueue_container(settings_copy, per_account)
        self._prune_deleted_containers(
            sharded_account, all_sharded_containers,
            prefix=sharded_container)

    def _process_container(self, settings, per_account=False):
        # save internal account/containers as the actual account/containers
        # that will be crawled. This is currently useful for sharded containers
        account = settings['account']
        container = settings['container']
        settings['internal_account'] = account
        settings['internal_container'] = container

        try:
            db_path, _, _ = self._get_db_info(
                account.encode('utf-8'), container.encode('utf-8'))
        except:
            self.log('error', "Failed to process %s/%s" % (
                account, container), exc_info=True)
            return

        # if container db is not on local node, we need to check
        # if container is sharded with a HEAD request because
        # shards of that container could potentially be stored on this node
        # even if root container is not. Otherwise we check if container is
        # sharded when we have the broker.
        if db_path:
            self._enqueue_container(settings, per_account)
        elif self._is_container_sharded(account, container):
            self._enqueue_sharded_container(settings, per_account)

    def _enqueue_container(self, settings, per_account=False):
        settings_signature = mapping_signature(settings)
        if settings_signature not in self._in_progress_containers:
            self._in_progress_containers.add(settings_signature)
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
                    settings_copy['container'] = container
                    self._process_container(settings_copy, per_account=True)

                # clean status dir off containers that have been deleted
                self._prune_deleted_containers(container_settings['account'],
                                               all_containers)
            else:
                self._process_container(container_settings)
        self._prune_status_files()

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
