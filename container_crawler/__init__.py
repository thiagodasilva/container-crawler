import eventlet
import eventlet.pools
eventlet.patcher.monkey_patch(all=True)

import os.path
import time
import traceback

from swift.common.db import DatabaseConnectionError
from swift.common.ring import Ring
from swift.common.internal_client import InternalClient
from swift.common.ring.utils import is_local_device
from swift.common.utils import whataremyips, hash_path, storage_directory
from swift.common.wsgi import ConfigString

from swift.container.backend import DATADIR, ContainerBroker


class RetryError(Exception):
    pass


class ContainerCrawler(object):
    # TODO: pick up the IC configuration from /etc/swift
    INTERNAL_CLIENT_CONFIG = """
[DEFAULT]
[pipeline:main]
pipeline = catch_errors proxy-logging cache proxy-server

[app:proxy-server]
use = egg:swift#proxy

[filter:cache]
use = egg:swift#memcache

[filter:proxy-logging]
use = egg:swift#proxy_logging

[filter:catch_errors]
use = egg:swift#catch_errors
""".lstrip()

    def __init__(self, conf, handler_class, logger=None):
        if not handler_class:
            raise RuntimeError('Handler class must be defined')

        self.logger = logger
        self.conf = conf
        self.root = conf['devices']
        self.bulk = conf.get('bulk_process', False)
        self.interval = 10
        self.swift_dir = '/etc/swift'
        self.container_ring = Ring(self.swift_dir, ring_name='container')

        self.status_dir = conf['status_dir']
        self.myips = whataremyips('0.0.0.0')
        self.items_chunk = conf['items_chunk']
        self.poll_interval = conf.get('poll_interval', 5)
        self.handler_class = handler_class

        if not self.bulk:
            self._init_workers(conf)
        self._init_ic_pool(conf)

        self.log('debug', 'Created the Container Crawler instance')

    def _init_workers(self, conf):
        self.workers = conf.get('workers', 10)
        self.pool = eventlet.GreenPool(self.workers)
        self.work_queue = eventlet.queue.Queue(self.workers * 2)

        # max_size=None means a Queue is infinite
        self.error_queue = eventlet.queue.Queue(maxsize=None)
        self.stats_queue = eventlet.queue.Queue(maxsize=None)
        for _ in xrange(self.workers):
            self.pool.spawn_n(self._worker)

    def _get_internal_client_config(self, conf):
        ic_conf_path = conf.get(
            'internal_client_path',
            os.path.join(self.swift_dir, 'internal-client.conf'))
        if os.path.exists(ic_conf_path):
            return ic_conf_path
        return ConfigString(self.INTERNAL_CLIENT_CONFIG)

    def _init_ic_pool(self, conf):
        ic_config = self._get_internal_client_config(conf)
        ic_name = conf.get('internal_client_logname', 'ContainerCrawler')
        pool_size = conf.get('workers', 1)
        self._swift_pool = eventlet.pools.Pool(
            create=lambda: InternalClient(ic_config, ic_name, 3),
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
                time.sleep(100)
                continue

            try:
                if work:
                    row, handler = work
                    with self._swift_pool.item() as swift_client:
                        handler.handle(row, swift_client)
            except RetryError:
                self.error_queue.put((row, None))
            except:
                self.error_queue.put((row, traceback.format_exc()))
            finally:
                self.work_queue.task_done()

    def _stop(self):
        for _ in xrange(self.workers):
            self.work_queue.put(None)
        self.pool.waitall()

    def _check_errors(self):
        if self.error_queue.empty():
            return

        retry_error = False

        while not self.error_queue.empty():
            row, error = self.error_queue.get()
            if error:
                self.log('error', u'Failed to handle row %s (%s): %r' % (
                    row['ROWID'], row['name'].decode('utf-8'), error))
            else:
                retry_error = True
        if not retry_error:
            raise RuntimeError('Failed to process rows')
        else:
            raise RetryError('Rows must be retried later')

    def log(self, level, message):
        if not self.logger:
            return
        getattr(self.logger, level)(message)

    def get_broker(self, account, container, part, node):
        db_hash = hash_path(account, container)
        db_dir = storage_directory(DATADIR, part, db_hash)
        db_path = os.path.join(self.root, node['device'], db_dir,
                               db_hash + '.db')
        return ContainerBroker(db_path, account=account, container=container)

    def submit_items(self, handler, rows):
        if self.bulk:
            with self._swift_pool.item() as swift_client:
                handler.handle(rows, swift_client)
            return

        for row in rows:
            self.work_queue.put((row, handler))
        self.work_queue.join()
        self._check_errors()

    def process_items(self, handler, rows, nodes_count, node_id):
        owned_rows = filter(
            lambda row: row['ROWID'] % nodes_count == node_id, rows)
        self.submit_items(handler, owned_rows)

        verified_rows = filter(
            lambda row: row['ROWID'] % nodes_count != node_id, rows)
        if verified_rows:
            self.submit_items(handler, verified_rows)

    def handle_container(self, handler):
        part, container_nodes = self.container_ring.get_nodes(
            handler._account, handler._container)
        nodes_count = len(container_nodes)

        for index, node in enumerate(container_nodes):
            if not is_local_device(self.myips, None, node['ip'],
                                   node['port']):
                continue
            broker = self.get_broker(handler._account,
                                     handler._container,
                                     part, node)
            broker_info = broker.get_info()
            last_row = handler.get_last_row(broker_info['id'])
            if not last_row:
                last_row = 0
            try:
                items = broker.get_items_since(last_row, self.items_chunk)
            except DatabaseConnectionError:
                continue
            if items:
                self.process_items(handler, items, nodes_count, index)
                handler.save_last_row(items[-1]['ROWID'], broker_info['id'])

    def call_handle_container(self, settings, per_account=False):
        """ Thin wrapper around the handle_container() method for error
            handling.

            Arguments
            settings -- dictionary with settings used for the
            per_account -- whether the whole account is crawled.
        """
        try:
            handler = self.handler_class(self.status_dir, settings,
                                         per_account=per_account)
            self.handle_container(handler)
        except RetryError:
            pass
        except:
            account = settings['account']
            container = settings['container']
            self.log('error', "Failed to process %s/%s with %s" % (
                account.decode('utf-8'), container.decode('utf-8'),
                self.handler_class.__name__))
            self.log('error', traceback.format_exc())

    def list_containers(self, account):
        # TODO: we should not have to retrieve all of the containers at once,
        # but it will require allocating a swift_client for this purpose from
        # the pool -- consider doing that at some point. However, as long as
        # there are fewer than a few million containers, getting all of them at
        # once should be cheap, paginating 10000 at a time.
        with self._swift_pool.item() as swift_client:
            return [c['name'] for c in swift_client.iter_containers(account)]

    def run_always(self):
        # Since we don't support reloading, the daemon should quit if there are
        # no containers configured
        if 'containers' not in self.conf or not self.conf['containers']:
            return
        self.log('debug', 'Entering the poll loop')
        while True:
            start = time.time()
            self.run_once()
            elapsed = time.time() - start
            if elapsed < self.poll_interval:
                time.sleep(self.poll_interval - elapsed)

    def run_once(self):
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
                    'Account not in specified in settings -- continue')
                continue

            if container_settings['container'] == '/*':
                all_containers = self.list_containers(
                    container_settings['account'])
                for container in all_containers:
                    settings_copy = container_settings.copy()
                    settings_copy['container'] = container
                    self.call_handle_container(settings_copy, per_account=True)
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
                self.call_handle_container(container_settings)
