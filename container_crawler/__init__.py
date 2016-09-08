import os.path
import time
import traceback

from swift.common.db import DatabaseConnectionError
from swift.common.ring import Ring
from swift.common.ring.utils import is_local_device
from swift.common.utils import whataremyips, hash_path, storage_directory
from swift.container.backend import DATADIR, ContainerBroker


class ContainerCrawler(object):
    def __init__(self, conf, handler_class, logger=None):
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

        self.log('debug', 'Created the Container Crawler instance')

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
            return handler.handle(rows)

        errors = []
        for row in rows:
            # TODO: use green threads here
            self.log('debug', 'handling row %s' % row['ROWID'])
            try:
                handler.handle(row)
            except Exception as e:
                self.log('error', 'Failed to handle row %s: %s' % (
                    row['ROWID'], repr(e)))
                errors.append((row, e))
        return errors

    def process_items(self, handler, rows, nodes_count, node_id):
        owned_rows = filter(
            lambda row: row['ROWID'] % nodes_count == node_id, rows)
        if self.submit_items(handler, owned_rows):
            raise RuntimeError('Failed to process rows')

        verified_rows = filter(
            lambda row: row['ROWID'] % nodes_count != node_id, rows)
        if self.submit_items(handler, verified_rows):
            raise RuntimeError('Failed to verify rows')

    def handle_container(self, settings):
        part, container_nodes = self.container_ring.get_nodes(
            settings['account'], settings['container'])
        nodes_count = len(container_nodes)
        handler = self.handler_class(self.status_dir, settings)

        for index, node in enumerate(container_nodes):
            if not is_local_device(self.myips, None, node['ip'],
                                   node['port']):
                continue
            last_row = handler.get_last_row()
            if not last_row:
                last_row = 0
            broker = self.get_broker(settings['account'],
                                     settings['container'],
                                     part, node)
            try:
                items = broker.get_items_since(last_row, self.items_chunk)
            except DatabaseConnectionError:
                continue
            if items:
                self.process_items(handler, items, nodes_count, index)
                handler.save_last_row(items[-1]['ROWID'])
            return

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
            try:
                self.handle_container(container_settings)
            except Exception as e:
                account = container_settings.get('account', 'N/A')
                container = container_settings.get('container', 'N/A')
                self.log('error', "Failed to process %s/%s with %s: %s" % (
                    account, container, self.handler_class, repr(e)))
                self.log('error', traceback.format_exc(e))
