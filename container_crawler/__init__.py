import logging
import os.path
import time
import traceback

from swift.common.db import DatabaseConnectionError
from swift.common.ring import Ring
from swift.common.ring.utils import is_local_device
from swift.common.utils import whataremyips, hash_path, storage_directory
from swift.container.backend import DATADIR, ContainerBroker


class ContainerCrawler(object):
    def __init__(self, conf, handler_class):
        self.logger = logging.getLogger('container-crawler')
        self.conf = conf
        self.root = conf['devices']
        self.interval = 10
        self.swift_dir = '/etc/swift'
        self.container_ring = Ring(self.swift_dir, ring_name='container')

        self.status_dir = conf['status_dir']
        self.myips = whataremyips('0.0.0.0')
        self.items_chunk = conf['items_chunk']
        self.poll_interval = conf.get('poll_interval', 5)
        self.handler_class = handler_class

        self.logger.debug('Created the Container Crawler instance')

    def get_broker(self, account, container, part, node):
        db_hash = hash_path(account, container)
        db_dir = storage_directory(DATADIR, part, db_hash)
        db_path = os.path.join(self.root, node['device'], db_dir,
                               db_hash + '.db')
        return ContainerBroker(db_path, account=account, container=container)

    # TODO: use green threads here
    def process_items(self, handler, rows, nodes_count, node_id):
        errors = False
        for row in rows:
            if (row['ROWID'] % nodes_count) != node_id:
                continue
            if self.logger:
                self.logger.debug('propagating %s' % row['ROWID'])
            try:
                handler.handle(row)
            except Exception as e:
                self.logger.error('Failed to handle row %s: %s' % (
                    row['ROWID'], repr(e)))
                errors = True
        if errors:
            raise RuntimeError('Failed to scan %s' % (str(handler)))

        for row in rows:
            # Validate the changes from other rows
            if (row['ROWID'] % nodes_count) == node_id:
                continue
            if self.logger:
                self.logger.debug('verifiying %s' % row['ROWID'])
            try:
                handler.handle(row)
            except Exception as e:
                self.logger.error('Failed to verify row %s: %s' % (
                    row['ROWID'], repr(e)))
                errors = True
        if errors:
            raise RuntimeError('Failed to verify %s' % (str(handler)))

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
                self.process_items(settings, items, nodes_count, index)
                handler.save_last_row(items[-1]['ROWID'])
            return

    def run_always(self):
        # Since we don't support reloading, the daemon should quit if there are
        # no containers configured
        if 'containers' not in self.conf or not self.conf['containers']:
            return
        self.logger.debug('Entering the poll loop')
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
                self.logger.error("Failed to process %s/%s with %s: %s" %
                                  (account, container, self.handler_class,
                                   repr(e)))
                self.logger.error(traceback.format_exc(e))
