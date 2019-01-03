# -*- coding: UTF-8 -*-

from contextlib import contextmanager
from tempfile import mkdtemp

import eventlet
import mock
import os
import shutil
import time
import unittest

from swift.common.utils import Timestamp

from container_crawler import crawler
from container_crawler.base_sync import BaseSync
from container_crawler.exceptions import RetryError


class TestContainerCrawler(unittest.TestCase):

    @mock.patch('container_crawler.utils.InternalClient')
    @mock.patch('container_crawler.crawler.Ring')
    def _setup_mocked_crawler(self, mock_ring, mock_ic):
        self.mock_ring = mock.Mock(
            get_nodes=mock.Mock(
                return_value=('deadbeef',
                              [{'ip': '127.0.0.1',
                                'port': 1234,
                                'device': '/dev/sda'}])))
        mock_ring.return_value = self.mock_ring
        self.mock_handler = mock.Mock(
            handle_container_metadata=mock.Mock(return_value=None),
            get_last_processed_row=mock.Mock(return_value=42),
            get_last_verified_row=mock.Mock(return_value=21))
        self.mock_handler_factory = mock.Mock(
            __str__=lambda obj: 'MockHandler',
            instance=mock.Mock(return_value=self.mock_handler))

        self.mock_ic = mock.Mock()
        mock_ic.return_value = self.mock_ic
        self.mock_broker = mock.Mock(
            get_info=mock.Mock(return_value={'id': 'hash'}),
            get_items_since=mock.Mock(return_value=[]),
            is_sharded=mock.Mock(return_value=False),
            is_deleted=mock.Mock(return_value=False),
            # Note that this is slightly different from real symantics as
            # metadata is a property, not a static value
            metadata={})

        self.crawler = crawler.Crawler(self.conf, self.mock_handler_factory)

    def setUp(self):
        self.conf = {
            'devices': '/devices',
            'items_chunk': 1000,
            'status_dir': mkdtemp(),
            'containers': [{'account': 'account',
                            'container': 'container'}]}
        self._setup_mocked_crawler()

    def tearDown(self):
        shutil.rmtree(self.conf['status_dir'])

    @contextmanager
    def _patch_broker(self):
        with mock.patch(
                'container_crawler.crawler.ContainerBroker') as mock_broker:
            mock_broker.return_value = self.mock_broker
            yield mock_broker

    def test_enumerator_worker(self):
        total_rows = 100
        items = [{'ROWID': x, 'created_at': Timestamp(time.time())}
                 for x in range(total_rows)]

        self.mock_broker.get_items_since.return_value = items
        handler = self.mock_handler
        logger = mock.Mock()
        self.crawler.logger = logger
        self.mock_ic.iter_containers.return_value = []

        for nodes in range(1, 7):
            for node_id in range(0, nodes):
                all_nodes = [
                    {'ip': '1.2.3.4', 'port': 1234, 'device': '/dev/sda'}
                    for _ in range(nodes)]
                all_nodes[node_id]['ip'] = '127.0.0.1'

                self.mock_ring.get_nodes.return_value = ('deadbeef', all_nodes)
                handler.handle.reset_mock()
                handler.save_last_processed_row.reset_mock()
                handler.save_last_verified_row.reset_mock()
                handler.handle_container_metadata.reset_mock()
                logger.info.reset_mock()

                handle_calls = filter(lambda x: x % nodes == node_id,
                                      range(total_rows))
                verify_calls = filter(lambda x: x not in handle_calls,
                                      range(total_rows))

                with self._patch_broker():
                    self.crawler.run_once()

                expected = [mock.call(items[x], self.mock_ic)
                            for x in handle_calls]
                expected += [mock.call(items[x], self.mock_ic)
                             for x in verify_calls]
                self.assertEqual(
                    expected, self.mock_handler.handle.call_args_list)
                handler.save_last_processed_row.assert_called_once_with(
                    items[handle_calls[-1]]['ROWID'],
                    self.mock_broker.get_info()['id'])
                if verify_calls:
                    handler.save_last_verified_row.assert_called_once_with(
                        items[verify_calls[-1]]['ROWID'],
                        self.mock_broker.get_info()['id'])

                handler.handle_container_metadata.assert_called_once_with(
                    {}, mock.ANY)
                logging_calls = [
                    mock.call('Processing %d rows since row %d for %s/%s' % (
                        len(handle_calls), 42, 'account', 'container')),
                    mock.call('Processed %d rows; last row: %d; for %s/%s' % (
                        len(handle_calls), items[handle_calls[-1]]['ROWID'],
                        'account', 'container')),
                ]
                if verify_calls:
                    logging_calls.append(
                        mock.call('Verifying %d rows since row %d for %s/%s'
                                  % (len(verify_calls), 21, 'account',
                                     'container')))
                    logging_calls.append(
                        mock.call('Verified %d rows; last row: %d; for %s/%s'
                                  % (len(verify_calls),
                                     items[verify_calls[-1]]['ROWID'],
                                     'account', 'container')))
                self.assertEqual(logging_calls, logger.info.mock_calls)

    def test_bulk_handling(self):
        self.conf['bulk_process'] = 'true'
        self._setup_mocked_crawler()
        self.assertEqual(self.crawler.bulk, True)

        total_rows = 20
        items = [{'ROWID': x, 'created_at': Timestamp(time.time())}
                 for x in range(total_rows)]

        self.mock_broker.get_items_since.return_value = items

        expected = [mock.call([items[x] for x in range(total_rows)],
                    self.mock_ic)]

        with self._patch_broker():
            self.crawler.run_once()

        self.assertEqual(
            expected, self.mock_handler.handle.call_args_list)

    @mock.patch('container_crawler.crawler.traceback')
    def test_bulk_errors(self, tb_mock):
        self.conf['bulk_process'] = 'true'
        self._setup_mocked_crawler()
        self.assertEqual(self.crawler.bulk, True)
        tb_mock.format_exc.return_value = 'exception traceback'

        self.mock_handler.handle.side_effect = RuntimeError('error')
        self.mock_handler.get_last_row.return_value = 42
        self.mock_broker.get_items_since.return_value = [
            {'ROWID': 1, 'name': 'foo', 'created_at': Timestamp(time.time())}]
        self.crawler.logger = mock.Mock()

        with self._patch_broker():
            self.crawler.run_once()

        self.assertEqual(
            [mock.call('Failed to process %s/%s with %s' % (
                       self.conf['containers'][0]['account'],
                       self.conf['containers'][0]['container'],
                       str(self.mock_handler_factory))),
             mock.call(tb_mock.format_exc.return_value)],
            self.crawler.logger.error.mock_calls)

    @mock.patch('container_crawler.crawler.traceback')
    def test_failed_handler_factory_instance(self, tb_mock):
        self.crawler.handler_factory.instance.side_effect =\
            RuntimeError('oops')
        self.crawler.logger = mock.Mock()
        tb_mock.format_exc.return_value = 'exception traceback'
        self.mock_ic.iter_containers.return_value = []

        settings = {'account': 'AUTH_account',
                    'container': 'container'}
        self.crawler.conf['containers'] = [settings]
        with self._patch_broker():
            self.crawler.run_once()
        self.crawler.handler_factory.instance.assert_called_once_with(
            settings, per_account=False)
        self.assertEqual(
            [mock.call.error('Failed to process AUTH_account/container with '
                             'MockHandler'),
             mock.call.error(tb_mock.format_exc.return_value)],
            self.crawler.logger.mock_calls)

    def test_enumerator_handling_rows_errors(self):
        rows = 10
        items = [{'ROWID': x, 'name': str(x),
                  'created_at': Timestamp(time.time())}
                 for x in range(rows)]
        self.mock_broker.get_items_since.return_value = items

        for node_id in (0, 1):
            all_nodes = [{'ip': '1.2.3.4', 'port': 1234, 'device': '/dev/sda'}
                         for _ in range(2)]
            all_nodes[node_id]['ip'] = '127.0.0.1'

            self.mock_ring.get_nodes.return_value = ('deadbeef', all_nodes)

            self.mock_handler.handle.reset_mock()
            self.mock_handler.handle.side_effect = RuntimeError('oops')

            with self._patch_broker():
                self.crawler.run_once()

            handle_rows = filter(lambda x: x % 2 == node_id, range(rows))
            verify_rows = filter(lambda x: x % 2 != node_id, range(rows))
            expected = [mock.call(items[row_id], self.mock_ic)
                        for row_id in handle_rows + verify_rows]
            self.assertEqual(expected, self.mock_handler.handle.call_args_list)

    def test_enumerator_verify_items_errors(self):
        rows = 10
        items = [{'ROWID': x, 'name': str(x),
                  'created_at': Timestamp(time.time())}
                 for x in range(rows)]
        self.mock_broker.get_items_since.return_value = items

        for node_id in (0, 1):
            # only fail the verify calls
            def fail_verify(row, client):
                if row['ROWID'] % 2 != node_id:
                    raise RuntimeError('oops')
                return

            all_nodes = [{'ip': '1.2.3.4', 'port': 1234, 'device': '/dev/sda'}
                         for _ in range(2)]
            all_nodes[node_id]['ip'] = '127.0.0.1'

            self.mock_ring.get_nodes.return_value = ('deadbeef', all_nodes)
            self.mock_handler.handle.reset_mock()
            self.mock_handler.handle.side_effect = fail_verify

            with self._patch_broker():
                self.crawler.run_once()

            handle_calls = filter(lambda x: x % 2 == node_id, range(rows))
            verify_calls = filter(lambda x: x % 2 != node_id, range(rows))
            expected = [mock.call(items[row_id], self.mock_ic)
                        for row_id in handle_calls]
            expected += [mock.call(items[row_id], self.mock_ic)
                         for row_id in verify_calls]
            self.assertEqual(
                expected,
                self.mock_handler.handle.call_args_list)

    @mock.patch('container_crawler.crawler.traceback.format_exc')
    def test_unicode_object_failure(self, mock_tb):
        row = {'ROWID': 42,
               'name': 'monkey-\xf0\x9f\x90\xb5',
               'created_at': Timestamp(time.time())}
        error = RuntimeError('fail')
        self.mock_handler.handle.side_effect = error
        self.mock_broker.get_items_since.return_value = [row]
        self.crawler.logger = mock.Mock()
        mock_tb.return_value = 'traceback'

        with self._patch_broker():
            self.crawler.run_once()

        self.assertEqual(
            [mock.call("Failed to handle row %d (%s): 'traceback'" % (
                       row['ROWID'], row['name'].decode('utf-8')))],
            self.crawler.logger.error.mock_calls)

    @mock.patch('container_crawler.crawler.traceback.format_exc')
    def test_worker_handles_all_exceptions(self, tb_mock):
        self.mock_handler.handle.side_effect = BaseException('base error')
        tb_mock.return_value = 'traceback'
        self.crawler.logger = mock.Mock()

        row = {'name': 'failed',
               'deleted': False,
               'ROWID': 1,
               'created_at': str(time.time())}
        self.mock_broker.get_items_since.return_value = [row]

        with self._patch_broker():
            self.crawler.run_once()

        self.assertEqual([mock.call(
            "Failed to handle row %d (%s): 'traceback'" % (
                row['ROWID'], row['name'].decode('utf-8')))],
            self.crawler.logger.error.mock_calls)

    @mock.patch('container_crawler.crawler.traceback.format_exc')
    def test_unicode_container_account_failure(self, tb_mock):
        container = {
            'account': 'account-\xf0\x9f\x90\xb5',
            'container': 'container-\xf0\x9f\x90\xb5'
        }
        tb_mock.return_value = 'traceback'
        self.crawler.conf['containers'] = [container]
        self.crawler.find_new_rows = mock.Mock(
            side_effect=BaseException('base error'))
        self.crawler.logger = mock.Mock()

        self.crawler.run_once()
        self.assertEqual(
            [mock.call('Failed to process %s/%s' % (
                container['account'],
                container['container'])),
             mock.call('traceback')],
            self.crawler.logger.error.mock_calls)

    @mock.patch('container_crawler.crawler.time')
    def test_exit_if_no_containers(self, time_mock):
        self.crawler.conf['containers'] = []
        time_mock.sleep.side_effect = RuntimeError('Should not sleep')
        self.crawler.run_always()

    @mock.patch('container_crawler.crawler.traceback.format_exc')
    def test_no_exception_on_failure(self, format_exc_mock):
        containers = [
            {'account': 'foo',
             'container': 'bar'},
            # missing container parameter should not cause an exception
            {'account': 'foo'}
        ]
        self.crawler.conf['containers'] = containers

        self.crawler.logger = mock.Mock()
        format_exc_mock.return_value = 'traceback'
        self.mock_broker.get_items_since.side_effect = RuntimeError('oops')

        with self._patch_broker():
            self.crawler.run_once()

        expected_handler_calls = [mock.call(containers[0], per_account=False)]
        self.assertEqual(expected_handler_calls,
                         self.mock_handler_factory.instance.mock_calls)
        expected_logger_calls = [
            mock.call("Container name not specified in settings -- continue"),
            mock.call("Failed to process foo/bar with %s" %
                      str(self.crawler.handler_factory)),
            mock.call('traceback'),
        ]
        self.assertEqual(expected_logger_calls,
                         self.crawler.logger.error.call_args_list)

    def test_processes_every_container(self):
        self.crawler.find_new_rows = mock.Mock(
            return_value=[[], [], None, None, None])
        self.crawler.conf['containers'] = [
            {'account': 'foo',
             'container': 'foo'},
            {'account': 'bar',
             'container': 'bar'}
        ]

        with self._patch_broker():
            self.crawler.run_once()
        expected_calls = [mock.call(container, per_account=False)
                          for container in self.crawler.conf['containers']]
        self.assertEqual(expected_calls,
                         self.mock_handler_factory.instance.mock_calls)

    @mock.patch('container_crawler.crawler.ContainerBroker')
    @mock.patch('os.path.exists')
    @mock.patch('os.listdir')
    def test_handles_every_container_in_account(
            self, ls_mock, exists_mock, broker_mock):
        account = 'foo'
        self.crawler.conf['containers'] = [
            {'account': account,
             'container': '/*'}
        ]
        test_containers = ['foo', 'bar', 'baz', u'fo\u00f4']

        self.mock_ic.iter_containers.return_value = [
            {'name': container} for container in test_containers]
        ls_mock.return_value = test_containers
        broker_mock.return_value = self.mock_broker

        class FakeHandler(BaseSync):
            def __init__(self, *args, **kwargs):
                super(FakeHandler, self).__init__('', *args, **kwargs)

            def handle(self, row):
                pass

            def get_last_processed_row(self, db_id):
                return 5000

            def get_last_verified_row(self, db_id):
                return 10

            def save_last_processed_row(self, row_id, db_id):
                pass

            def handle_container_metadata(self, metadata, db_id):
                pass

            def save_last_verified_row(self, row_id, db_id):
                pass

        self.mock_handler_factory.instance.side_effect = FakeHandler
        node = {'ip': '127.0.0.1', 'port': '8888', 'device': '/dev/foobar'}
        self.crawler.container_ring.get_nodes.return_value = (
            'deadbeef', [node])

        exists_mock.return_value = True

        self.crawler.run_once()

        self.mock_ic.iter_containers.assert_called_once_with(account,
                                                             prefix='')

        expected = [
            (mock.call.is_deleted(),
             mock.call.is_sharded(),
             mock.call.get_info(),
             mock.call.get_items_since(5000, 1000),
             mock.call.get_items_since(10, 1000))
            for _ in test_containers]
        self.assertEqual(
            reduce(lambda x, y: list(x) + list(y), expected),
            self.mock_broker.mock_calls)
        ls_mock.assert_called_once_with(
            '%s/%s' % (self.conf['status_dir'], account))
        expected_handler_calls = [
            mock.call({'account': account,
                       'root_account': account,
                       'root_container': container,
                       'container': container},
                      per_account=True)
            for container in test_containers]
        self.assertEqual(
            expected_handler_calls,
            self.mock_handler_factory.instance.mock_calls)

    @mock.patch('os.path.exists')
    @mock.patch('os.unlink')
    @mock.patch('os.listdir')
    def test_removes_missing_status_files(self, ls_mock, unlink_mock,
                                          exists_mock):
        account = u'fo\u00ef'
        self.crawler.conf['containers'] = [
            {'account': account,
             'container': '/*'}
        ]
        test_containers = [u'foo', u'bar', u'baz', u'unic\u062fde']

        self.mock_ic.iter_containers.return_value = []
        ls_mock.return_value = [container.encode('utf-8')
                                for container in test_containers]
        exists_mock.return_value = True

        self.crawler.run_once()

        self.mock_ic.iter_containers.assert_called_once_with(account,
                                                             prefix='')
        ls_mock.assert_called_once_with(
            ('%s/%s' % (self.conf['status_dir'], account)).encode('utf-8'))
        self.assertEqual([
            mock.call(('%s/%s/%s' % (self.conf['status_dir'], account, cont))
                      .encode('utf-8'))
            for cont in sorted(test_containers)],
            sorted(unlink_mock.mock_calls))

    def test_handle_retry_error(self):
        rows = [{'name': 'foo', 'ROWID': 1,
                 'created_at': Timestamp(time.time())}]
        self.mock_broker.get_items_since.return_value = rows

        self.crawler.submit_items = mock.Mock()
        self.crawler.submit_items.side_effect = RetryError

        settings = {
            'account': 'foo',
            'container': 'bar'
        }

        self.crawler.conf = {'containers': [settings]}
        with self._patch_broker():
            self.crawler.run_once()

        self.mock_handler.get_last_processed_row\
            .assert_called_once_with('hash')
        self.assertEqual(
            [], self.mock_handler.save_last_processed_row.mock_calls)
        self.crawler.submit_items.assert_called_once_with(
            self.mock_handler, rows, mock.ANY)

    def test_handle_unicode_account_container(self):
        account = u'ünìçódê'
        container = u'c😁ntainer'
        self.conf['containers'] = [
            {'account': account,
             'container': container}]
        self.crawler.logger = mock.Mock()
        row = {'name': 'object', 'ROWID': 1337,
               'created_at': Timestamp(time.time())}
        self.mock_broker.get_items_since.side_effect = [[row], []]
        self.mock_handler._account = account
        self.mock_handler._container = container

        with self._patch_broker():
            self.crawler.run_once()

        self.assertEqual(
            [mock.call.info('Processing 1 rows since row 42 for %s/%s' % (
                account, container)),
             mock.call.info(
                'Processed 1 rows; last row: 1337; for %s/%s' % (
                    account, container))],
            self.crawler.logger.mock_calls)
        self.mock_handler_factory.instance.assert_called_once_with(
            self.conf['containers'][0], per_account=False)
        self.mock_handler.get_last_processed_row\
            .assert_called_once_with('hash')
        self.mock_handler.handle.assert_called_once_with(
            row, self.mock_ic)
        self.mock_handler.save_last_processed_row\
            .assert_called_once_with(1337, 'hash')

    def test_verifying_rows(self):
        self.mock_ring.get_nodes.return_value = (
            'deadbeef',
            [{'ip': '1.2.3.4',
              'port': 1234,
              'device': '/dev/sda'},
             {'ip': '127.0.0.1',
              'port': 1234,
              'device': '/dev/sda'}])
        rows = [{'ROWID': i,
                 'name': 'obj%d' % i,
                 'created_at': Timestamp(time.time())}
                for i in range(80, 100, 2)]
        self.mock_broker.get_items_since.side_effect = ([], rows)
        self.crawler.logger = mock.Mock()
        self.mock_handler._account = 'account'
        self.mock_handler._container = 'container'

        with self._patch_broker():
            self.crawler.run_once()

        self.assertEqual(
            [mock.call.info('Verifying %d rows since row %d for %s/%s' % (
                len(rows), 21, 'account', 'container')),
             mock.call.info('Verified %d rows; last row: %d; for %s/%s'
                            % (len(rows), rows[-1]['ROWID'],
                               self.mock_handler._account,
                               self.mock_handler._container))],
            self.crawler.logger.mock_calls)
        self.assertEqual([mock.call(row, self.mock_ic) for row in rows],
                         self.mock_handler.handle.mock_calls)

    @mock.patch('container_crawler.utils.InternalClient')
    @mock.patch('container_crawler.crawler.Ring')
    def test_bulk_init(self, mock_ring, mock_ic):
        self.mock_ring = mock.Mock()
        mock_ring.return_value = self.mock_ring
        self.mock_handler_factory = mock.Mock(
            __str__=lambda obj: 'MockHandler')

        self.mock_ic = mock.Mock()
        mock_ic.return_value = self.mock_ic

        del self.conf['containers']
        self.conf['bulk_process'] = True
        self.conf['enumerator_workers'] = 42
        daemon = crawler.Crawler(
            self.conf, self.mock_handler_factory)
        self.assertEqual(True, daemon.bulk)
        self.assertEqual(1, daemon._swift_pool.min_size)
        self.assertEqual(1, daemon._swift_pool.max_size)
        self.assertEqual(1, daemon._swift_pool.current_size)
        self.assertEqual(42, daemon.enumerator_pool.size)
        self.assertEqual(42, daemon.enumerator_queue.maxsize)

    @mock.patch('container_crawler.utils.InternalClient')
    @mock.patch('container_crawler.crawler.Ring')
    def test_no_bulk_init(self, mock_ring, mock_ic):
        self.mock_ring = mock.Mock()
        mock_ring.return_value = self.mock_ring

        self.mock_ic = mock.Mock()
        mock_ic.return_value = self.mock_ic

        del self.conf['containers']
        self.conf['bulk_process'] = False
        self.conf['workers'] = 50
        self.conf['enumerator_workers'] = 84
        daemon = crawler.Crawler(
            self.conf, self.mock_handler_factory)
        self.assertEqual(False, daemon.bulk)
        self.assertEqual(50, daemon._swift_pool.min_size)
        self.assertEqual(50, daemon._swift_pool.max_size)
        self.assertEqual(50, daemon._swift_pool.current_size)
        self.assertEqual(50, daemon.worker_pool.size)
        self.assertEqual(100, daemon.work_queue.maxsize)
        self.assertEqual(84, daemon.enumerator_pool.size)
        self.assertEqual(84, daemon.enumerator_queue.maxsize)

    def test_one_handler_per_container(self):
        handler_progress = eventlet.queue.Queue()

        def _handle(*args):
            handler_progress.get()
            return

        self.mock_handler.handle.side_effect =\
            _handle

        containers = [
            {'account': u'fo\u00f2',
             'container': u'b\u00e1r'},
            {'account': u'fo\u00f2',
             'container': u'q\u00fax'},
        ]
        self.crawler.conf['containers'] = containers

        self.crawler._submit_containers()
        self.assertEqual(2, self.crawler.enumerator_queue.unfinished_tasks)
        self.crawler._submit_containers()
        self.assertEqual(2, self.crawler.enumerator_queue.unfinished_tasks)
        map(handler_progress.put, [None] * 2)
        self.crawler.enumerator_queue.join()
        self.assertEqual(0, self.crawler.enumerator_queue.unfinished_tasks)
        self.assertEqual(0, len(self.crawler._in_progress_containers))

        self.crawler._submit_containers()
        self.assertEqual(2, self.crawler.enumerator_queue.unfinished_tasks)
        map(handler_progress.put, [None] * 2)
        self.crawler.enumerator_queue.join()
        self.assertEqual(0, self.crawler.enumerator_queue.unfinished_tasks)
        self.assertEqual(0, len(self.crawler._in_progress_containers))

    def test_one_handler_per_account_containers(self):
        handler_progress = eventlet.queue.Queue()

        def _handle(*args):
            handler_progress.get()
            return

        mock_handler_instance = mock.Mock()
        mock_handler_instance.handler.side_effect = handler_progress
        self.mock_handler_factory.instance.return_value = mock_handler_instance

        containers = [u'b\u00e1r', u'q\u00fax']
        self.crawler.conf['containers'] = [
            {'account': u'fo\u00f2',
             'container': '/*'}]
        self.crawler.list_containers = mock.Mock(return_value=containers)

        self.crawler._submit_containers()
        self.assertEqual(2, self.crawler.enumerator_queue.unfinished_tasks)
        self.crawler._submit_containers()
        self.assertEqual(2, self.crawler.enumerator_queue.unfinished_tasks)
        map(handler_progress.put, [None] * 2)
        self.crawler.enumerator_queue.join()
        self.assertEqual(0, self.crawler.enumerator_queue.unfinished_tasks)
        self.assertEqual(0, len(self.crawler._in_progress_containers))

        self.crawler._submit_containers()
        self.assertEqual(2, self.crawler.enumerator_queue.unfinished_tasks)
        map(handler_progress.put, [None] * 2)
        self.crawler.enumerator_queue.join()
        self.assertEqual(0, self.crawler.enumerator_queue.unfinished_tasks)
        self.assertEqual(0, len(self.crawler._in_progress_containers))

    def test_skip_non_local_containers(self):
        self.mock_handler_factory.instance.side_effect =\
            RuntimeError('Should not be called!')

        self.mock_ring.get_nodes.return_value = ['part', []]
        self.crawler.conf['containers'] = [
            {'account': 'foo',
             'container': 'bar'},
            {'account': 'foo',
             'container': 'baz'}]
        self.crawler.logger = mock.Mock()
        metadata = {'x-backend-sharding-state': 'unsharded'}
        self.mock_ic.get_container_metadata.return_value = metadata

        self.crawler._submit_containers()
        self.crawler.enumerator_queue.join()
        self.crawler.logger.error.assert_not_called()
        self.mock_handler.save_last_row.assert_not_called()

    @mock.patch('container_crawler.crawler.ContainerBroker')
    def test_skip_missing_containers(self, broker_mock):
        # NB: covers both db-file-is-not-on-disk and db-file-is-marked-deleted

        broker = mock.Mock()
        broker.is_deleted.return_value = True
        broker_mock.return_value = broker

        fake_node = {'ip': '127.0.0.1', 'port': 1337, 'device': '/dev/sda'}
        part = 'deadbeef'
        self.mock_ring.get_nodes.return_value = (part, [fake_node])
        self.crawler.conf['containers'] = [
            {'account': 'foo',
             'container': 'bar'},
            {'account': 'foo',
             'container': 'baz'}]
        self.crawler.logger = mock.Mock()

        self.crawler._submit_containers()
        self.crawler.enumerator_queue.join()
        self.assertEqual([], self.crawler.logger.error.mock_calls)
        self.mock_handler.save_last_row.assert_not_called()

    def test_processed_before_verification(self):
        nodes = [{'ip': '1.2.3.4', 'port': 1234, 'device': '/dev/sda'},
                 {'ip': '127.0.0.1', 'port': 1234, 'device': '/dev/sda'}]
        rows = [{'ROWID': i, 'name': 'obj%d' % i} for i in range(90, 100)]
        handle_rows = [row for row in rows if row['ROWID'] % 2]
        verify_rows = [row for row in rows if not row['ROWID'] % 2]

        real_container_job = crawler.ContainerJob()
        mock_job = mock.Mock(wraps=real_container_job)

        with mock.patch(
                'container_crawler.crawler.ContainerJob') as mock_job_class:
            self._setup_mocked_crawler()
            finished_processing = [False]
            self.mock_ring.get_nodes.return_value = ('deadbeef', nodes)
            self.mock_broker.get_items_since.return_value = rows

            assertion_errors = []

            def _verify_wait_all():
                real_container_job.wait_all()

                if not finished_processing[0]:
                    expected = [mock.call(row, self.mock_ic)
                                for row in handle_rows]
                    finished_processing[0] = True
                else:
                    expected = [mock.call(row, self.mock_ic)
                                for row in verify_rows]
                try:
                    self.assertEqual(
                        expected,
                        self.mock_handler.handle.mock_calls)
                except AssertionError as e:
                    assertion_errors.append(e)
                finally:
                    self.mock_handler.handle.reset_mock()

            mock_job.wait_all.side_effect = _verify_wait_all
            mock_job_class.return_value = mock_job

            self.mock_ic.iter_containers.return_value = []
            with self._patch_broker():
                self.crawler.run_once()

            if assertion_errors:
                for err in assertion_errors:
                    print err
                raise assertion_errors[0]

    def test_verification_slack_skips_new_objects(self):
        # Should not verify any of the recent objects.
        old_rows = [{'ROWID': 200 + i,
                     'created_at': Timestamp(time.time() - 7200).short,
                     'name': 'obj%d' % i} for i in range(10)]
        new_rows = [{'ROWID': 300 + i,
                     'created_at': Timestamp(time.time()).short,
                     'name': 'obj%d' % (i + 10)} for i in range(10)]
        self.mock_broker.get_items_since.side_effect = (
            [], old_rows + new_rows)

        logger = mock.Mock()
        self.crawler.logger = logger

        self.crawler._verification_slack = 3600
        with self._patch_broker():
            self.crawler.run_once()
        self.assertEqual(
            10, len(self.mock_handler.handle.mock_calls))
        self.assertEqual(
            [mock.call(row, self.mock_ic) for row in old_rows],
            self.mock_handler.handle.mock_calls)
        self.assertEqual(
            [mock.call('Verifying %s rows since row %d for %s/%s' % (
                       len(old_rows), 21, 'account', 'container')),
             mock.call('Verified %s rows; last row: %d; for %s/%s' % (
                       len(old_rows), old_rows[-1]['ROWID'],
                       'account', 'container'))],
            logger.info.mock_calls)

        logger.error.assert_not_called()

    @mock.patch('container_crawler.crawler.is_local_device')
    @mock.patch('glob.glob')
    def test_process_sharded_container(self, glob_mock, mock_local_dev):
        sharded_containers = [
            'foo-etag-ts-1', 'foo-etag-ts-2', 'foo-etag-ts-3']
        metadata = {'x-backend-sharding-state': 'sharded'}
        self.mock_ic.get_container_metadata.return_value = metadata
        self.mock_ic.iter_containers.return_value = [
            {'name': container} for container in sharded_containers]
        glob_mock.return_value = sharded_containers
        os.makedirs('%s/.shards_acc' % self.conf['status_dir'])
        mock_local_dev.return_value = False
        container_setting = {
            'account': 'acc',
            'container': 'foo'}
        self.crawler._process_container(container_setting)

        self.assertEqual(3, self.crawler.enumerator_queue.unfinished_tasks)
        glob_mock.assert_called_once_with(
            '%s/.shards_acc/foo*' % self.conf['status_dir'])
        os.rmdir('%s/.shards_acc' % self.conf['status_dir'])

    def _prepare_status_dir(self, acc_status_dir, containers):
        # setup status dir with fake status files
        os.makedirs(acc_status_dir)
        for f in containers:
            p = os.path.join(acc_status_dir, f)
            open(p, 'a').close()

    def test_prune_deleted_containers(self):
        acc = 'testacc'
        acc_status_dir = os.path.join(self.conf['status_dir'], acc)
        containers = ['foo', 'bar', 'baz', 'xyz']
        current = ['foo', 'bar']

        self._prepare_status_dir(acc_status_dir, containers)

        self.crawler._prune_deleted_containers(acc, current)

        self.assertEqual(
            current.sort(),
            os.listdir(acc_status_dir).sort())

    def test_prune_deleted_sharded_containers(self):
        acc = '.shards_AUTH_testacc'
        acc_status_dir = os.path.join(self.conf['status_dir'], acc)
        sharded_containers = [
            'foo-etag-ts-1', 'foo-etag-ts-2', 'foo-etag-ts-3',
            'bar-etag-ts-1', 'bar-etag-ts-2']
        current = ['foo-etag-ts-1', 'foo-etag-ts-2']

        self._prepare_status_dir(acc_status_dir, sharded_containers)

        self.crawler._prune_deleted_containers(acc, current, prefix='foo')

        expected = [
            'foo-etag-ts-1', 'foo-etag-ts-2',
            'bar-etag-ts-1', 'bar-etag-ts-2']

        self.assertEqual(
            expected.sort(),
            os.listdir(acc_status_dir).sort())

    def test_list_containers_prefix(self):
        self.mock_ic.iter_containers.return_value = []
        self.crawler.list_containers('acc')
        self.mock_ic.iter_containers.assert_called_once_with('acc', prefix='')
        self.mock_ic.iter_containers.reset_mock()

        self.crawler.list_containers('.shards_AUTH_acc', prefix='foo')
        self.mock_ic.iter_containers.assert_called_once_with(
            '.shards_AUTH_acc', prefix='foo')

    def test_local_db_is_sharded(self):
        # first container is sharded, but second is not
        sharded_containers = [
            'bar-etag-ts-1', 'bar-etag-ts-2', 'bar-etag-ts-3']
        self.mock_ic.iter_containers.return_value = [
            {'name': container} for container in sharded_containers]
        self.crawler.conf['containers'] = [
            {'account': 'foo',
             'container': 'bar'},
            {'account': 'qux',
             'container': 'mos'}
        ]
        self.mock_broker.is_sharded.side_effect = [True, False, False, False,
                                                   False]
        with self._patch_broker():
            self.crawler.run_once()

        expected_calls = [mock.call(container, per_account=False)
                          for container in self.crawler.conf['containers']]

        # add calls to handle sharded containers
        expected_calls += [
            mock.call(
                {'root_account': 'foo',
                 'account': '.shards_foo',
                 'root_container': 'bar',
                 'container': sharded_cont}, per_account=False)
            for sharded_cont in sharded_containers]
        self.assertEqual(expected_calls,
                         self.mock_handler_factory.instance.mock_calls)
