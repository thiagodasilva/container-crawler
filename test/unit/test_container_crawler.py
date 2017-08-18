# -*- coding: UTF-8 -*-

import mock
import container_crawler
import unittest

from container_crawler import RetryError
from container_crawler.base_sync import BaseSync


class TestContainerCrawler(unittest.TestCase):

    @mock.patch('container_crawler.InternalClient')
    @mock.patch('container_crawler.Ring')
    def setUp(self, mock_ring, mock_ic):
        self.mock_ring = mock.Mock()
        mock_ring.return_value = self.mock_ring
        self.mock_handler = mock.Mock()
        self.mock_handler.__name__ = 'MockHandler'

        self.mock_ic = mock.Mock()
        mock_ic.return_value = self.mock_ic

        self.conf = {'devices': '/devices',
                     'items_chunk': 1000,
                     'status_dir': '/var/scratch'}
        self.crawler = container_crawler.ContainerCrawler(
            self.conf, self.mock_handler)

    def test_process_items(self):
        total_rows = 20
        items = [{'ROWID': x} for x in range(0, total_rows)]

        for nodes in range(1, 7):
            for node_id in range(0, nodes):
                mock_handler = mock.Mock()
                self.crawler.handler_class = mock.Mock()
                self.crawler.handler_class.return_value = mock_handler
                handle_calls = filter(lambda x: x % nodes == node_id,
                                      range(0, total_rows))
                verify_calls = filter(lambda x: x not in handle_calls,
                                      range(0, total_rows))

                self.crawler.process_items(mock_handler, items, nodes, node_id)
                expected = [mock.call({'ROWID': x}, self.mock_ic)
                            for x in handle_calls]
                expected += [mock.call({'ROWID': x}, self.mock_ic)
                             for x in verify_calls]
                self.assertEqual(expected,
                                 mock_handler.handle.call_args_list)

    def test_bulk_handling(self):
        self.crawler.bulk = True

        total_rows = 20
        items = [{'ROWID': x} for x in range(0, total_rows)]
        mock_handler = mock.Mock()
        self.crawler.handler_class = mock.Mock()
        mock_handler.handle.return_value = []
        self.crawler.handler_class.return_value = mock_handler
        self.crawler.process_items(mock_handler, items, 1, 0)
        expected = [mock.call([{'ROWID': x} for x in range(0, total_rows)],
                    self.mock_ic)]
        self.assertEqual(expected, mock_handler.handle.call_args_list)

    def test_bulk_errors(self):
        self.mock_ring.get_nodes.return_value = ['part', []]
        self.crawler.bulk = True

        mock_handler = mock.Mock()
        mock_handler.handle.side_effect = RuntimeError('error')

        with self.assertRaises(RuntimeError):
            self.crawler.process_items(mock_handler, [], 1, 0)

    def test_failed_handler_class_constructor(self):
        self.mock_ring.get_nodes.return_value = ['part', []]

        self.crawler.handler_class = mock.Mock()
        self.crawler.handler_class.__name__ = 'MockHandler'
        self.crawler.handler_class.side_effect = RuntimeError('oops')

        self.crawler.logger = mock.Mock()

        settings = {'account': 'AUTH_account',
                    'container': 'container'}
        self.crawler.call_handle_container(settings)
        self.crawler.handler_class.assert_called_once_with(
            '/var/scratch', settings, per_account=False)
        self.crawler.logger.assert_has_calls([
            mock.call.error('Failed to process AUTH_account/container with '
                            'MockHandler'),
            mock.call.error(mock.ANY)
        ])

    def test_process_items_errors(self):
        rows = 10
        items = [{'ROWID': x, 'name': str(x)} for x in range(0, rows)]

        for node_id in (0, 1):
            mock_handler = mock.Mock()
            mock_handler.handle.side_effect = RuntimeError('oops')

            with self.assertRaises(RuntimeError):
                self.crawler.process_items(mock_handler, items, 2, node_id)

            handle_calls = filter(lambda x: x % 2 == node_id,
                                  range(0, rows))
            expected = [mock.call({'ROWID': row_id, 'name': str(row_id)},
                                  self.mock_ic) for row_id in handle_calls]
            self.assertEqual(expected,
                             mock_handler.handle.call_args_list)

    def test_verify_items_errors(self):
        rows = 10
        items = [{'ROWID': x, 'name': str(x)} for x in range(0, rows)]

        for node_id in (0, 1):
            mock_handler = mock.Mock()
            mock_handler.handle.side_effect = RuntimeError('oops')

            # only fail the verify calls
            def fail_verify(row, client):
                if row['ROWID'] % 2 != node_id:
                    raise RuntimeError('oops')
                return

            mock_handler.handle.side_effect = fail_verify

            with self.assertRaises(RuntimeError):
                self.crawler.process_items(mock_handler, items, 2, node_id)

            handle_calls = filter(lambda x: x % 2 == node_id, range(0, rows))
            verify_calls = filter(lambda x: x % 2 != node_id, range(0, rows))
            expected = [mock.call({'ROWID': row_id, 'name': str(row_id)},
                                  self.mock_ic) for row_id in handle_calls]
            expected += [mock.call({'ROWID': row_id, 'name': str(row_id)},
                                   self.mock_ic) for row_id in verify_calls]
            self.assertEqual(expected,
                             mock_handler.handle.call_args_list)

    @mock.patch('container_crawler.traceback.format_exc')
    def test_process_items_errors_unicode(self, tb_mock):
        row = {'ROWID': 42, 'name': 'monkey-\xf0\x9f\x90\xb5'}
        error = RuntimeError('fail')
        mock_handler = mock.Mock()
        mock_handler.handle.side_effect = error
        self.crawler.logger = mock.Mock()
        tb_mock.return_value = 'traceback'

        with self.assertRaises(RuntimeError):
            self.crawler.submit_items(mock_handler, [row])
        self.crawler.logger.error.assert_called_once_with(
            "Failed to handle row %d (%s): 'traceback'" % (
                row['ROWID'], row['name'].decode('utf-8')))

    @mock.patch('container_crawler.traceback.format_exc')
    def test_worker_handles_all_exceptions(self, tb_mock):
        mock_handler = mock.Mock()
        mock_handler.handle.side_effect = BaseException('base error')
        tb_mock.return_value = 'traceback'
        self.crawler.logger = mock.Mock()

        row = {'name': 'failed', 'deleted': False, 'ROWID': 1}

        with self.assertRaises(RuntimeError):
            self.crawler.submit_items(mock_handler, [row])
        self.crawler.logger.error.assert_called_once_with(
            "Failed to handle row %d (%s): 'traceback'" % (
                row['ROWID'], row['name'].decode('utf-8')))

    @mock.patch('container_crawler.traceback.format_exc')
    def test_unicode_container_account_failure(self, tb_mock):
        container = {
            'account': 'account-\xf0\x9f\x90\xb5',
            'container': 'container-\xf0\x9f\x90\xb5'
        }
        tb_mock.return_value = 'traceback'
        self.crawler.conf['containers'] = [container]
        self.crawler.handle_container = mock.Mock()
        self.crawler.handle_container.side_effect = BaseException('base error')
        self.crawler.logger = mock.Mock()

        self.crawler.run_once()
        self.crawler.logger.error.asser_has_calls(
            mock.call('Failed to process %s/%s with %s' % (
                container['account'].decode('utf-8'),
                container['container'].decode('utf-8'),
                self.crawler.handler_class.__name__)),
            mock.call('traceback'))

    @mock.patch('container_crawler.time')
    def test_exit_if_no_containers(self, time_mock):
        time_mock.sleep.side_effect = RuntimeError('Should not sleep')
        self.crawler.run_always()

    @mock.patch('container_crawler.traceback.format_exc')
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

        self.crawler.handle_container = mock.Mock()
        self.crawler.handle_container.side_effect = RuntimeError('oops')
        self.crawler.run_once()

        expected_handler_calls = [mock.call(
            self.conf['status_dir'], containers[0], per_account=False)]
        self.assertEqual(expected_handler_calls,
                         self.mock_handler.call_args_list)
        expected_logger_calls = [
            mock.call("Failed to process foo/bar with %s" %
                      (self.crawler.handler_class.__name__)),
            mock.call('traceback'),
            mock.call("Container name not specified in settings -- continue")
        ]
        self.assertEqual(expected_logger_calls,
                         self.crawler.logger.error.call_args_list)

    def test_processes_every_container(self):
        self.crawler.handle_container = mock.Mock()
        self.crawler.conf['containers'] = [
            {'account': 'foo',
             'container': 'foo'},
            {'account': 'bar',
             'container': 'bar'}
        ]

        self.crawler.run_once()
        expected_calls = [mock.call(self.conf['status_dir'],
                                    container, per_account=False)
                          for container in self.crawler.conf['containers']]
        self.assertEquals(expected_calls,
                          self.mock_handler.call_args_list)

    @mock.patch('container_crawler.InternalClient')
    @mock.patch('container_crawler.Ring')
    @mock.patch('container_crawler.os')
    def test_internal_client_path(self, os_mock, ring_mock, ic_mock):
        os_mock.path.exists.return_value = True
        os_mock.path.join.side_effect = lambda *x: '/'.join(x)

        container_crawler.ContainerCrawler(self.conf, self.mock_handler)

        ic_mock.assert_has_calls([
            mock.call('/etc/swift/internal-client.conf',
                      'ContainerCrawler', 3)] * 10)

    @mock.patch('container_crawler.ConfigString')
    @mock.patch('container_crawler.InternalClient')
    @mock.patch('container_crawler.Ring')
    @mock.patch('container_crawler.os')
    def test_internal_client_path_not_found(
            self, os_mock, ring_mock, ic_mock, conf_mock):
        os_mock.path.exists.return_value = False
        os_mock.path.join.side_effect = lambda *x: '/'.join(x)
        conf_string = mock.Mock()
        conf_mock.return_value = conf_string

        container_crawler.ContainerCrawler(self.conf, self.mock_handler)

        os_mock.path.exists.assert_called_once_with(
            '/etc/swift/internal-client.conf')
        conf_mock.assert_called_once_with(
            container_crawler.ContainerCrawler.INTERNAL_CLIENT_CONFIG)
        ic_mock.assert_has_calls([
            mock.call(conf_string, 'ContainerCrawler', 3)] * 10)

    @mock.patch('os.path.exists')
    @mock.patch('container_crawler.ContainerBroker')
    @mock.patch('os.listdir')
    def test_handles_every_container_in_account(self, ls_mock, broker_mock,
                                                exists_mock):
        account = 'foo'
        self.crawler.conf['containers'] = [
            {'account': account,
             'container': '/*'}
        ]
        test_containers = ['foo', 'bar', 'baz']

        self.mock_ic.iter_containers.return_value = [
            {'name': container} for container in test_containers]
        ls_mock.return_value = test_containers
        broker_mock.return_value.get_items_since.return_value = []
        broker_mock.return_value.get_info.return_value = {'id': 12345}

        class FakeHandler(BaseSync):
            def handle(self, row):
                pass

            def get_last_row(self, db_id):
                return 42

            def save_last_row(self, row_id, db_id):
                pass

        self.mock_handler.side_effect = FakeHandler
        node = {'ip': '127.0.0.1', 'port': '8888', 'device': '/dev/foobar'}
        self.crawler.container_ring.get_nodes.return_value = (
            'deadbeef', [node])

        exists_mock.return_value = True

        self.crawler.run_once()

        self.mock_ic.iter_containers.assert_called_once_with(account)
        expected = [
            (mock.call(mock.ANY, account=account, container=container),
             mock.call().get_info(),
             mock.call().get_items_since(42, 1000))
            for container in test_containers]
        broker_mock.assert_has_calls(
            reduce(lambda x, y: list(x) + list(y), expected))
        ls_mock.assert_called_once_with(
            '%s/%s' % (self.conf['status_dir'], account))
        expected_handler_calls = [
            mock.call(self.conf['status_dir'],
                      {'account': account, 'container': container},
                      per_account=True)
            for container in test_containers]
        self.mock_handler.assert_has_calls(expected_handler_calls)

    @mock.patch('os.path.exists')
    @mock.patch('os.unlink')
    @mock.patch('os.listdir')
    def test_removes_missing_directories(self, ls_mock, unlink_mock,
                                         exists_mock):
        account = 'foo'
        self.crawler.conf['containers'] = [
            {'account': account,
             'container': '/*'}
        ]
        test_containers = ['foo', 'bar', 'baz']

        self.mock_ic.iter_containers.return_value = []
        ls_mock.return_value = test_containers
        exists_mock.return_value = True

        self.crawler.run_once()

        self.mock_ic.iter_containers.assert_called_once_with(account)
        ls_mock.assert_called_once_with(
            '%s/%s' % (self.conf['status_dir'], account))
        unlink_mock.assert_has_calls([
            mock.call('%s/%s/%s' % (self.conf['status_dir'], account, cont))
            for cont in test_containers], any_order=True)

    def test_handle_retry_error(self):
        fake_node = {'ip': '127.0.0.1', 'port': 1337}
        part = 'deadbeef'
        self.mock_ring.get_nodes.return_value = (part, [fake_node])

        broker = mock.Mock()
        broker.get_info.return_value = {'id': 12345}
        rows = [{'name': 'foo'}]
        broker.get_items_since.return_value = rows
        self.crawler.get_broker = mock.Mock()
        self.crawler.get_broker.return_value = broker

        self.crawler.process_items = mock.Mock()
        self.crawler.process_items.side_effect = RetryError

        settings = {
            'account': 'foo',
            'container': 'bar'
        }

        handler_instance = mock.Mock()
        handler_instance.get_last_row.return_value = 1337
        self.mock_handler.return_value = handler_instance

        self.crawler.call_handle_container(settings)

        handler_instance.get_last_row.assert_called_once_with(12345)
        self.assertEqual([], handler_instance.save_last_row.mock_calls)
        self.crawler.process_items.assert_called_once_with(
            handler_instance, rows, 1, 0)

    def test_handle_unicode_account_container(self):
        settings = {'account': u'ünìçódê', 'container': u'c😁ntainer'}
        self.crawler.logger = mock.Mock()
        self.mock_ring.get_nodes.return_value = (
            '/some/path', [{'port': 555,
                            'ip': '127.0.0.1',
                            'device': '/dev/foo'}])

        broker_class = 'container_crawler.ContainerBroker'

        fake_handler = mock.Mock(spec=BaseSync)
        fake_handler._account = settings['account']
        fake_handler._container = settings['container']
        fake_handler.get_last_row.return_value = 1337
        self.mock_handler.return_value = fake_handler

        with mock.patch('%s.get_info' % broker_class) as info_mock, \
                mock.patch('%s.get_items_since' % broker_class) as items_mock:
            info_mock.return_value = {'id': 'deadbeef'}
            items_mock.return_value = [{'name': 'object', 'ROWID': 42}]

            self.crawler.call_handle_container(settings)

        self.assertEqual(
            [mock.call.info('Processing 1 rows since row 1337 for %s/%s' % (
                    settings['account'], settings['container'])),
             mock.call.info(
                'Processed 1 rows; verified 0 rows; last row: 42')],
            self.crawler.logger.mock_calls)
        self.mock_handler.assert_called_once_with(
            self.conf['status_dir'], settings, per_account=False)
        fake_handler.get_last_row.assert_called_once_with('deadbeef')
        fake_handler.handle.assert_called_once_with(
            {'name': 'object', 'ROWID': 42}, self.mock_ic)
        fake_handler.save_last_row.assert_called_once_with(42, 'deadbeef')

    @mock.patch('container_crawler.InternalClient')
    @mock.patch('container_crawler.Ring')
    def test_bulk_init(self, mock_ring, mock_ic):
        self.mock_ring = mock.Mock()
        mock_ring.return_value = self.mock_ring
        self.mock_handler = mock.Mock()
        self.mock_handler.__name__ = 'MockHandler'

        self.mock_ic = mock.Mock()
        mock_ic.return_value = self.mock_ic

        self.conf = {'devices': '/devices',
                     'items_chunk': 1000,
                     'status_dir': '/var/scratch',
                     'bulk_process': True}
        crawler = container_crawler.ContainerCrawler(
            self.conf, self.mock_handler)
        self.assertEqual(True, crawler.bulk)
        self.assertEqual(1, crawler._swift_pool.min_size)
        self.assertEqual(1, crawler._swift_pool.max_size)
        self.assertEqual(1, crawler._swift_pool.current_size)

    @mock.patch('container_crawler.InternalClient')
    @mock.patch('container_crawler.Ring')
    def test_no_bulk_init(self, mock_ring, mock_ic):
        self.mock_ring = mock.Mock()
        mock_ring.return_value = self.mock_ring
        self.mock_handler = mock.Mock()
        self.mock_handler.__name__ = 'MockHandler'

        self.mock_ic = mock.Mock()
        mock_ic.return_value = self.mock_ic

        self.conf = {'devices': '/devices',
                     'items_chunk': 1000,
                     'status_dir': '/var/scratch',
                     'bulk_process': False,
                     'workers': 50}
        crawler = container_crawler.ContainerCrawler(
            self.conf, self.mock_handler)
        self.assertEqual(False, crawler.bulk)
        self.assertEqual(50, crawler._swift_pool.min_size)
        self.assertEqual(50, crawler._swift_pool.max_size)
        self.assertEqual(50, crawler._swift_pool.current_size)
        self.assertEqual(50, crawler.pool.size)
        self.assertEqual(100, crawler.work_queue.maxsize)
