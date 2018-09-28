# -*- coding: UTF-8 -*-

import eventlet
import mock
import container_crawler
import unittest

from container_crawler import RetryError
from container_crawler.base_sync import BaseSync


class TestContainerCrawler(unittest.TestCase):

    @mock.patch('container_crawler.utils.InternalClient')
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
        self.container_job = container_crawler.ContainerJob()

    def test_process_items(self):
        total_rows = 100
        items = [{'ROWID': x} for x in range(0, total_rows)]

        for nodes in range(1, 7):
            for node_id in range(0, nodes):
                self.container_job._reset()
                mock_handler = mock.Mock()
                self.crawler.handler_class = mock.Mock()
                self.crawler.handler_class.return_value = mock_handler
                handle_calls = filter(lambda x: x % nodes == node_id,
                                      range(0, total_rows))
                verify_calls = filter(lambda x: x not in handle_calls,
                                      range(0, total_rows))

                self.crawler.process_items(
                    mock_handler, items, nodes, node_id, self.container_job)
                self.container_job.wait_all()

                expected = [mock.call({'ROWID': x}, self.mock_ic)
                            for x in handle_calls]
                expected += [mock.call({'ROWID': x}, self.mock_ic)
                             for x in verify_calls]
                self.assertEqual(expected,
                                 mock_handler.handle.call_args_list)
                self.assertEqual(0, self.container_job._outstanding)

    def test_bulk_handling(self):
        self.conf['bulk_process'] = 'true'
        with mock.patch('container_crawler.utils.InternalClient',
                        return_value=self.mock_ic), \
                mock.patch('container_crawler.Ring',
                           return_value=self.mock_ring):
            self.crawler = container_crawler.ContainerCrawler(
                self.conf, self.mock_handler)
        self.assertEqual(self.crawler.bulk, True)

        total_rows = 20
        items = [{'ROWID': x} for x in range(0, total_rows)]
        mock_handler = mock.Mock()
        self.crawler.handler_class = mock.Mock()
        mock_handler.handle.return_value = []
        self.crawler.handler_class.return_value = mock_handler
        self.crawler.process_items(
            mock_handler, items, 1, 0, self.container_job)
        expected = [mock.call([{'ROWID': x} for x in range(0, total_rows)],
                    self.mock_ic)]
        self.assertEqual(expected, mock_handler.handle.call_args_list)
        self.assertEqual(0, self.container_job._outstanding)

    def test_bulk_errors(self):
        self.mock_ring.get_nodes.return_value = ['part', []]
        self.conf['bulk_process'] = 'true'
        with mock.patch('container_crawler.utils.InternalClient',
                        return_value=self.mock_ic), \
                mock.patch('container_crawler.Ring',
                           return_value=self.mock_ring):
            self.crawler = container_crawler.ContainerCrawler(
                self.conf, self.mock_handler)
        self.assertEqual(self.crawler.bulk, True)

        mock_handler = mock.Mock()
        mock_handler.handle.side_effect = RuntimeError('error')

        with self.assertRaises(RuntimeError):
            self.crawler.process_items(
                mock_handler,
                [{'ROWID': 1, 'name': 'foo'}],
                1, 0, self.container_job)

    def test_failed_handler_class_constructor(self):
        self.mock_ring.get_nodes.return_value = ['part', []]

        self.crawler.handler_class = mock.Mock()
        self.crawler.handler_class.__name__ = 'MockHandler'
        self.crawler.handler_class.side_effect = RuntimeError('oops')

        self.crawler.logger = mock.Mock()

        settings = {'account': 'AUTH_account',
                    'container': 'container'}
        self.crawler.conf['containers'] = [settings]
        self.crawler.run_once()
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

            self.crawler.process_items(
                mock_handler, items, 2, node_id, self.container_job)
            self.container_job.wait_all()

            handle_rows = filter(lambda x: x % 2 == node_id, range(0, rows))
            verify_rows = filter(lambda x: x % 2 != node_id, range(0, rows))
            expected = [mock.call({'ROWID': row_id, 'name': str(row_id)},
                                  self.mock_ic)
                        for row_id in handle_rows + verify_rows]
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

            self.crawler.process_items(
                mock_handler, items, 2, node_id, self.container_job)
            self.container_job.wait_all()

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

        self.crawler.process_items(
            mock_handler, [row], 1, 0, self.container_job)
        self.container_job.wait_all()

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

        self.crawler.process_items(
            mock_handler, [row], 1, 0, self.container_job)
        self.container_job.wait_all()
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
            mock.call("Container name not specified in settings -- continue"),
            mock.call("Failed to process foo/bar with %s" %
                      (self.crawler.handler_class.__name__)),
            mock.call('traceback'),
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
        broker_mock.return_value.is_deleted.return_value = False
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
             mock.call().is_deleted(),
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
        broker.is_deleted.return_value = False
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

        self.crawler.conf = {'containers': [settings]}
        self.crawler.run_once()

        handler_instance.get_last_row.assert_called_once_with(12345)
        self.assertEqual([], handler_instance.save_last_row.mock_calls)
        self.crawler.process_items.assert_called_once_with(
            handler_instance, rows, 1, 0, mock.ANY)

    def test_handle_unicode_account_container(self):
        settings = {'account': u'ünìçódê', 'container': u'c😁ntainer'}
        self.crawler.logger = mock.Mock()
        self.mock_ring.get_nodes.return_value = (
            '/some/path', [{'port': 555,
                            'ip': '127.0.0.1',
                            'device': '/dev/foo'}])

        fake_handler = mock.Mock(spec=BaseSync)
        fake_handler._account = settings['account']
        fake_handler._container = settings['container']
        fake_handler.get_last_row.return_value = 1337
        self.mock_handler.return_value = fake_handler
        self.crawler.conf = {'containers': [settings]}

        fake_broker = mock.Mock()
        fake_broker.is_deleted.return_value = False
        fake_broker.get_info.return_value = {'id': 'deadbeef'}
        fake_broker.get_items_since.return_value = [
            {'name': 'object', 'ROWID': 42}]

        with mock.patch('container_crawler.ContainerBroker',
                        return_value=fake_broker):
            self.crawler.run_once()

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

    @mock.patch('container_crawler.utils.InternalClient')
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
                     'bulk_process': True,
                     'enumerator_workers': 42}
        crawler = container_crawler.ContainerCrawler(
            self.conf, self.mock_handler)
        self.assertEqual(True, crawler.bulk)
        self.assertEqual(1, crawler._swift_pool.min_size)
        self.assertEqual(1, crawler._swift_pool.max_size)
        self.assertEqual(1, crawler._swift_pool.current_size)
        self.assertEqual(42, crawler.enumerator_pool.size)
        self.assertEqual(42, crawler.enumerator_queue.maxsize)

    @mock.patch('container_crawler.utils.InternalClient')
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
                     'workers': 50,
                     'enumerator_workers': 84}
        crawler = container_crawler.ContainerCrawler(
            self.conf, self.mock_handler)
        self.assertEqual(False, crawler.bulk)
        self.assertEqual(50, crawler._swift_pool.min_size)
        self.assertEqual(50, crawler._swift_pool.max_size)
        self.assertEqual(50, crawler._swift_pool.current_size)
        self.assertEqual(50, crawler.worker_pool.size)
        self.assertEqual(100, crawler.work_queue.maxsize)
        self.assertEqual(84, crawler.enumerator_pool.size)
        self.assertEqual(84, crawler.enumerator_queue.maxsize)

    def test_one_handler_per_container(self):
        handler_progress = eventlet.queue.Queue()

        def _handle(*args):
            handler_progress.get()
            return

        mock_handler_instance = mock.Mock()
        mock_handler_instance.handler.side_effect = handler_progress
        self.mock_handler.return_value = mock_handler_instance

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
        self.mock_handler.return_value = mock_handler_instance

        containers = [u'b\u00e1r', u'q\u00fax']
        self.crawler.conf['containers'] = [
            {'account': u'fo\u00f2',
             'container': '/*'}]
        self.crawler.list_containers = mock.Mock(return_value=[
            container.encode('utf-8')
            for container in containers
        ])

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
        self.mock_handler.return_value.side_effect = RuntimeError(
            'Should not be called!')

        self.mock_ring.get_nodes.return_value = ['part', []]
        self.crawler.conf['containers'] = [
            {'account': 'foo',
             'container': 'bar'},
            {'account': 'foo',
             'container': 'baz'}]
        self.crawler.logger = mock.Mock()

        self.crawler._submit_containers()
        self.crawler.enumerator_queue.join()
        self.crawler.logger.error.assert_not_called()
        self.mock_handler.return_value.save_last_row.assert_not_called()

    def test_skip_missing_containers(self):
        # NB: covers both db-file-is-not-on-disk and db-file-is-marked-deleted

        broker = mock.Mock()
        broker.is_deleted.return_value = True
        self.crawler.get_broker = mock.Mock(return_value=broker)

        fake_node = {'ip': '127.0.0.1', 'port': 1337}
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
        self.mock_handler.return_value.save_last_row.assert_not_called()
