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
    def _setup_mocked_crawler(self, mock_ring, mock_ic):
        self.mock_ring = mock.Mock(
            get_nodes=mock.Mock(
                return_value=('deadbeef',
                              [{'ip': '127.0.0.1',
                                'port': 1234}])))
        mock_ring.return_value = self.mock_ring
        self.mock_handler = mock.Mock()
        self.mock_handler.__name__ = 'MockHandler'
        self.mock_handler.return_value.get_last_row.return_value = 42

        self.mock_ic = mock.Mock()
        mock_ic.return_value = self.mock_ic
        self.mock_broker = mock.Mock(
            get_info=mock.Mock(return_value={'id': 'hash'}),
            get_items_since=mock.Mock(return_value=[]),
            is_deleted=mock.Mock(return_value=False))

        self.crawler = container_crawler.ContainerCrawler(
            self.conf, self.mock_handler)
        self.crawler.get_broker = mock.Mock(
            return_value=self.mock_broker)

    def setUp(self):
        self.conf = {
            'devices': '/devices',
            'items_chunk': 1000,
            'status_dir': '/var/scratch',
            'containers': [{'account': 'account',
                            'container': 'container'}]}
        self._setup_mocked_crawler()

    def test_enumerator_worker(self):
        total_rows = 100
        items = [{'ROWID': x} for x in range(0, total_rows)]

        self.mock_broker.get_items_since.return_value = items

        for nodes in range(1, 7):
            for node_id in range(0, nodes):
                all_nodes = [{'ip': '1.2.3.4', 'port': 1234}
                             for _ in range(nodes)]
                all_nodes[node_id]['ip'] = '127.0.0.1'

                self.mock_ring.get_nodes.return_value = ('deadbeef', all_nodes)
                self.mock_handler.return_value.handle.reset_mock()

                handle_calls = filter(lambda x: x % nodes == node_id,
                                      range(0, total_rows))
                verify_calls = filter(lambda x: x not in handle_calls,
                                      range(0, total_rows))

                self.crawler.run_once()

                expected = [mock.call({'ROWID': x}, self.mock_ic)
                            for x in handle_calls]
                expected += [mock.call({'ROWID': x}, self.mock_ic)
                             for x in verify_calls]
                self.assertEqual(
                    expected,
                    self.mock_handler.return_value.handle.call_args_list)

    def test_bulk_handling(self):
        self.conf['bulk_process'] = 'true'
        self._setup_mocked_crawler()
        self.assertEqual(self.crawler.bulk, True)

        total_rows = 20
        items = [{'ROWID': x} for x in range(0, total_rows)]

        self.mock_broker.get_items_since.return_value = items

        expected = [mock.call([{'ROWID': x} for x in range(0, total_rows)],
                    self.mock_ic)]

        self.crawler.run_once()

        self.assertEqual(
            expected, self.mock_handler.return_value.handle.call_args_list)

    def test_bulk_errors(self):
        self.conf['bulk_process'] = 'true'
        self._setup_mocked_crawler()
        self.assertEqual(self.crawler.bulk, True)

        self.mock_handler.return_value.handle.side_effect =\
            RuntimeError('error')
        self.mock_handler.return_value.get_last_row.return_value = 42
        self.mock_broker.get_items_since.return_value = [
            {'ROWID': 1, 'name': 'foo'}]
        self.crawler.logger = mock.Mock()

        self.crawler.run_once()

        self.crawler.logger.error.assert_has_calls([
            mock.call('Failed to process %s/%s with %s' % (
                      self.conf['containers'][0]['account'],
                      self.conf['containers'][0]['container'],
                      self.mock_handler.__name__))])

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

    def test_enumerator_handling_rows_errors(self):
        rows = 10
        items = [{'ROWID': x, 'name': str(x)} for x in range(0, rows)]
        self.mock_broker.get_items_since.return_value = items

        for node_id in (0, 1):
            all_nodes = [{'ip': '1.2.3.4', 'port': 1234}
                         for _ in range(2)]
            all_nodes[node_id]['ip'] = '127.0.0.1'

            self.mock_ring.get_nodes.return_value = ('deadbeef', all_nodes)

            self.mock_handler.return_value.handle.reset_mock()
            self.mock_handler.return_value.handle.side_effect =\
                RuntimeError('oops')

            self.crawler.run_once()

            handle_rows = filter(lambda x: x % 2 == node_id, range(0, rows))
            verify_rows = filter(lambda x: x % 2 != node_id, range(0, rows))
            expected = [mock.call({'ROWID': row_id, 'name': str(row_id)},
                                  self.mock_ic)
                        for row_id in handle_rows + verify_rows]
            self.assertEqual(
                expected,
                self.mock_handler.return_value.handle.call_args_list)

    def test_enumerator_verify_items_errors(self):
        rows = 10
        items = [{'ROWID': x, 'name': str(x)} for x in range(0, rows)]
        self.mock_broker.get_items_since.return_value = items

        for node_id in (0, 1):
            # only fail the verify calls
            def fail_verify(row, client):
                if row['ROWID'] % 2 != node_id:
                    raise RuntimeError('oops')
                return

            all_nodes = [{'ip': '1.2.3.4', 'port': 1234}
                         for _ in range(2)]
            all_nodes[node_id]['ip'] = '127.0.0.1'

            self.mock_ring.get_nodes.return_value = ('deadbeef', all_nodes)
            self.mock_handler.return_value.handle.reset_mock()
            self.mock_handler.return_value.handle.side_effect = fail_verify

            self.crawler.run_once()

            handle_calls = filter(lambda x: x % 2 == node_id, range(0, rows))
            verify_calls = filter(lambda x: x % 2 != node_id, range(0, rows))
            expected = [mock.call({'ROWID': row_id, 'name': str(row_id)},
                                  self.mock_ic) for row_id in handle_calls]
            expected += [mock.call({'ROWID': row_id, 'name': str(row_id)},
                                   self.mock_ic) for row_id in verify_calls]
            self.assertEqual(
                expected,
                self.mock_handler.return_value.handle.call_args_list)

    @mock.patch('container_crawler.traceback.format_exc')
    def test_unicode_object_failure(self, mock_tb):
        row = {'ROWID': 42, 'name': 'monkey-\xf0\x9f\x90\xb5'}
        error = RuntimeError('fail')
        self.mock_handler.return_value.handle.side_effect = error
        self.mock_broker.get_items_since.return_value = [row]
        self.crawler.logger = mock.Mock()
        mock_tb.return_value = 'traceback'

        self.crawler.run_once()

        self.crawler.logger.error.assert_has_calls(
            [mock.call("Failed to handle row %d (%s): 'traceback'" % (
                       row['ROWID'], row['name'].decode('utf-8')))])

    @mock.patch('container_crawler.traceback.format_exc')
    def test_worker_handles_all_exceptions(self, tb_mock):
        self.mock_handler.return_value.handle.side_effect =\
            BaseException('base error')
        tb_mock.return_value = 'traceback'
        self.crawler.logger = mock.Mock()

        row = {'name': 'failed', 'deleted': False, 'ROWID': 1}
        self.mock_broker.get_items_since.return_value = [row]
        self.crawler.run_once()

        self.crawler.logger.error.assert_has_calls([
            mock.call(
                "Failed to handle row %d (%s): 'traceback'" % (
                    row['ROWID'], row['name'].decode('utf-8')))])

    @mock.patch('container_crawler.traceback.format_exc')
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
        self.crawler.logger.error.asser_has_calls(
            mock.call('Failed to process %s/%s with %s' % (
                container['account'].decode('utf-8'),
                container['container'].decode('utf-8'),
                self.crawler.handler_class.__name__)),
            mock.call('traceback'))

    @mock.patch('container_crawler.time')
    def test_exit_if_no_containers(self, time_mock):
        self.crawler.conf['containers'] = []
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

        self.crawler.find_new_rows = mock.Mock(
            side_effect=RuntimeError('oops'))
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
        self.crawler.find_new_rows = mock.Mock(
            return_value=[[], [], None, None, None])
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
    @mock.patch('os.listdir')
    def test_handles_every_container_in_account(self, ls_mock, exists_mock):
        account = 'foo'
        self.crawler.conf['containers'] = [
            {'account': account,
             'container': '/*'}
        ]
        test_containers = ['foo', 'bar', 'baz']

        self.mock_ic.iter_containers.return_value = [
            {'name': container} for container in test_containers]
        ls_mock.return_value = test_containers
        self.mock_broker.get_items_since.return_value = []

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
            (mock.call.is_deleted(),
             mock.call.get_info(),
             mock.call.get_items_since(42, 1000))
            for _ in test_containers]
        self.mock_broker.assert_has_calls(
            reduce(lambda x, y: list(x) + list(y), expected))
        self.crawler.get_broker.assert_has_calls(
            [mock.call(account, container, mock.ANY, mock.ANY)
             for container in test_containers])
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
        rows = [{'name': 'foo', 'ROWID': 1}]
        self.mock_broker.get_items_since.return_value = rows

        self.crawler.submit_items = mock.Mock()
        self.crawler.submit_items.side_effect = RetryError

        settings = {
            'account': 'foo',
            'container': 'bar'
        }

        self.crawler.conf = {'containers': [settings]}
        self.crawler.run_once()

        self.mock_handler.return_value.get_last_row.assert_called_once_with(
            'hash')
        self.assertEqual(
            [], self.mock_handler.return_value.save_last_row.mock_calls)
        self.crawler.submit_items.assert_called_once_with(
            self.mock_handler.return_value, rows, mock.ANY)

    def test_handle_unicode_account_container(self):
        account = u'ünìçódê'
        container = u'c😁ntainer'
        self.conf['containers'] = [
            {'account': account,
             'container': container}]
        self.crawler.logger = mock.Mock()
        self.mock_broker.get_items_since.return_value = [
            {'name': 'object', 'ROWID': 1337}]
        self.mock_handler.return_value._account = account
        self.mock_handler.return_value._container = container

        self.crawler.run_once()

        self.assertEqual(
            [mock.call.info('Processing 1 rows since row 42 for %s/%s' % (
                account, container)),
             mock.call.info(
                'Processed 1 rows; verified 0 rows; last row: 1337')],
            self.crawler.logger.mock_calls)
        self.mock_handler.assert_called_once_with(
            self.conf['status_dir'], self.conf['containers'][0],
            per_account=False)
        self.mock_handler.return_value.get_last_row.assert_called_once_with(
            'hash')
        self.mock_handler.return_value.handle.assert_called_once_with(
            {'name': 'object', 'ROWID': 1337}, self.mock_ic)
        self.mock_handler.return_value.save_last_row.assert_called_once_with(
            1337, 'hash')

    def test_verifying_rows(self):
        self.mock_ring.get_nodes.return_value = (
            'deadbeef',
            [{'ip': '1.2.3.4',
              'port': 1234,
              'device': '/dev/sda'},
             {'ip': '127.0.0.1',
              'port': 1234,
              'device': '/dev/sda'}])
        rows = [{'ROWID': i, 'name': 'obj%d' % i} for i in range(80, 100, 2)]
        self.mock_broker.get_items_since.return_value = rows
        self.crawler.logger = mock.Mock()
        self.mock_handler.return_value._account = 'account'
        self.mock_handler.return_value._container = 'container'

        self.crawler.run_once()

        self.assertEqual(
            [mock.call.info('Verifying %d rows since row %d for %s/%s' % (
                len(rows), 42, 'account', 'container')),
             mock.call.info('Processed 0 rows; verified %d rows; last row: %d'
                            % (len(rows), rows[-1]['ROWID']))],
            self.crawler.logger.mock_calls)
        self.assertEqual([mock.call(row, self.mock_ic) for row in rows],
                         self.mock_handler.return_value.handle.mock_calls)

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

        self.mock_handler.return_value.handle.side_effect =\
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

    def test_processed_before_verification(self):
        nodes = [{'ip': '1.2.3.4', 'port': 1234},
                 {'ip': '127.0.0.1', 'port': 1234}]
        rows = [{'ROWID': i, 'name': 'obj%d' % i} for i in range(90, 100)]
        handle_rows = [row for row in rows if row['ROWID'] % 2]
        verify_rows = [row for row in rows if not row['ROWID'] % 2]

        real_container_job = container_crawler.ContainerJob()
        mock_job = mock.Mock(wraps=real_container_job)

        with mock.patch('container_crawler.ContainerJob') as mock_job_class:
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
                        self.mock_handler.return_value.handle.mock_calls)
                except AssertionError as e:
                    assertion_errors.append(e)
                finally:
                    self.mock_handler.return_value.handle.reset_mock()

            mock_job.wait_all.side_effect = _verify_wait_all
            mock_job_class.return_value = mock_job

            self.crawler.run_once()

            if assertion_errors:
                for err in assertion_errors:
                    print err
                raise assertion_errors[0]
