import mock
import container_crawler
import unittest


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
        self.crawler.handler_class.side_effect = RuntimeError('oops')

        settings = {'account': 'AUTH_account',
                    'container': 'container'}
        with self.assertRaises(RuntimeError):
            self.crawler.handle_container(settings)

        self.crawler.handler_class.assert_called_once_with(
            '/var/scratch', settings)

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

        expected_handle_calls = [mock.call(conf) for conf in containers]
        self.assertEqual(expected_handle_calls,
                         self.crawler.handle_container.call_args_list)
        expected_logger_calls = [
            mock.call("Failed to process foo/bar with %s" %
                      (self.crawler.handler_class.__name__)),
            mock.call('traceback'),
            mock.call("Failed to process foo/N/A with %s" %
                      (self.crawler.handler_class.__name__)),
            mock.call('traceback')
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
        expected_calls = [mock.call(container)
                          for container in self.crawler.conf['containers']]
        self.assertEquals(expected_calls,
                          self.crawler.handle_container.call_args_list)
