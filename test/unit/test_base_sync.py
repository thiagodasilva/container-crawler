import container_crawler.base_sync
import container_crawler.crawler
import mock
import unittest


class TestBaseSync(unittest.TestCase):
    def test_base_sync_constructor(self):
        settings = {
            'account': u'a\u00e7\u00e7ount',
            'container': u'c\u00f3ntainer',
        }

        status_dir = u'/st\u00e1tus/dir'

        instance = container_crawler.base_sync.BaseSync(
            status_dir, settings, True)
        self.assertTrue(instance._per_account)
        self.assertEqual(
            ('%s/%s/%s' % (status_dir, settings['account'],
                           settings['container'])).encode('utf-8'),
            instance._status_file)
        self.assertEqual(
            ('%s/%s' % (status_dir, settings['account'])).encode('utf-8'),
            instance._status_account_dir)

    @mock.patch('container_crawler.crawler.os.listdir')
    @mock.patch('container_crawler.utils.InternalClient')
    @mock.patch('container_crawler.crawler.ContainerBroker')
    @mock.patch('container_crawler.crawler.Ring')
    def test_base_sync_interface(self, mock_ring, mock_broker, mock_ic,
                                 mock_listdir):
        '''Test the NOOP base sync implementation.

        Guards against API changes.
        '''

        class BaseSyncNoop(container_crawler.base_sync.BaseSync):
            def handle(self, rows, swift_client):
                return

            def get_last_processed_row(self, db_id):
                return 0

            def save_last_processed_row(self, row_id, db_id):
                return

            def get_last_verified_row(self, db_id):
                return 0

            def save_last_verified_row(self, row_id, db_id):
                return

        class HandlerFactory(object):
            def instance(self, settings, per_account=False):
                return BaseSyncNoop('/tmp/status', settings, per_account)

        mock_broker.return_value = mock.Mock(
            get_info=mock.Mock(return_value={'id': 'hash'}),
            get_items_since=mock.Mock(return_value=[]),
            is_sharded=mock.Mock(return_value=False),
            is_deleted=mock.Mock(return_value=False),
            is_root_container=mock.Mock(return_value=True),
            metadata={})

        mock_ring.return_value = mock.Mock(
            get_nodes=mock.Mock(
                return_value=('deadbeef',
                              [{'ip': '127.0.0.1',
                                'port': 1234,
                                'device': '/dev/sda'}])))
        conf = {'devices': '/devices',
                'items_chunk': 1000,
                'status_dir': '/tmp/status',
                'containers': [{'account': 'account',
                                'container': 'container'}]}
        mock_listdir.return_value = ['account']

        logger = mock.Mock()
        crawler = container_crawler.crawler.Crawler(
            conf, HandlerFactory(), logger=logger)
        crawler.run_once()

        logger.error.assert_not_called()
