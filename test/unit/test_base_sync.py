import container_crawler.base_sync
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
