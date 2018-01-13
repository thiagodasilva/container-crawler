import container_crawler.utils
import mock
import unittest


class TestUtils(unittest.TestCase):
    @mock.patch('container_crawler.utils.InternalClient')
    @mock.patch('container_crawler.utils.os')
    def test_internal_client_path(self, os_mock, ic_mock):
        os_mock.path.exists.return_value = True
        os_mock.path.join.side_effect = lambda *x: '/'.join(x)

        conf = {'internal_client_logname': 'TestClient',
                'internal_client_path': '/etc/swift/internal-client.conf'}

        container_crawler.utils.create_internal_client(conf, '/etc/swift')

        ic_mock.assert_called_once_with(conf['internal_client_path'],
                                        conf['internal_client_logname'], 3)

    @mock.patch('container_crawler.utils.ConfigString')
    @mock.patch('container_crawler.utils.InternalClient')
    @mock.patch('container_crawler.utils.os')
    def test_internal_client_path_not_found(self, os_mock, ic_mock, conf_mock):
        os_mock.path.exists.return_value = False
        os_mock.path.join.side_effect = lambda *x: '/'.join(x)
        conf_string = mock.Mock()
        conf_mock.return_value = conf_string

        conf = {'internal_client_logname': 'TestClient',
                'internal_client_path': '/etc/swift/internal-client.conf'}

        container_crawler.utils.create_internal_client(conf, '/etc/swift')

        os_mock.path.exists.assert_called_once_with(
            conf['internal_client_path'])
        conf_mock.assert_called_once_with(
            container_crawler.utils.INTERNAL_CLIENT_CONFIG)
        ic_mock.assert_called_once_with(
            conf_string, conf['internal_client_logname'], 3)
