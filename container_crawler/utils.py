import os.path
from swift.common.internal_client import InternalClient
from swift.common.wsgi import ConfigString


INTERNAL_CLIENT_CONFIG = """
[DEFAULT]
[pipeline:main]
pipeline = catch_errors proxy-logging cache proxy-server

[app:proxy-server]
use = egg:swift#proxy
account_autocreate = True

[filter:cache]
use = egg:swift#memcache

[filter:proxy-logging]
use = egg:swift#proxy_logging

[filter:catch_errors]
use = egg:swift#catch_errors
""".lstrip()


def create_internal_client(conf, swift_dir):
    ic_config = conf.get(
        'internal_client_path',
        os.path.join(swift_dir, 'internal-client.conf'))
    if not os.path.exists(ic_config):
        ic_config = ConfigString(INTERNAL_CLIENT_CONFIG)

    ic_name = conf.get('internal_client_logname', 'ContainerCrawler')
    return InternalClient(ic_config, ic_name, 3)
