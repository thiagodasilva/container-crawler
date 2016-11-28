import os


class BaseSync(object):
    """
    Base class for all classes that implement the sync functionality. Sets up
    the swift client with the default configuration, which points to the local
    Swift configuration. Supplies the skeleton methods that must be overwritten
    by all child classes.
    """

    def __init__(self, status_dir, settings):
        self._status_dir = status_dir
        self._account = settings['account']
        self._container = settings['container']
        self._status_file = os.path.join(self._status_dir, self._account,
                                         self._container)
        self._status_account_dir = os.path.join(self._status_dir,
                                                self._account)

    def handle(self, rows, swift_client):
        raise NotImplementedError

    def get_last_row(self, db_id):
        raise NotImplementedError

    def save_last_row(self, row_id, db_id):
        raise NotImplementedError
