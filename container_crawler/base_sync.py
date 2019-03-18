import os


class BaseSync(object):
    """
    Base class for all classes that implement the sync functionality. Sets up
    the swift client with the default configuration, which points to the local
    Swift configuration. Supplies the skeleton methods that must be overwritten
    by all child classes.
    """

    def __init__(self, status_dir, settings, per_account=False):
        """Base class that all users of the ContainerCrawler should derive from

        Arguments:
        status_dir -- directory that contains the sync status files
        settings -- sync settings; contains at least the following keys:
            account -- the Swift account that is synced
            container -- the specific container within the account
            Any other keys are provider specific.

        Keyword arguments:
        per_account -- whether the operation is happening on every container in
                       the account.
        """
        self._status_dir = status_dir
        self._account = settings['account']
        self._container = settings['container']
        self._status_file = os.path.join(self._status_dir, self._account,
                                         self._container).encode('utf-8')
        self._status_account_dir = os.path.join(self._status_dir,
                                                self._account).encode('utf-8')
        self._per_account = per_account

    def handle(self, rows, swift_client):
        raise NotImplementedError

    def get_last_processed_row(self, db_id):
        raise NotImplementedError

    def save_last_processed_row(self, row_id, db_id):
        raise NotImplementedError

    def get_last_verified_row(self, db_id):
        raise NotImplementedError

    def save_last_verified_row(self, row_id, db_id):
        raise NotImplementedError

    def handle_container_info(self, broker_info, metadata):
        # Optional to implement
        pass
