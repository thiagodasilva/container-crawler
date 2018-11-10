class RetryError(Exception):
    pass


class SkipContainer(Exception):
    '''
    Raised during initialization when the provided container (for
    whatever reason) *should not* be crawled.
    '''
    pass
