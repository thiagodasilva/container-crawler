## 0.0.3 (2017-05-05)

Features:

    - treat containers named "/\*" as a way to index all containers under a
      specified account. The leading slash is chosen, as Swift does not allow
      "/" in the container names.

Improvements:

    - use a pool of Swift InternalClient objects (as opposed to instantiating
      one on every crawl)

## 0.0.2 (2017-02-28)

Bug fixes:

    - fix the handling of unicode characters in object names when logging errors
    - handler tasks no longer fail to complete if an exception is raised

Improvements:

    - logs the object name on failure (as opposed to only the row ID)
