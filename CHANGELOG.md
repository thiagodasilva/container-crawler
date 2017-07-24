## 0.0.7.1 (2017-07-24)

Bug fixes:

    - The internal client pool was always set to 1. This resulted in
      serialization of processing.

## 0.0.7 (2017-07-13)

Bug fixes:

    - To properly handle unicode characters in the container names, we should
      decode them when listing using the Swift Internal Client, as they are
      returned as UTF-8 encoded strings.

## 0.0.6 (2017-07-12)

Bug fixes:

    - Support unicode characters in account/containers properly.
    - Added an indicator of the per-account setting being set for a container
      mapping.

## 0.0.5 (2017-06-21)

Bug fixes:

    - Check the directory existence before attempting to remove truncated
      container status files.

## 0.0.4 (2017-05-30)

Features:

    - Added the ability to use the internal-client.conf configuration file. This
      enables consumers to override the default Swift InternalClient
      configuration to include, for example, the encryption and keymaster
      middlewares.
    - Consumers can defer action on rows by raising a RetryError. This may be
      useful if the row should be processed after some amount of time in the
      future.

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
