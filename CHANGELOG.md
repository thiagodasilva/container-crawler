## 0.0.15 (2018-09-19)

Bug fixes:

    - Fixed a deadlock introduced in 0.0.14, which blocks progress when there
      are more items to upload than worker threads.

## 0.0.14 (2018-08-27)

Improvements:

    - Add an option to crawl containers in parallel. This allows handling
      configurations and accounts with 1000s of containers. The
      `enumerator_workers` option controls how many containers are handled in
      parallel and defaults to 10.

## 0.0.13 (2018-04-02)

Bug fixes:

    - Configuration files can now explicitly set `bulk_process` to false to turn
      off bulk processing. Previously this was only possible by omitting the
      config value entirely.
    - Running with bulk processing enabled no longer raises AttributeErrors
      all the time.

## 0.0.12 (2018-02-13)

Improvements:

    - Add SkipContainer exception that handlers can raise during initialization.

## 0.0.11 (2018-01-31)

Bug fixes:

    - Check errors after verifying the rows of other nodes, as otherwise a given
      node may never pick up the work left from the other node.

## 0.0.10 (2018-01-30)

Improvements:

    - Reorganize InternalClient helpers in utils.

## 0.0.9 (2017-08-23)

Bug fixes:

    - Fix a regression where bulk processing could no longer instantiate a
      ContainerCrawler object.

## 0.0.8 (2017-08-01)

Improvements:

    - Add logging for each set of processed and verified rows. This is useful
      for ensuring that container crawler continues to make progress.

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
