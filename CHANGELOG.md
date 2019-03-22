## 0.1.4 (2019-03-22)

Improvements:

- Added log messages when container DBs do not exist.
- Expanded the API to allow clients to get container information (as well as
  container metadata). The `handle_container_metadata()` method changed to
  `handle_container_info()`.
- Uses the object name to shard the work between "primary"/"verification", as
  opposed to row IDs. This leads to a consistent partitioning regardless of row
  ID inconsistencies across databases.

## 0.1.3 (2019-02-08)

Improvements:

- Only attempts to sync container metadata for root sharded containers.

## 0.1.2 (2019-01-17)

Improvements:

- Added support for crawling sharded Swift container. Container sharding is
  an OpenStack Swift feature that allows a single container database to be
  distributed across many smaller databases. This improvement does not require
  any changes in downstream consumers.

## 0.1.1 (2018-12-14)

Improvements:

- Add a `swift_bind_ip` option to allow specifying which IP should be checked
  during the check of which node is affiliated with 1space. Default is 0.0.0.0.
- Expand the ContainerCrawler API with a `handle_container_metadata` method.
  This allows the library users to act on container's metadata, as well as the
  rows within it.

## 0.1.0 (2018-11-13)

Improvements:

- Changed the code structure, such that exceptions are placed into a separate
  module and can be imported without importing the main Crawler code. The main
  library class has also been renamed to Crawler and placed in the `crawler`
  module.

## 0.0.18 (2018-10-17)

Bug fixes:

- Fixed non-ASCII character handling in container names when the default
  locale is not set (as oppposed to being UTF-8).

## 0.0.17 (2018-10-12)

Improvements:

- Changes the interface to accept a factory object rather than a class to
  instantiate the handler for each row (or batch of rows).

Bug fixes:

- Fixes handling of containers with non-ASCII characters when configured to
  process all containers in an account.

## 0.0.16 (2018-10-07)

Improvements:

- Added a `verification_slack` parameter that can defer verification by a
  specific number of minutes.

Bug fixes:

- Fixed an issue where verified and primary objects were processed at the
  same time.

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

- Treat containers named "/\*" as a way to index all containers under a
  specified account. The leading slash is chosen, as Swift does not allow
  "/" in the container names.

Improvements:

- Use a pool of Swift InternalClient objects (as opposed to instantiating
  one on every crawl)

## 0.0.2 (2017-02-28)

Bug fixes:

- Fix the handling of unicode characters in object names when logging errors
- handler tasks no longer fail to complete if an exception is raised

Improvements:

- Logs the object name on failure (as opposed to only the row ID)
