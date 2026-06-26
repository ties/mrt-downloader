## v0.0.16

  * Use the OSDF RouteViews mirror as the primary source for RouteViews MRT
    file downloads, with archive.routeviews.org and archive2.routeviews.org as
    fallbacks.
  * Verify downloads retry cleanly when aiohttp reports an incomplete response
    payload before the advertised Content-Length is reached.

## v0.0.15

  * Use `https://api.routeviews.org/meta/collectors` for routeviews collector
    index because that has download paths.
  * Retry on index failures and retry on 429.
  * Update dependencies

## v0.0.14 

  * Support hour subdirs with `ByYearMonthDayHourStrategy`.


## v0.0.13:

  * Swap between {archive, archive2}.routeviews.org for downloads
  * Handle unparseable filenames in listing
  * Fix exception on python 3.11

## v0.0.12:

  * Improve SQLite cache lock handling so cache contention does not abort downloads.

## v0.0.11:

  * Add year/month/day partitioning, for paths like
    `rrc26/2026/05/05/bview.20260505.2200.gz`


## v0.0.10:

  * updated dependencies
  * Allow limiting download parallelism (for high core count systems)

## v0.0.9:

  * retry on timeout-error and connection-error

## v.0.0.8:

  * Clean up tempfile after IO fails

## v0.0.7:

  * Retry downloads on failure
  * Use a cache (in sqlite) for downloaded indexes
  * Use newer aiohttp version

## v0.0.6:

  * CI/CD: workflow now required for merge (missing workflow was allowed
    beforehand)
  * Python 3.14 in CI/CD
  * Assume that the filename follows the conventions, and parse [day/hour/minute](day/hour/minute)
    where possible.
  * Use newer aiohttp version


## v0.0.5:

  * add routeviews support
  * add multiple file partitioning strategies
  * add parsing utility for paths, to get variables from it (see
    `src/tests/files_test.py`)
  * remove special case for route-views2 collector url (thanks Hans and team for the symlink!)

## v0.0.4:

  * Re-add python 3.11 support (previous release had 3.13 requirement by accident)
  * update aiohttp

## v0.0.3:

  * Add routeviews support
  * Add multiple partitioning methods for how files are stored.
  * Use python 3.12 (for `@deprecated` decorator)

## v0.0.2:

  * Print the target directory that was used at the end of the run.
