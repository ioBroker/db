# Mono Repo for all standard DB classes for ioBroker 

## Changelog
<!--
	Placeholder for the next version (at the beginning of the line):
	### __WORK IN PROGRESS__
-->

### __WORK IN PROGRESS__
* (Apollon77) Add a workaround mainly for testing that subscribes to states/objects before db is connected

### 1.0.1 (2021-01-05)
* (Apollon77) fix Buffer deprecation

### 1.0.0 (2021-01-04)
* (Apollon77) finalize first iteration of restructuring of db classes and always use the client class for all communications

### 0.0.14 (2020-12-30)
* (Apollon77) initialize data directory for file DBs relative to js-controller dir

### 0.0.13 (2020-12-30)
* (Apollon77) handle non-object cases in clone better (Sentry IOBROKER-JS-CONTROLLER-1Z9)
* (Apollon77) fix path to iobroker-data to store data files

### 0.0.12 (2020-12-30)
* (Apollon77) make sure common.custom is always an object and handle legacy cases to fix former invalid content to prevent crashes

### 0.0.10 (2020-12-29)
* (Apollon77) log an info message on redis for states and objects db reconnect after an error state

### 0.0.9 (2020-12-28)
* (Apollon77) add method to get default port for the various db types
* (foxriver76) use standard acl as first priority on setObject

### 0.0.6 (2020-09-22)
* (Apollon77) Fix tools.js lookup and add local development and test-proof ways

### 0.0.1 (2020-09-22)
* (Apollon77) Converted the DB classes to multiple packages managed in a monorepo; Initial release