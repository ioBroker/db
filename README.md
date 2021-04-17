# Mono Repo for all standard DB classes for ioBroker 

## Changelog
<!--
	Placeholder for the next version (at the beginning of the line):
	### __WORK IN PROGRESS__
-->

### 1.2.1 (2021-04-17)
* (Apollon77) Fix async responses from chmodFile and chownFile
* (Apollon77) optimize db initialization for fileDB and enhance error case handling
* (Apollon77/foxriver76) several optimizations and fixes

### 1.2.0 (2021-03-23)
* (bluefox) fix the redis function applyViewFunc if the name is a localized object

### 1.1.5 (2021-02-08)
* (AlCalzone) fix jsonl db to use correct settings and compact the db

### 1.1.4 (2021-02-08)
* (Apollon77/AlCalzone) change dependency handling between the db packages

### 1.1.3 (2021-02-08)
* (AlCalzone) use absolute dir for jsonl db

### 1.1.2 (2021-02-07)
* (AlCalzone) fix jsonl db proxy object

### 1.1.1 (2021-02-07)
* (AlCalzone) fix jsonl db proxy object

### 1.1.0 (2021-02-07)
* (Apollon77) adjust scan entry count to 250 to have smaller script runs and hopefully better redis performance
* (Apollon77) Restructure base MemFileDB to prepare jsonl
* (Apollon77) When creating backup simply rename the file instead of read/write - lowers write i/o by 50%
* (AlCalzone) EXPERIMENTAL: First version of jsonl DB classes to test if this is better on i/o

### 1.0.11 (2021-02-01)
* (foxriver76) fix redis client names

### 1.0.10 (2021-01-28)
* (Apollon77) add missing log namespaces in some places
* (Apollon77) Do not log error objects directly, but e.message
* (Apollon77) deleting a not existing object is handled as success instead of Not-Exists error

### 1.0.9 (2021-01-23)
* (Apollon77) make sure errors in lua script initialization do not run into endless loop
* (Apollon77) make sure in setState that a null state do not crash

### 1.0.8 (2021-01-21)
* (Apollon77) enhance error handling in one place in objects db

### 1.0.7 (2021-01-15)
* (Apollon77) Map Redis connectivity issues to the normal ERROR_DB_CLOSED error to allow unique handling in js-controller

### 1.0.6 (2021-01-10)
* (Apollon77) fix rename of directories
* (Apollon77) fix reading of root dir via redis

### 1.0.5 (2021-01-10)
* (Apollon77) fix issues in readDir with directories

### 1.0.4 (2021-01-09)
* (Apollon77) optimize deleting directories and files in file storage

### 1.0.3 (2021-01-05)
* (Apollon77) fix logging for server in testing
* (Apollon77) fix defaultport request for setup custom

### 1.0.2 (2021-01-05)
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