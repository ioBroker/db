# Mono Repo for all standard DB classes for ioBroker 

## Changelog
<!--
	Placeholder for the next version (at the beginning of the line):
	### __WORK IN PROGRESS__
-->

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