# Objects in REDIS for ioBroker
This is not an Adapter for ioBroker, but part of js-controller to enable the storage of objects in REDIS.

## Changelog
### 3.3.8 (2020-05-09)
* (Apollon77) check that data is existing for writeFile

### 3.3.7 (2020-05-04)
* (foxriver76) Added fileExists function

### 3.3.6 (2020-05-03)
* (foxriver76) change logging for invalid readFile's to debug

### 3.3.5 (2020-05-01)
* (foxriver76) fix logging in some places

### 3.3.4 (2020-04-28)
* (Apollon77) Fixed one callback typo

### 3.3.3 (2020-04-26)
* (bluefox) Catch some errors if no callback defined

### 3.3.2 (2020-04-17)
* (Apollon77) make sure when db connection is closed while reading view data it is handled correctly

### 3.3.1 (2020-04-15)
* (Apollon77) baseline version to generate map files for official js.controller 3 latest release 

### 3.3.0 (2020-04-11)
* (Apollon77) use deep-clone and isDeepStrictEqual
* (Apollon77) implement Async methods for all relevant methods

### 3.2.1 (2020-04-06)
* (Apollon77) Adjust invalid protocol error message

### 3.2.0 (2020-04-06)
* (foxriver76) make sure all internal paths for file store are linux style
* (foxriver76) add check and warning logs when file actions are tried without a proper meta object

### 3.1.1 (2020-04-03)
* (Apollon77) Fix some Objects File checks 

### 3.1.0 (2020-04-01)
* (Apollon77) Make sure methods that call callback async (e.g. because of db communication) always do that async

### 3.0.0 (2020-03-28)
* (foxriver76) Performance increase: adjust lua scripts and JS code to use SCAN for filter scripts
* (foxriver76) code formatting
