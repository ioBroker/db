# Objects in REDIS for ioBroker
This is not an Adapter for ioBroker, but part of js-controller to enable the storage of objects in REDIS.

## Changelog
### 1.2.9 (2019-11-29)
* (bluefox) Fixed: new objects were created with "admin" as owner and not with current user

### 1.2.8 (2019-11-22)
* (Apollon77) fix potential crash

### 1.2.7 (2019-11-18)
* (Apollon77) prevent pot. "sandboxed" objects from being wrong used in extendObject

### 1.2.6 (2019-11-14)
* (Apollon77) code refactoring and some roe error checks for disconnect cases

### 1.2.3 (2019-11-12)
* (bluefox) add logging

### 1.2.2 (2019-11-10)
* (bluefox) formatting and some sanity checks

### 1.2.1 (2019-11-07)
* (Apollon77) prevent callbacks from being called multiple times when (un)subscribing using an array

### 1.2.0 (2019-11-05)
* (bluefox) fix applyViewFunc if the start and the end key are equal 

### 1.1.50 (2019-10-28)
* (Apollon77) fix common.name filtering with empty names to work as before

### 1.1.49 (2019-10-27)
* (Apollon77) also allow numbers as first character

### 1.1.48 (2019-10-26)
* (Apollon77) Handle invalid file path as not found, only log debug

### 1.1.47 (2019-10-25)
* (Apollon77) Enhance logging

### 1.1.46 (2019-10-24)
* (Apollon77) Add some more logic to prevent access with invalid ids 

### 1.1.45 (2019-10-19)
* (Apollon77) Adjust "enhancedLogging" to be per DB
* (bluefox) Formatting

### 1.1.43 (2019-10-19)
* (Apollon77) Adjust "enhancedLogging" to be per DB

### 1.1.42 (2019-10-19)
* (Apollon77) No need to copy the Buffer when returning files

### 1.1.41 (2019-10-19)
* (Apollon77) Allow to use Redis password as "pass" parameter

### 1.1.40 (2019-10-17)
* (Apollon77) set all currently used "preserveSettings" manually till real solution in 2.1

### 1.1.39 (2019-10-16)
* (Apollon77) work on reconnection delays

### 1.1.38 (2019-10-14)
* (bluefox) Check empty objects

### 1.1.37 (2019-10-14)
* (Apollon77) Reconnection enhancements

### 1.1.36 (2019-10-14)
* (Apollon77) Reconnection enhancements

### 1.1.35 (2019-10-13)
* (Apollon77) Check setObject that argument is an object
* (Apollon77) make sure also objects with _ are allowed

### 1.1.34 (2019-10-13)
* (Apollon77) Enhance one error message

### 1.1.33 (2019-10-09)
* (Apollon77) Also allow objects with capital letters as first letter in name

### 1.1.32 (2019-10-07)
* (Apollon77) increase LUA script timeout to 10s for now till real fix for ObjectViews in js-controller 2.1

### 1.1.30 (2019-10-06)
* (Apollon77) make sure ioredis do not throw errors on unhandled promises when closing the connection

### 1.1.28 (2019-10-04)
* (Apollon77) try to optimize run performance a bit, especially run callbacks and onchange handlings on next tick

### 1.1.26 (2019-10-04)
* (Apollon77) sanitize file path the same for redis as for file

### 1.1.25 (2019-10-03)
* (Apollon77) adjust object namespace regex and handling

### 1.1.24 (2019-10-01)
* (Apollon77) allow to set Redis DB and network family, defaults to 0

### 1.1.23 (2019-09-30)
* (Apollon77) small optimizations

### 1.1.22 (2019-09-27)
* (Apollon77) fix fallback handling with old style "emit"

### 1.1.21 (2019-09-26)
* (Apollon77) getting States with wildcard not at the end is not working well in lua, simulate it for now

### 1.1.20 (2019-09-26)
* (Apollon77) only use quit from redid and let ioredis handle the disconnect

### 1.1.19 (2019-09-24)
* (Apollon77) optimize communication processes by calling quit instead of just disconnecting

### 1.1.18 (2019-09-22)
* (Apollon77) optimize handling for directories to be more compatible with socket.io before

### 1.1.17 (2019-09-21)
* (Apollon77) normalize filenames to prevent problems with multiple slashes in the path

### 1.1.15 (2019-09-16)
* (Apollon77) enhance some error messages

### 1.1.14 (2019-09-15)
* (Apollon77) fix getObjectView for "custom" because it was returning null values

### 1.1.13 (2019-09-15)
* (Apollon77) latest fixes, add some more edge case handling

### 1.1.12 (2019-09-15)
* (Apollon77) fix and enhance rename method

### 1.1.11 (2019-09-14)
* (Apollon77) fix chmod, chown and special file functions

### 1.1.10 (2019-09-10)
* (Apollon77) fix unlink to be compatible to socket.io

### 1.1.8 (2019-09-09)
* (Apollon77) fix readDir to be compatible to before and some other file stuff

### 1.1.7 (2019-09-08)
* (Apollon77) fix some special object views

### 1.1.6 (2019-08-30)
* (Apollon77) correct logging message

### 1.1.5 (2019-08-19)
* (Apollon77) enhance logging to always contain the namespace

### 1.1.4 (2019-08-11)
* (Apollon77) enhance filter view queries to work more generic

### 1.1.3 (2019-08-07)
* (Apollon77) fix redis initializations

### 1.1.2 (2019-08-06)
* (Apollon77) optimize code

### 1.1.1 (2019-07-28)
* (Apollon77) handle error case for not existing keys

### 1.1.0 (2019-07-19)
* (Apollon77) Add Redis Sentinel Support

### 1.0.3 (2019-06-25)
* (bluefox) Add aliases

### 1.0.1 (2019-05-23)
* (bluefox) catch parse errors

### 0.4.4 (2019-05-10)
* (Apollon77) Remove additional logging and finalize for now, one bug left

### 0.4.0-3 (2019-05-07)
* (Apollon77) switch to ioredis as library and add some debug

### 0.3.3-8 (2019-05-05)
* (Apollon77) fixes and optimizations

### 0.3.2 (2019-05-05)
* (bluefox) remove objectsUtils.js

### 0.3.1 (2019-05-05)
* (Apollon77) fixes for Redis-In-Mem-Servers

### 0.3.0 (2019-04-12)
* (Apollon77) prepare for use with Redis-In-Mem-Servers

### 0.2.8 (2018-12-31)
* (bluefox) allow array for subscribeForeignObjects and subscribeObjects