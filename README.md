# go-redis-clone

## About
A simple to run basic implementation of a Redis server - this is just for a bit of fun but the intention is that most/all of the Redis commands are supported eventually.

## Running
1. Make sure you have Go installed
2. Pull the github repo
3. Run `go get`
4. Run `go run .` - this will start the redis server on localhost:6379

## Supported Commands
Currently supported Redis Commands
- SET
- GET
- EXISTS
- DEL (Only allows one key ATM)
- PERSIST
- SUBSCRIBE (single channel only)
- PUBLISH
- UNSUBSCRIBE (channel must be specified)
