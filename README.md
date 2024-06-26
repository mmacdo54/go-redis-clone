# go-redis-clone

## About
A simple to run basic implementation of a Redis server - this is just for a bit of fun but the intention is that most/all of the Redis commands are supported eventually.

## Running
1. Make sure you have Go installed
2. Make sure you have docker installed
2. Pull the github repo
3. Run `docker compose up -d && go run .` - this will start the database on localhost:27017 and redis server on localhost:6379

## Supported Commands
Currently supported Redis Commands
- AUTH (only supports password)
- SET
- GET
- EXISTS
- DEL
- COPY
- LPUSH
- LPUSHX
- LPOP (Only first item for now)
- RPUSH
- RPUSHX
- RPOP (Only first item for now)
- LLEN
- LINDEX
- SADD
- SMEMBERS
- SISMEMBER
- PERSIST
- EXPIRE
- EXPIREAT
- PEXPIRE
- PEXPIREAT
- EXPIRETIME
- SUBSCRIBE
- PUBLISH
- UNSUBSCRIBE

## Config
Currently config can only be set by using a redis.conf file, the following config options are supported:

- requirepass {password} - must be between 16-128 characters and only special characters (no spaces) are !,&,#,$,^,<,>, and -
