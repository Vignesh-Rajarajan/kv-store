# KV-Store

## Description
This is a simple key-value store that supports the following operations:
- `GET <key>`: Returns the value associated with the key
- `SET <key> <value>`: Sets the value for the key
- `DELETE <key>`: Deletes the key and its value

This has replication support for the following:
- `GET <key>`: Returns the value associated with the key
- `SET <key> <value>`: Sets the value for the key

## Usage
To run the server, run the following command:
```
./launch.sh 
```
which has the properties set in for :
    `-db-location` : The location of the database
    `-http-addr` : The port to run the server on
    `-replication` : The replication factor for the server
    `-config-file` : The location of the config file

To run the Benchmark, run the following command:
```
go run benchmark/main.go
```
which has the default properties set in for :
    `-http-addr` : The port to run the server on
    `-config-file` : The location of the config file

