dynago
======

DynamoDB client for Go

## Installation
    $ go get github.com/raff/dynago

## Library Documentation
http://godoc.org/github.com/raff/dynago

## Command line tool

### Installation
    $ go install github.com/raff/dynago/dynagosh
    
### Execution
    $ dynagosh

### Configuration
  Create a file named .dynagorc in the current directory or in your home directory that looks like this:

    [dynago]
    profile=default_profile_name

    [profile "default_profile_name"]
    region=us-west-2
    accessKey=aws_access_key
    secretKey=aws_secret_key
    
The file should contain two or more sections:

* One named "dynago" with one entry with name "profile" and value the name of the default profile to use.
* One or more "profile" section (the name should be [profile "xxx"]) where "xxx" identify the profile name.
Each of these sections should have the correct accessKey and secretKey to access your DynamoDB tables and optionally the region (default "us-east-1")

### Usage with DynamoDB Local

In order to test against DynamoDB local (the local "test" version of DynamoDB):
* Download/install and run DynamoDB Local
* Add an entry in /etc/hosts for a URL that looks like a DynamoDB endpoint:

    127.0.0.1 dynamodb.local.amazonaws.com
* Add a section in .dynagorc for the "local" environment:

    [profile "local"]
    region = http://dynamodb.local.amazonaws.com:8000
    accessKey = AAAAAAAAAAAAAAAAAAAA
    secretKey = xxxxxxxxxxxxxxxxxxxx

* run dynagosh using the local environment:

    > dynagosh --env=local
