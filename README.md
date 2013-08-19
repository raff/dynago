dynago
======

DynamoDB client for Go

## Installation
    $ go get github.com/raff/dynago

## Documentation
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

    [profile "default_prifile_name"]
    region=us-west-2
    accessKey=aws_access_key
    secretKey=aws_secret_key
    
The file should contain two or more sections:

* One named "dynago" with one entry with name "profile" and value the name of the default profile to use.
* One or more "profile" section (the name should be [profile "xxx"]) where "xxx" identify the profile name.
* Each of these sections should have one entry 
