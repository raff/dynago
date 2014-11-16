dynago
======

DynamoDB client for Go,
*now with [streams](http://dynamodb-preview.s3-website-us-west-2.amazonaws.com/docs/streams-dg/About.html) support*

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
* Download/install and run [DynamoDB Local](  http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.DynamoDBLocal.html)
  (use [this version](http://dynamodb-preview.s3-website-us-west-2.amazonaws.com/docs/streams-dg/About.html#RequiredToolsAndResources.DynamoDBLocal) for streams support).
* Add an entry in /etc/hosts for a URL that looks like a DynamoDB endpoint ([aws4.Sign](https://github.com/raff/aws4/blob/master/sign.go#L50) picks the region from the endpoint URL):
```
    127.0.0.1 dynamodb.local.amazonaws.com
```
* Add a section in .dynagorc for the "local" environment:
```
    [profile "local"]
    region = http://dynamodb.local.amazonaws.com:8000
    accessKey = AAAAAAAAAAAAAAAAAAAA
    secretKey = xxxxxxxxxxxxxxxxxxxx
```
* run dynagosh using the local environment:
```
    > dynagosh --env=local
```

### Use stream commands
* Run against DynamoDB Local (streams preview)
  See previous section
* Create a table with stream enabled
```
dynagosh> create message id:S title:S 5 5 new

struct {
  AttributeDefinitions: [
    struct {
      AttributeName: "id",
      AttributeType: "S"
    },
    struct {
      AttributeName: "title",
      AttributeType: "S"
    }
  ],
  CreationDateTime: 2014-11-15 16:47:33 -0800 PST,
  ItemCount: 0,
  KeySchema: [
    struct {
      AttributeName: "id",
      KeyType: "HASH"
    },
    struct {
      AttributeName: "title",
      KeyType: "RANGE"
    }
  ],
  LocalSecondaryIndexes: [
  ],
  ProvisionedThroughput: struct {
    LastDecreaseDateTime: 1969-12-31 16:00:00 -0800 PST,
    LastIncreaseDateTime: 1969-12-31 16:00:00 -0800 PST,
    NumberOfDecreasesToday: 0,
    ReadCapacityUnits: 5,
    WriteCapacityUnits: 5
  },
  TableName: "message",
  TableSizeBytes: 0,
  TableStatus: "ACTIVE",
  StreamSpecification: struct {
    StreamEnabled: true,
    StreamViewType: "NEW_IMAGE"
  }
}
```
* List tables
```
dynagosh> ls

Available tables
   message
```
* List streams
```
dynagosh> lss

Available streams
0 78e731027d8fd50ed642340b7c9a63b314160988539599c66a
```
* Add a message
```
dynagosh> put message "{\"id\":\"1\", \"title\":\"hello\", \"message\":\"hello there\"}"

{
}
consumed: 0
```
* Add another message
```
dynagosh> put message "{\"id\":\"2\", \"title\":\"hi\", \"message\":\"hello agi\"}"
{
}
consumed: 0
```
* List stream records
```
dynagosh> lsr 0

INSERT {
  "id": "1",
  "message": "hello there",
  "title": "hello"
}

INSERT {
  "message": "hello agi",
  "title": "hi",
  "id": "2"
}
```
* Verbose stream records
```
dynagosh> lsr -verbose 0

struct {
  NextShardIterator: "000/NzhlNzMxMDI3ZDhmZDUwZWQ2NDIzNDBiN2M5YTYzYjMxNDE2MDk4ODUzOTU5OWM2NmEvc2hhcmRJZC0wMDAwMDAwMTQxNj
A5ODg1Mzk2Ni0xMmY2NjhkMS8wMDAwMDAwMDAwMDAwMDAwMDAwMDMvMDAwMDAwMDAwMDAwMDAwMDAxNDE2MDk4OTg1Nzg2",
  Records: [
    struct {
      AwsRegion: "ddblocal",
      Dynamodb: struct {
        Keys: {
          "id": "1",
          "title": "hello"
        },
        NewImage: {
          "message": "hello there",
          "title": "hello",
          "id": "1"
        },
        OldImage: {
        },
        SequenceNumber: "000000000000000000001",
        SizeBytes: 44,
        StreamViewType: "NEW_IMAGE"
      },
      EventID: "0c744dd5-737b-42bb-864b-d18d5d5e71af",
      EventName: "INSERT",
      EventSource: "aws:dynamodb",
      EventVersion: "1.0"
    },
    struct {
      AwsRegion: "ddblocal",
      Dynamodb: struct {
        Keys: {
          "title": "hi",
          "id": "2"
        },
        NewImage: {
          "id": "2",
          "message": "hello agi",
          "title": "hi"
        },
        OldImage: {
        },
        SequenceNumber: "000000000000000000002",
        SizeBytes: 36,
        StreamViewType: "NEW_IMAGE"
      },
      EventID: "599e9002-2c73-43e0-80d5-127fd6947ba4",
      EventName: "INSERT",
      EventSource: "aws:dynamodb",
      EventVersion: "1.0"
    }
  ]
}
```
