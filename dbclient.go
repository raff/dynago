package dynago

import (
	//"github.com/bmizerany/aws4"
	//"github.com/bmizerany/aws4/dydb"
	"github.com/raff/aws4"
	"github.com/raff/aws4/dydb"
)

const (
	REGION_US_EAST_1 = "https://dynamodb.us-east-1.amazonaws.com/"
	REGION_US_WEST_1 = "https://dynamodb.us-west-1.amazonaws.com/"
	REGION_US_WEST_2 = "https://dynamodb.us-west-2.amazonaws.com/"
)

//////////////////////////////////////////////////////////////////////////////
//
// A wrapper for aws4.dydb.DB so that we can expose DynamoDB operations
//

type DBClient struct {
	dydb.DB
}

// Create a new DynamoDB client
func NewDBClient() (db *DBClient) {
	db = &DBClient{}
	return
}

func (db *DBClient) WithRegion(region string) *DBClient {
	db.URL = region
	return db
}

func (db *DBClient) WithCredentials(accessKey, secretKey string) *DBClient {
	db.Client = &aws4.Client{Keys: &aws4.Keys{accessKey, secretKey}}
	return db
}
