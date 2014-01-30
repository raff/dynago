package dynago

import (
	//"github.com/bmizerany/aws4"
	//"github.com/bmizerany/aws4/dydb"
	"github.com/raff/aws4"
	"github.com/raff/aws4/dydb"

	"strings"
)

const (
	REGION_US_EAST_1 = "https://dynamodb.us-east-1.amazonaws.com/"
	REGION_US_WEST_1 = "https://dynamodb.us-west-1.amazonaws.com/"
	REGION_US_WEST_2 = "https://dynamodb.us-west-2.amazonaws.com/"

	region_pattern = "https://dynamodb.{}.amazonaws.com/"

        RETRY_COUNT = 10
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

func (db *DBClient) Query(action string, v interface{}) dydb.Decoder {
    return db.DB.RetryQuery(action, v, RETRY_COUNT)
}

func (db *DBClient) WithRegion(region string) *DBClient {

	if !strings.Contains(region, "/") {
		// not a URL
		region = strings.Replace(region_pattern, "{}", region, 1)
	}
	db.URL = region
	return db
}

func (db *DBClient) WithCredentials(accessKey, secretKey string) *DBClient {
	db.Client = &aws4.Client{Keys: &aws4.Keys{AccessKey: accessKey, SecretKey: secretKey}}
	return db
}
