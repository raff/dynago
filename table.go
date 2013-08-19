package dynago

import (
	"encoding/json"
	"time"
)

const (
	HASH_KEY_TYPE  = "HASH"
	RANGE_KEY_TYPE = "RANGE"

	STRING_ATTRIBUTE     = "S"
	STRING_SET_ATTRIBUTE = "SS"
	NUMBER_ATTRIBUTE     = "N"
	NUMBER_SET_ATTRIBUTE = "NS"
	BINARY_ATTRIBUTE     = "B"
	BINARY_SET_ATTRIBUTE = "BS"

	TABLE_STATUS_CREATING = "CREATING"
	TABLE_STATUS_DELETING = "DELETING"
	TABLE_STATUS_UPDATING = "UPDATING"
	TABLE_STATUS_ACTIVE   = "ACTIVE"
)

// EpochTime is like Time, but unmarshal from a number (seconds since Unix epoch) instead of a formatted string
// (this is what AWS returns)

type EpochTime struct {
	time.Time
}

// Unmarshal from number to time.Time

func (t *EpochTime) UnmarshalJSON(data []byte) (err error) {
	var v float64
	if err = json.Unmarshal(data, &v); err != nil {
		return
	}

	*t = EpochTime{time.Unix(int64(v), 0)} // need to convert the fractional part in nanoseconds
	return nil
}

// Table definition

type AttributeDefinition struct {
	AttributeName string
	AttributeType string
}

type KeySchemaElement struct {
	AttributeName string
	KeyType       string
}

type ProjectionDescription struct {
	NonKeyAttributes []string
	ProjectionType   string
}

type LocalSecondaryIndexDescription struct {
	IndexName      string
	IndexSizeBytes int64
	ItemCount      int64

	KeySchema  []KeySchemaElement
	Projection ProjectionDescription
}

type ProvisionedThroughputDescription struct {
	LastDecreaseDateTime   EpochTime
	LastIncreaseDateTime   EpochTime
	NumberOfDecreasesToday int
	ReadCapacityUnits      int
	WriteCapacityUnits     int
}

type TableDescription struct {
	AttributeDefinitions []AttributeDefinition

	CreationDateTime EpochTime
	ItemCount        int64

	KeySchema             []KeySchemaElement
	LocalSecondaryIndexes []LocalSecondaryIndexDescription
	ProvisionedThroughput ProvisionedThroughputDescription

	TableName      string
	TableSizeBytes int64
	TableStatus    string
}

//////////////////////////////////////////////////////////////////////////////
//
// ListTables
//

type ListTablesResult struct {
	TableNames []string
}

func (db *DBClient) ListTables() ([]string, error) {
	var listRes ListTablesResult
	if err := db.Query("ListTables", nil).Decode(&listRes); err != nil {
		return nil, err
	} else {
		return listRes.TableNames, nil
	}
}

//////////////////////////////////////////////////////////////////////////////
//
// DescribeTable
//

type DescribeTableRequest struct {
	TableName string
}

type DescribeTableResult struct {
	Table TableDescription
}

func (db *DBClient) DescribeTable(tableName string) (*TableDescription, error) {
	var descRes DescribeTableResult

	if err := db.Query("DescribeTable", DescribeTableRequest{tableName}).Decode(&descRes); err != nil {
		return nil, err
	}

	return &descRes.Table, nil
}

//////////////////////////////////////////////////////////////////////////////
//
// CreateTable
//

type ProvisionedThroughputRequest struct {
	ReadCapacityUnits  int
	WriteCapacityUnits int
}

type LocalSecondaryIndexRequest struct {
	IndexName  string
	KeySchema  []KeySchemaElement
	Projection ProjectionDescription
}

type CreateTableRequest struct {
	TableName             string
	ProvisionedThroughput ProvisionedThroughputRequest
	AttributeDefinitions  []AttributeDefinition
	KeySchema             []KeySchemaElement
	LocalSecondaryIndexes []LocalSecondaryIndexRequest
}

type CreateTableResult struct {
	TableDescription TableDescription
}

func (db *DBClient) CreateTable(tableName string, hashKey *AttributeDefinition, rangeKey *AttributeDefinition, rc, wc int) (*TableDescription, error) {
	createReq := CreateTableRequest{
		TableName:             tableName,
		ProvisionedThroughput: ProvisionedThroughputRequest{rc, wc},
	}

	attrs := []AttributeDefinition{*hashKey}
	schema := []KeySchemaElement{KeySchemaElement{hashKey.AttributeName, HASH_KEY_TYPE}}
	if rangeKey != nil {
		attrs = append(attrs, *rangeKey)
		schema = append(schema, KeySchemaElement{rangeKey.AttributeName, RANGE_KEY_TYPE})
	}

	createReq.AttributeDefinitions = attrs
	createReq.KeySchema = schema

	var createRes CreateTableResult

	if err := db.Query("CreateTable", createReq).Decode(&createRes); err != nil {
		return nil, err
	}

	return &createRes.TableDescription, nil
}

//////////////////////////////////////////////////////////////////////////////
//
// UpdateTable
//

type UpdateTableRequest struct {
	TableName             string
	ProvisionedThroughput ProvisionedThroughputRequest
}

type UpdateTableResult struct {
	TableDescription TableDescription
}

func (db *DBClient) UpdateTable(tableName string, rc, wc int) (*TableDescription, error) {
	/*
	   here we should do a DescribeTable first, and then a loop of UpdateTable requests
	   considering that we can only double each value every time
	*/

	updReq := UpdateTableRequest{
		TableName:             tableName,
		ProvisionedThroughput: ProvisionedThroughputRequest{rc, wc},
	}

	var updRes UpdateTableResult

	if err := db.Query("UpdateTable", updReq).Decode(&updRes); err != nil {
		return nil, err
	}

	return &updRes.TableDescription, nil
}

//////////////////////////////////////////////////////////////////////////////
//
// DeleteTable
//

type DeleteTableRequest struct {
	TableName string
}

type DeleteTableResult struct {
	Table TableDescription
}

func (db *DBClient) DeleteTable(tableName string) (*TableDescription, error) {
	var delRes DeleteTableResult

	if err := db.Query("DeleteTable", DeleteTableRequest{tableName}).Decode(&delRes); err != nil {
		return nil, err
	}

	return &delRes.Table, nil
}
