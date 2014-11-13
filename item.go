package dynago

import (
	"encoding/json"
	"time"
)

const (
	SELECT_ALL        = "ALL_ATTRIBUTES"
	SELECT_PROJECTED  = "ALL_PROJECTED_ATTRIBUTES"
	SELECT_ATTRIBUTES = "SPECIFIC_ATTRIBUTES"
	SELECT_COUNT      = "COUNT"
)

var (
	RETURN_CONSUMED = map[bool]string{true: "TOTAL", false: "NONE"}

	RETURN_TOTAL_CONSUMED = "TOTAL"
	RETURN_IDEX_CONSUMED  = "INDEXED"

	RETURN_METRICS = map[bool]string{true: "SIZE", false: "NONE"}

	RETURN_NONE        = "NONE"
	RETURN_ALL_OLD     = "ALL_OLD"
	RETURN_ALL_NEW     = "ALL_NEW"
	RETURN_UPDATED_OLD = "UPDATED_OLD"
	RETURN_UPDATED_NEW = "UPDATED_NEW"
)

type ConsumedCapacityDescription struct {
	CapacityUnits float32
	TableName     string
}

type KeyValue struct {
	Key   AttributeDefinition
	Value interface{}
}

// Items are maps of name/value pairs
type Item map[string]interface{}

func (pi *Item) UnmarshalJSON(data []byte) error {
	var dbitem DBItem

	if err := json.Unmarshal(data, &dbitem); err != nil {
		return err
	}

	item := make(Item)

	for k, v := range dbitem {
		item[k] = DecodeValue(v)
	}

	*pi = item
	return nil
}

func (pi *Item) MarshalJSON() ([]byte, error) {
	dbitem := DBItem{}

	for k, v := range *pi {
		dbitem[k] = EncodeValue(v)
	}

	return json.Marshal(dbitem)
}

//////////////////////////////////////////////////////////////////////////////
//
// PutItem
//

type PutItemRequest struct {
	TableName string
	Item      Item

	ConditionalExpression string `json:",omitempty"`
	ConditionalOperator   string `json:",omitempty"`

	ExpressionAttributeNames  map[string]string  `json:",omitempty"`
	ExpressionAttributeValues AttributeNameValue `json:",omitempty"`

	ReturnConsumedCapacity      string `json:",omitempty"` // INDEXED | TOTAL | NONE
	ReturnItemCollectionMetrics string `json:",omitempty"` // SIZE | NONE
	ReturnValues                string `json:",omitempty"` // NONE | ALL_OLD | UPDATED_OLD | ALL_NEW | UPDATED_NEW
}

type PutItemResult struct {
	Attributes Item

	ConsumedCapacity      ConsumedCapacityDescription
	ItemCollectionMetrics ItemCollectionMetrics
}

type ItemCollectionMetrics struct {
	ItemCollectionKey   AttributeNameValue
	SizeEstimateRangeGB []float64
}

type PutOption func(*PutItemRequest)

func PutConditionalExpression(expr string) PutOption {
	return func(putReq *PutItemRequest) {
		putReq.ConditionalExpression = expr
	}
}

func PutConditionalOperator(and bool) PutOption {
	return func(putReq *PutItemRequest) {
		if and {
			putReq.ConditionalOperator = "AND"
		} else {
			putReq.ConditionalOperator = "OR"
		}
	}
}

func PutExpressionAttributeNames(names map[string]string) PutOption {
	return func(putReq *PutItemRequest) {
		putReq.ExpressionAttributeNames = names
	}
}

func PutExpressionAttributeValues(values AttributeNameValue) PutOption {
	return func(putReq *PutItemRequest) {
		putReq.ExpressionAttributeValues = values
	}
}

func PutReturnConsumed(target string) PutOption {
	return func(putReq *PutItemRequest) {
		putReq.ReturnConsumedCapacity = target
	}
}

func PutReturnMetrics(ret bool) PutOption {
	return func(putReq *PutItemRequest) {
		putReq.ReturnItemCollectionMetrics = RETURN_METRICS[ret]
	}
}

func PutReturnValues(target string) PutOption {
	return func(putReq *PutItemRequest) {
		putReq.ReturnValues = target
	}
}

func (db *DBClient) PutItem(tableName string, item Item, options ...PutOption) (*Item, float32, error) {
	var putReq = PutItemRequest{TableName: tableName, Item: item}
	var putRes PutItemResult

	for _, option := range options {
		option(&putReq)
	}

	if err := db.Query("PutItem", &putReq).Decode(&putRes); err != nil {
		return nil, 0.0, err
	} else {
		return &putRes.Attributes, putRes.ConsumedCapacity.CapacityUnits, err
	}
}

//////////////////////////////////////////////////////////////////////////////
//
// GetItem
//

type GetItemRequest struct {
	TableName              string
	Key                    map[string]AttributeValue
	AttributesToGet        []string
	ConsistentRead         bool
	ReturnConsumedCapacity string
}

type GetItemResult struct {
	ConsumedCapacity ConsumedCapacityDescription

	Item Item
}

func (db *DBClient) GetItem(tableName string, hashKey *KeyValue, rangeKey *KeyValue, attributes []string, consistent bool, consumed bool) (map[string]interface{}, float32, error) {

	getReq := GetItemRequest{TableName: tableName, AttributesToGet: attributes, ConsistentRead: consistent, ReturnConsumedCapacity: RETURN_CONSUMED[consumed]}
	getReq.Key = EncodeAttribute(hashKey.Key, hashKey.Value)
	if rangeKey != nil {
		getReq.Key[rangeKey.Key.AttributeName] = EncodeAttributeValue(rangeKey.Key, rangeKey.Value)
	}

	var getRes GetItemResult

	if err := db.Query("GetItem", getReq).Decode(&getRes); err != nil {
		return nil, 0.0, err
	}

	if len(getRes.Item) == 0 {
		return nil, getRes.ConsumedCapacity.CapacityUnits, nil
	}

	return getRes.Item, getRes.ConsumedCapacity.CapacityUnits, nil
}

//////////////////////////////////////////////////////////////////////////////
//
// Query
//

type QueryRequest struct {
	TableName              string
	AttributesToGet        []string `json:",omitempty"`
	ScanIndexForward       bool
	ExclusiveStartKey      AttributeNameValue   `json:",omitempty"`
	KeyConditions          map[string]Condition `json:",omitempty"`
	IndexName              string               `json:",omitempty"`
	Limit                  *int                 `json:",omitempty"`
	Select                 string               `json:",omitempty"`
	ReturnConsumedCapacity string               `json:",omitempty"`

	table *TableInstance
}

type QueryResult struct {
	Items            []Item
	ConsumedCapacity ConsumedCapacityDescription
	LastEvaluatedKey AttributeNameValue
	Count            int
	ScannedCount     int
}

func QueryTable(table *TableInstance) *QueryRequest {
	return &QueryRequest{TableName: table.Name, ScanIndexForward: true, KeyConditions: make(map[string]Condition), table: table}
}

func Query(tableName string) *QueryRequest {
	return &QueryRequest{TableName: tableName, ScanIndexForward: true, KeyConditions: make(map[string]Condition)}
}

func (queryReq *QueryRequest) WithAttributes(attributes []string) *QueryRequest {
	queryReq.AttributesToGet = attributes
	return queryReq
}

func (queryReq *QueryRequest) WithStartKey(startKey AttributeNameValue) *QueryRequest {
	queryReq.ExclusiveStartKey = startKey
	return queryReq
}

func (queryReq *QueryRequest) WithIndex(indexName string) *QueryRequest {
	queryReq.IndexName = indexName
	return queryReq
}

func (queryReq *QueryRequest) WithCondition(attrName string, condition Condition) *QueryRequest {
	queryReq.KeyConditions[attrName] = condition
	return queryReq
}

func (queryReq *QueryRequest) WithAttrCondition(cond AttrCondition) *QueryRequest {
	for k, v := range cond {
		queryReq.KeyConditions[k] = v
	}

	return queryReq
}

func (queryReq *QueryRequest) WithLimit(limit int) *QueryRequest {
	queryReq.Limit = &limit
	return queryReq
}

func (queryReq *QueryRequest) WithSelect(selectValue string) *QueryRequest {
	queryReq.Select = selectValue
	return queryReq
}

func (queryReq *QueryRequest) WithConsumed(consumed bool) *QueryRequest {
	queryReq.ReturnConsumedCapacity = RETURN_CONSUMED[consumed]
	return queryReq
}

func (queryReq *QueryRequest) Exec(db *DBClient) ([]Item, AttributeNameValue, float32, error) {
	if db == nil && queryReq.table != nil {
		db = queryReq.table.DB
	}

	var queryRes QueryResult

	if err := db.Query("Query", queryReq).Decode(&queryRes); err != nil {
		return nil, nil, 0.0, err
	}

	return queryRes.Items, queryRes.LastEvaluatedKey, queryRes.ConsumedCapacity.CapacityUnits, nil
}

//////////////////////////////////////////////////////////////////////////////
//
// Scan
//

type ScanRequest struct {
	TableName              string
	AttributesToGet        []string
	ExclusiveStartKey      AttributeNameValue
	ScanFilter             map[string]Condition
	Limit                  *int   `json:",omitempty"`
	Segment                *int   `json:",omitempty"`
	TotalSegments          *int   `json:",omitempty"`
	Select                 string `json:",omitempty"`
	ReturnConsumedCapacity string `json:",omitempty"`

	table *TableInstance
}

func ScanTable(table *TableInstance) *ScanRequest {
	return &ScanRequest{TableName: table.Name, table: table}
}

func Scan(tableName string) *ScanRequest {
	return &ScanRequest{TableName: tableName}
}

func (scanReq *ScanRequest) WithAttributes(attributes []string) *ScanRequest {
	scanReq.AttributesToGet = attributes
	return scanReq
}

func (scanReq *ScanRequest) WithStartKey(startKey AttributeNameValue) *ScanRequest {
	scanReq.ExclusiveStartKey = startKey
	return scanReq
}

func (scanReq *ScanRequest) WithFilter(attrName string, condition Condition) *ScanRequest {
	if scanReq.ScanFilter == nil {
		scanReq.ScanFilter = map[string]Condition{attrName: condition}
	} else {
		scanReq.ScanFilter[attrName] = condition
	}
	return scanReq
}

func (scanReq *ScanRequest) WithFilters(filters AttrCondition) *ScanRequest {
	scanReq.ScanFilter = filters
	return scanReq
}

func (scanReq *ScanRequest) WithLimit(limit int) *ScanRequest {
	scanReq.Limit = &limit
	return scanReq
}

func (scanReq *ScanRequest) WithSegment(segment, totalSegments int) *ScanRequest {
	scanReq.Segment = &segment
	scanReq.TotalSegments = &totalSegments
	return scanReq
}

func (scanReq *ScanRequest) WithSelect(selectValue string) *ScanRequest {
	scanReq.Select = selectValue
	return scanReq
}

func (scanReq *ScanRequest) WithConsumed(consumed bool) *ScanRequest {
	scanReq.ReturnConsumedCapacity = RETURN_CONSUMED[consumed]
	return scanReq
}

func (scanReq *ScanRequest) Exec(db *DBClient) ([]Item, AttributeNameValue, float32, error) {
	var scanRes QueryResult

	if err := db.Query("Scan", scanReq).Decode(&scanRes); err != nil {
		return nil, nil, 0.0, err
	}

	return scanRes.Items, scanRes.LastEvaluatedKey, scanRes.ConsumedCapacity.CapacityUnits, nil
}

func (scanReq *ScanRequest) Count(db *DBClient) (count int, scount int, consumed float32, err error) {
	return scanReq.CountWithDelay(db, 0)
}

func (scanReq *ScanRequest) CountWithDelay(db *DBClient, delay time.Duration) (count int, scount int, consumed float32, err error) {
	var scanRes QueryResult

	req := *scanReq
	req.Select = SELECT_COUNT

	for {
		scanRes.LastEvaluatedKey = nil

		if err = db.Query("Scan", &req).Decode(&scanRes); err != nil {
			break
		}

		count += scanRes.Count
		scount += scanRes.ScannedCount
		consumed += scanRes.ConsumedCapacity.CapacityUnits

		if scanRes.LastEvaluatedKey == nil {
			break
		}

		req.ExclusiveStartKey = make(AttributeNameValue)
		for k, v := range scanRes.LastEvaluatedKey {
			req.ExclusiveStartKey[k] = v
		}

		if delay > 0 {
			time.Sleep(delay)
		}
	}

	return
}
