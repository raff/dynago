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
	RETURN_INDEX_CONSUMED = "INDEXED"

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
	var dbitem AttributeNameValue

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
	dbitem := AttributeNameValue{}

	for k, v := range *pi {
		dbitem[k] = EncodeValue(v)
	}

	return json.Marshal(dbitem)
}

//////////////////////////////////////////////////////////////////////////////
//
// Put/Update/Delete Item Result
//

type ItemResult struct {
	Attributes            Item
	ConsumedCapacity      ConsumedCapacityDescription
	ItemCollectionMetrics ItemCollectionMetrics
}

type ItemCollectionMetrics struct {
	ItemCollectionKey   AttributeNameValue
	SizeEstimateRangeGB []float64
}

//////////////////////////////////////////////////////////////////////////////
//
// Put/Update/Delete Item Request
//

type ItemRequest struct {
	TableName string

	Item             *Item              `json:",omitempty"` // PutItem
	Key              AttributeNameValue `json:",omitempty"` // UpdateItem/DeleteItem
	UpdateExpression string             `json:",omitempty"` // UpdateItem

	ConditionExpression       string             `json:",omitempty"`
	ExpressionAttributeNames  map[string]string  `json:",omitempty"`
	ExpressionAttributeValues AttributeNameValue `json:",omitempty"`

	ReturnConsumedCapacity      string `json:",omitempty"` // INDEXED | TOTAL | NONE
	ReturnItemCollectionMetrics string `json:",omitempty"` // SIZE | NONE
	ReturnValues                string `json:",omitempty"` // NONE | ALL_OLD | UPDATED_OLD | ALL_NEW | UPDATED_NEW
}

type ItemOption func(*ItemRequest)

func ConditionExpression(expr string) ItemOption {
	return func(req *ItemRequest) {
		req.ConditionExpression = expr
	}
}

func ExpressionAttributeNames(names map[string]string) ItemOption {
	return func(req *ItemRequest) {
		req.ExpressionAttributeNames = names
	}
}

func ExpressionAttributeValues(values map[string]interface{}) ItemOption {
	return func(req *ItemRequest) {
		req.ExpressionAttributeValues = EncodeItem(values)
	}
}

func ReturnConsumed(target string) ItemOption {
	return func(req *ItemRequest) {
		req.ReturnConsumedCapacity = target
	}
}

func ReturnMetrics(ret bool) ItemOption {
	return func(req *ItemRequest) {
		req.ReturnItemCollectionMetrics = RETURN_METRICS[ret]
	}
}

func ReturnValues(target string) ItemOption {
	return func(req *ItemRequest) {
		req.ReturnValues = target
	}
}

//////////////////////////////////////////////////////////////////////////////
//
// PutItem
//

func (db *DBClient) PutItem(tableName string, item Item, options ...ItemOption) (*Item, float32, error) {
	var req = ItemRequest{TableName: tableName, Item: &item}
	var res ItemResult

	for _, option := range options {
		option(&req)
	}

	if err := db.Query("PutItem", &req).Decode(&res); err != nil {
		return nil, 0.0, err
	} else {
		return &res.Attributes, res.ConsumedCapacity.CapacityUnits, err
	}
}

//////////////////////////////////////////////////////////////////////////////
//
// UpdateItem
//

func (db *DBClient) UpdateItem(tableName string, hashKey *KeyValue, rangeKey *KeyValue, updates string, options ...ItemOption) (*Item, float32, error) {
	var req = ItemRequest{TableName: tableName, UpdateExpression: updates}
	var res ItemResult

	req.Key = EncodeAttribute(hashKey.Key, hashKey.Value)
	if rangeKey != nil {
		req.Key[rangeKey.Key.AttributeName] = EncodeAttributeValue(rangeKey.Key, rangeKey.Value)
	}

	if rangeKey != nil {
		req.Key[rangeKey.Key.AttributeName] = EncodeAttributeValue(rangeKey.Key, rangeKey.Value)
	}

	for _, option := range options {
		option(&req)
	}

	if err := db.Query("UpdateItem", &req).Decode(&res); err != nil {
		return nil, 0.0, err
	} else {
		return &res.Attributes, res.ConsumedCapacity.CapacityUnits, err
	}
}

//////////////////////////////////////////////////////////////////////////////
//
// DeleteItem
//

func (db *DBClient) DeleteItem(tableName string, hashKey *KeyValue, rangeKey *KeyValue, options ...ItemOption) (*Item, float32, error) {
	var req = ItemRequest{TableName: tableName}
	var res ItemResult

	req.Key = EncodeAttribute(hashKey.Key, hashKey.Value)
	if rangeKey != nil {
		req.Key[rangeKey.Key.AttributeName] = EncodeAttributeValue(rangeKey.Key, rangeKey.Value)
	}

	for _, option := range options {
		option(&req)
	}

	if err := db.Query("DeleteItem", &req).Decode(&res); err != nil {
		return nil, 0.0, err
	} else {
		return &res.Attributes, res.ConsumedCapacity.CapacityUnits, err
	}
}

//////////////////////////////////////////////////////////////////////////////
//
// GetItem
//

type GetItemRequest struct {
	TableName              string
	Key                    AttributeNameValue
	AttributesToGet        []string `json:",omitempty"`
	ConsistentRead         bool
	ReturnConsumedCapacity string `json:",omitempty"`
}

type GetItemResult struct {
	ConsumedCapacity ConsumedCapacityDescription

	Item Item
}

func (db *DBClient) GetItem(tableName string, hashKey *KeyValue, rangeKey *KeyValue, attributes []string, consistent bool, consumed bool) (map[string]interface{}, float32, error) {

	req := GetItemRequest{TableName: tableName, AttributesToGet: attributes, ConsistentRead: consistent, ReturnConsumedCapacity: RETURN_CONSUMED[consumed]}
	req.Key = EncodeAttribute(hashKey.Key, hashKey.Value)
	if rangeKey != nil {
		req.Key[rangeKey.Key.AttributeName] = EncodeAttributeValue(rangeKey.Key, rangeKey.Value)
	}

	var res GetItemResult

	if err := db.Query("GetItem", req).Decode(&res); err != nil {
		return nil, 0.0, err
	}

	if len(res.Item) == 0 {
		return nil, res.ConsumedCapacity.CapacityUnits, nil
	}

	return res.Item, res.ConsumedCapacity.CapacityUnits, nil
}

//////////////////////////////////////////////////////////////////////////////
//
// Query
//

type QueryRequest struct {
	TableName        string
	AttributesToGet  []string `json:",omitempty"` // deprecated
	ScanIndexForward bool

	ExclusiveStartKey AttributeNameValue   `json:",omitempty"`
	KeyConditions     map[string]Condition `json:",omitempty"` // deprecated
	IndexName         string               `json:",omitempty"`

	KeyConditionExpression    string             `json:",omitempty"`
	FilterExpression          string             `json:",omitempty"`
	ProjectionExpression      string             `json:",omitempty"`
	ExpressionAttributeNames  map[string]string  `json:",omitempty"`
	ExpressionAttributeValues AttributeNameValue `json:",omitempty"`

	Limit                  *int   `json:",omitempty"`
	Select                 string `json:",omitempty"`
	ReturnConsumedCapacity string `json:",omitempty"`

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
	return &QueryRequest{TableName: table.Name, ScanIndexForward: true, table: table}
}

func Query(tableName string) *QueryRequest {
	return &QueryRequest{TableName: tableName, ScanIndexForward: true}
}

// deprecated
func (req *QueryRequest) SetAttributes(attributes []string) *QueryRequest {
	req.AttributesToGet = attributes
	return req
}

func (req *QueryRequest) SetStartKey(startKey AttributeNameValue) *QueryRequest {
	req.ExclusiveStartKey = startKey
	return req
}

func (req *QueryRequest) SetIndex(indexName string) *QueryRequest {
	req.IndexName = indexName
	return req
}

// deprecated
func (req *QueryRequest) SetCondition(attrName string, condition Condition) *QueryRequest {
	req.KeyConditions[attrName] = condition
	return req
}

// deprecated
func (req *QueryRequest) SetAttrCondition(cond AttrCondition) *QueryRequest {
	for k, v := range cond {
		req.KeyConditions[k] = v
	}

	return req
}

func (req *QueryRequest) SetConditionExpression(cond string) *QueryRequest {
	req.KeyConditionExpression = cond
	return req
}

func (req *QueryRequest) SetFilterExpression(filter string) *QueryRequest {
	req.FilterExpression = filter
	return req
}

func (req *QueryRequest) SetProjectionExpression(proj string) *QueryRequest {
	req.ProjectionExpression = proj
	return req
}

func (req *QueryRequest) SetLimit(limit int) *QueryRequest {
	req.Limit = &limit
	return req
}

func (req *QueryRequest) SetSelect(selectValue string) *QueryRequest {
	req.Select = selectValue
	return req
}

func (req *QueryRequest) SetConsumed(consumed bool) *QueryRequest {
	req.ReturnConsumedCapacity = RETURN_CONSUMED[consumed]
	return req
}

func (req *QueryRequest) Exec(db *DBClient) ([]Item, AttributeNameValue, float32, error) {
	if db == nil && req.table != nil {
		db = req.table.DB
	}

	var res QueryResult

	if err := db.Query("Query", req).Decode(&res); err != nil {
		return nil, nil, 0.0, err
	}

	return res.Items, res.LastEvaluatedKey, res.ConsumedCapacity.CapacityUnits, nil
}

//////////////////////////////////////////////////////////////////////////////
//
// Scan
//

type ScanRequest struct {
	TableName string
	//AttributesToGet   []string    // use ProjectionExpression instead
	ExclusiveStartKey AttributeNameValue

	FilterExpression          string             `json:",omitempty"`
	ProjectionExpression      string             `json:",omitempty"`
	ExpressionAttributeNames  map[string]string  `json:",omitempty"`
	ExpressionAttributeValues AttributeNameValue `json:",omitempty"`

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

func (req *ScanRequest) SetStartKey(startKey AttributeNameValue) *ScanRequest {
	req.ExclusiveStartKey = startKey
	return req
}

func (req *ScanRequest) SetFilterExpression(filter string) *ScanRequest {
	req.FilterExpression = filter
	return req
}

func (req *ScanRequest) SetAttributeNames(names map[string]string) *ScanRequest {
	req.ExpressionAttributeNames = names
	return req
}

func (req *ScanRequest) SetAttributeValues(values map[string]interface{}) *ScanRequest {
	req.ExpressionAttributeValues = EncodeItem(values)
	return req
}

func (req *ScanRequest) SetProjectionExpression(proj string) *ScanRequest {
	req.ProjectionExpression = proj
	return req
}

func (req *ScanRequest) SetLimit(limit int) *ScanRequest {
	req.Limit = &limit
	return req
}

func (req *ScanRequest) SetSegment(segment, totalSegments int) *ScanRequest {
	req.Segment = &segment
	req.TotalSegments = &totalSegments
	return req
}

func (req *ScanRequest) SetSelect(selectValue string) *ScanRequest {
	req.Select = selectValue
	return req
}

func (req *ScanRequest) SetConsumed(consumed bool) *ScanRequest {
	req.ReturnConsumedCapacity = RETURN_CONSUMED[consumed]
	return req
}

func (req *ScanRequest) Exec(db *DBClient) ([]Item, AttributeNameValue, float32, error) {
	var res QueryResult

	if err := db.Query("Scan", req).Decode(&res); err != nil {
		return nil, nil, 0.0, err
	}

	return res.Items, res.LastEvaluatedKey, res.ConsumedCapacity.CapacityUnits, nil
}

func (req *ScanRequest) Count(db *DBClient) (count int, scount int, consumed float32, err error) {
	return req.CountWithDelay(db, 0)
}

func (req *ScanRequest) CountWithDelay(db *DBClient, delay time.Duration) (count int, scount int, consumed float32, err error) {
	var res QueryResult

	creq := *req
	creq.Select = SELECT_COUNT

	for {
		res.LastEvaluatedKey = nil

		if err = db.Query("Scan", &creq).Decode(&res); err != nil {
			break
		}

		count += res.Count
		scount += res.ScannedCount
		consumed += res.ConsumedCapacity.CapacityUnits

		if res.LastEvaluatedKey == nil {
			break
		}

		creq.ExclusiveStartKey = make(AttributeNameValue)
		for k, v := range res.LastEvaluatedKey {
			creq.ExclusiveStartKey[k] = v
		}

		if delay > 0 {
			time.Sleep(delay)
		}
	}

	return
}
