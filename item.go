package dynago

const (
	SELECT_ALL           = "ALL_ATTRIBUTES"
	SELECT_ALL_PROJECTED = "ALL_PROJECTED_ATTRIBUTES"
	SELECT_SPECIFIC      = "SPECIFIC_ATTRIBUTES"
	SELECT_COUNT         = "COUNT"
)

var (
	RETURN_CONSUMED = map[bool]string{true: "TOTAL", false: "NONE"}
)

type ConsumedCapacityDescription struct {
	CapacityUnits float32
	TableName     string
}

type KeyValue struct {
	Key   AttributeDefinition
	Value interface{}
}

type ItemValues map[string]AttributeValue

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

	Item ItemValues
}

func (db *DBClient) GetItem(tableName string, hashKey *KeyValue, rangeKey *KeyValue, attributes []string, consistent bool, consumed bool) (*ItemValues, float32, error) {

	getReq := GetItemRequest{TableName: tableName, AttributesToGet: attributes, ConsistentRead: consistent, ReturnConsumedCapacity: RETURN_CONSUMED[consumed]}
	if hashKey != nil {
		getReq.Key = EncodeAttribute(hashKey.Key, hashKey.Value)
	}
	if rangeKey != nil {
		name := rangeKey.Key.AttributeName
		value := EncodeAttribute(rangeKey.Key, rangeKey.Value)
		getReq.Key[name] = value[name]
	}

	var getRes GetItemResult

	if err := db.Query("GetItem", getReq).Decode(&getRes); err != nil {
		return nil, 0.0, err
	}

	return &getRes.Item, getRes.ConsumedCapacity.CapacityUnits, nil
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
	Limit                  int	`json:",omitempty"`
	Segment                int	`json:",omitempty"`
	TotalSegments          int	`json:",omitempty"`
	Select                 string	`json:",omitempty"`
	ReturnConsumedCapacity string	`json:",omitempty"`
}

type ScanResult struct {
	Items            []ItemValues
	ConsumedCapacity ConsumedCapacityDescription
	LastEvaluatedKey AttributeNameValue
	Count            int
	ScannedCount     int
}

/*
func (db *DBClient) Scan(tableName string, hashKey *KeyValue, rangeKey *KeyValue, attributes []string, consistent bool, consumed bool) ([]ItemValues, float32, error) {
	return nil, 0.0, nil
}
*/

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

func (scanReq *ScanRequest) WithLimit(limit int) *ScanRequest {
	scanReq.Limit = limit
	return scanReq
}

func (scanReq *ScanRequest) WithSegment(segment, totalSegments int) *ScanRequest {
	scanReq.Segment = segment
	scanReq.TotalSegments = totalSegments
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

func (scanReq *ScanRequest) Exec(db *DBClient) ([]ItemValues, AttributeNameValue, float32, error) {
	var scanRes ScanResult

	if err := db.Query("Scan", scanReq).Decode(&scanRes); err != nil {
		return nil, nil, 0.0, err
	}

	return scanRes.Items, scanRes.LastEvaluatedKey, scanRes.ConsumedCapacity.CapacityUnits, nil
}
