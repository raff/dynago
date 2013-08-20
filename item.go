package dynago

const (
	SELECT_ALL = "ALL_ATTRIBUTES"
	SELECT_ALL_PROJECTED = "ALL_PROJECTED_ATTRIBUTES"
	SELECT_SPECIFIC = "SPECIFIC_ATTRIBUTES"
	SELECT_COUNT = "COUNT"
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
		getReq.Key = MakeAttribute(hashKey.Key, hashKey.Value)
	}
	if rangeKey != nil {
		name := rangeKey.Key.AttributeName
		value := MakeAttribute(rangeKey.Key, rangeKey.Value)
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
	TableName string
	AttributesToGet []string
	ExclusiveStartKey map[string]AttributeValue
	ScanFilter map[string]Condition
	Limit int
	Segment int
	TotalSegments int
	Select string
	ReturnConsumedCapacity string
}
