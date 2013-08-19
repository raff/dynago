package dynago

var (
	RETURN_CONSUMED = map[bool]string{true: "TOTAL", false: "NONE"}
)

type ConsumedCapacityDescription struct {
	CapacityUnits float32
	TableName     string
}

type KeyValue struct {
	KeyName  string
	KeyValue interface{}
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
		getReq.Key = map[string]AttributeValue{hashKey.KeyName: MakeAttributeValue(hashKey.KeyValue)}
	}
	if rangeKey != nil {
		getReq.Key[rangeKey.KeyName] = MakeAttributeValue(rangeKey.KeyValue)
	}

	var getRes GetItemResult

	if err := db.Query("GetItem", getReq).Decode(&getRes); err != nil {
		return nil, 0.0, err
	}

	return &getRes.Item, getRes.ConsumedCapacity.CapacityUnits, nil
}
