package dynago

import (
	"fmt"
)

const (
	STRING_ATTRIBUTE     = "S"
	STRING_SET_ATTRIBUTE = "SS"
	NUMBER_ATTRIBUTE     = "N"
	NUMBER_SET_ATTRIBUTE = "NS"
	BINARY_ATTRIBUTE     = "B"
	BINARY_SET_ATTRIBUTE = "BS"
)

var (
	BOOLEAN_VALUES = map[bool]string{true: "1", false: "0"}
)

// Attribute values are encoded as { "type": "value" }
type AttributeValue map[string]interface{}

// Attributes are encoded as { "name": { "type": "value" } }
type AttributeNameValue map[string]AttributeValue

// Encode a value according to its type
func EncodeValue(value interface{}) AttributeValue {
	switch v := value.(type) {
	case string:
		return AttributeValue{"S": v}

	case []string:
		return AttributeValue{"SS": v}

	case bool:
		return AttributeValue{"N": BOOLEAN_VALUES[v]}

	case uint, uint8, uint32, uint64, int, int8, int32, int64:
		return AttributeValue{"N": fmt.Sprintf("%d", v)}

	case float32:
		return AttributeValue{"N": fmt.Sprintf("%f", v)}

	case []float32:
		vv := make([]string, len(v))
		for i, n := range v {
			vv[i] = fmt.Sprintf("%f", n)
		}
		return AttributeValue{"NN": vv}

	case []float64:
		vv := make([]string, len(v))
		for i, n := range v {
			vv[i] = fmt.Sprintf("%f", n)
		}
		return AttributeValue{"NN": vv}

	default:
		return AttributeValue{}
	}
}

// Encode an attribute with its value
func EncodeAttribute(attr AttributeDefinition, value interface{}) AttributeNameValue {
	var v interface{}

	switch attr.AttributeType {
	case STRING_ATTRIBUTE:
		v = fmt.Sprintf("%v", value)

	case STRING_SET_ATTRIBUTE:
		switch value := value.(type) {
		case []string:
			v = value
		}

	case NUMBER_ATTRIBUTE:
		switch value := value.(type) {
		case string:
			v = value

		default:
			v = fmt.Sprintf("%f", value)
		}
	}

	return AttributeNameValue{attr.AttributeName: AttributeValue{attr.AttributeType: v}}
}
