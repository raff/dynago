package dynago

import (
	"fmt"
	"strconv"
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

type AttributeValue map[string]interface{}
type AttributeNameValue map[string]AttributeValue

func MakeAttributeValue(value interface{}) AttributeValue {
	switch v := value.(type) {
	case string:
		return AttributeValue{"S": v}

	case []string:
		return AttributeValue{"SS": v}

	case bool:
		return AttributeValue{"N": BOOLEAN_VALUES[v]}

	case uint, uint8, uint32, uint64:
		return AttributeValue{"N": strconv.FormatUint(v.(uint64), 10)}

	case int, int8, int32, int64:
		return AttributeValue{"N": strconv.FormatInt(v.(int64), 10)}

	case float32:
		return AttributeValue{"N": strconv.FormatFloat(float64(v), 'f', 10, 32)}

	case []float32:
		vv := make([]string, 0, len(v))
		for i, n := range v {
			vv[i] = strconv.FormatFloat(float64(n), 'f', 10, 32)
		}
		return AttributeValue{"NN": vv}

	case []float64:
		vv := make([]string, 0, len(v))
		for i, n := range v {
			vv[i] = strconv.FormatFloat(n, 'f', 10, 64)
		}
		return AttributeValue{"NN": vv}

	default:
		return AttributeValue{}
	}
}

func MakeAttribute(attr AttributeDefinition, value interface{}) AttributeNameValue {
	v := ""

	switch attr.AttributeType {
	case STRING_ATTRIBUTE:
		v = fmt.Sprintf("%v", value)

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
