package dynago

import (
	"fmt"
	"strconv"
	"strings"
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

// DBItems are encoded as maps of "name": { "type": "value" }
type DBItem map[string]AttributeValue

// Encode a value according to its type
func EncodeValue(value interface{}) AttributeValue {
	switch v := value.(type) {
	case string:
		return AttributeValue{STRING_ATTRIBUTE: v}

	case []string:
		return AttributeValue{STRING_SET_ATTRIBUTE: v}

	case bool:
		return AttributeValue{NUMBER_ATTRIBUTE: BOOLEAN_VALUES[v]}

	case uint, uint8, uint32, uint64, int, int8, int32, int64:
		return AttributeValue{NUMBER_ATTRIBUTE: fmt.Sprintf("%d", v)}

	case float32:
		return AttributeValue{NUMBER_ATTRIBUTE: fmt.Sprintf("%f", v)}

	case []float32:
		vv := make([]string, len(v))
		for i, n := range v {
			vv[i] = fmt.Sprintf("%f", n)
		}
		return AttributeValue{NUMBER_SET_ATTRIBUTE: vv}

	case []float64:
		vv := make([]string, len(v))
		for i, n := range v {
			vv[i] = fmt.Sprintf("%f", n)
		}
		return AttributeValue{NUMBER_SET_ATTRIBUTE: vv}

	default:
		return AttributeValue{}
	}
}

func DecodeValue(attrValue AttributeValue) interface{} {
	if len(attrValue) != 1 {
		// panic
	}

	for k, v := range attrValue {
		switch k {
		case STRING_ATTRIBUTE:
			return v.(string)

		case STRING_SET_ATTRIBUTE:
			return v.([]string)

		case NUMBER_ATTRIBUTE:
			s := v.(string)
			if strings.Contains(s, ".") {
				f, _ := strconv.ParseFloat(s, 32)
				return float32(f)
			} else {
				i, _ := strconv.Atoi(s)
				return i
			}

		case NUMBER_SET_ATTRIBUTE:
			ss := v.([]string)
			ff := make([]float32, len(ss))
			for i, n := range ss {
				f, _ := strconv.ParseFloat(n, 32)
				ff[i] = float32(f)
			}
			return ff
		}
	}

	return nil
}

// Encode a value according to the attribute type
func EncodeAttributeValue(attr AttributeDefinition, value interface{}) AttributeValue {
	if value == nil {
		return AttributeValue{attr.AttributeType: nil}
	} else if s, ok := value.(string); ok && s == "" {
		return AttributeValue{attr.AttributeType: nil}
	}

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

	case NUMBER_SET_ATTRIBUTE:
		switch value := value.(type) {
		case []string:
			v = value

		case []int:
			av := make([]string, len(value))
			for i, n := range value {
				av[i] = fmt.Sprintf("%v", n)
			}
			v = av

		case []float32:
			av := make([]string, len(value))
			for i, n := range value {
				av[i] = fmt.Sprintf("%f", n)
			}
			v = av

		case []float64:
			av := make([]string, len(value))
			for i, n := range value {
				av[i] = fmt.Sprintf("%f", n)
			}
			v = av
		}
	}

	return AttributeValue{attr.AttributeType: v}
}

func EncodeAttributeValues(attr AttributeDefinition, values ...interface{}) []AttributeValue {

	result := make([]AttributeValue, len(values))

	for i, v := range values {
		result[i] = EncodeAttributeValue(attr, v)
	}

	return result
}

// Encode an attribute with its value
func EncodeAttribute(attr AttributeDefinition, value interface{}) AttributeNameValue {
	return AttributeNameValue{attr.AttributeName: EncodeAttributeValue(attr, value)}
}

// Encode a user item (map of name/values) into a DynamoDB item
func EncodeItem(item map[string]interface{}) DBItem {
	result := make(DBItem)

	for k, v := range item {
		if v != nil {
			result[k] = EncodeValue(v)
		}
	}

	return result
}

func DecodeItem(item DBItem) map[string]interface{} {
	result := make(map[string]interface{})

	for k, v := range item {
		result[k] = DecodeValue(v)
	}

	return result
}
