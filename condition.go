package dynago

type Condition struct {
	ComparisonOperator string
	AttributeValueList []AttributeValue
}

type AttrCondition map[string]Condition

var (
	NO_CONDITION = AttrCondition{}
	NO_VALUE     = []AttributeValue{}
)

func MakeCondition(op, typ string, values ...string) Condition {
	avalues := make([]AttributeValue, len(values))
	for i, v := range values {
		avalues[i] = AttributeValue{typ: v}
	}
	return Condition{op, avalues}
}

func EQ(v AttributeValue) Condition {
	return Condition{"EQ", []AttributeValue{v}}
}

func NE(v AttributeValue) Condition {
	return Condition{"NE", []AttributeValue{v}}
}

func LE(v AttributeValue) Condition {
	return Condition{"LE", []AttributeValue{v}}
}

func LT(v AttributeValue) Condition {
	return Condition{"LT", []AttributeValue{v}}
}

func GE(v AttributeValue) Condition {
	return Condition{"GE", []AttributeValue{v}}
}

func GT(v AttributeValue) Condition {
	return Condition{"GT", []AttributeValue{v}}
}

func NOT_NULL() Condition {
	return Condition{"NOT_NULL", NO_VALUE}
}

func NULL() Condition {
	return Condition{"NULL", NO_VALUE}
}

func CONTAINS(v AttributeValue) Condition {
	return Condition{"CONTAINS", []AttributeValue{v}}
}

func NOT_CONTAINS(v AttributeValue) Condition {
	return Condition{"NOT_CONTAINS", []AttributeValue{v}}
}

func BEGINS_WITH(v AttributeValue) Condition {
	return Condition{"BEGINS_WITH", []AttributeValue{v}}
}

func IN(v []AttributeValue) Condition {
	return Condition{"IN", v}
}

func BETWEEN(v1, v2 AttributeValue) Condition {
	return Condition{"BETWEEN", []AttributeValue{v1, v2}}
}

func (attr *AttributeDefinition) EQ(value interface{}) AttrCondition {
	return AttrCondition{attr.AttributeName: EQ(EncodeAttributeValue(*attr, value))}
}

func (attr *AttributeDefinition) NE(value interface{}) AttrCondition {
	return AttrCondition{attr.AttributeName: NE(EncodeAttributeValue(*attr, value))}
}

func (attr *AttributeDefinition) LE(value interface{}) AttrCondition {
	return AttrCondition{attr.AttributeName: LE(EncodeAttributeValue(*attr, value))}
}

func (attr *AttributeDefinition) LT(value interface{}) AttrCondition {
	return AttrCondition{attr.AttributeName: LT(EncodeAttributeValue(*attr, value))}
}

func (attr *AttributeDefinition) GE(value interface{}) AttrCondition {
	return AttrCondition{attr.AttributeName: GE(EncodeAttributeValue(*attr, value))}
}

func (attr *AttributeDefinition) GT(value interface{}) AttrCondition {
	return AttrCondition{attr.AttributeName: GT(EncodeAttributeValue(*attr, value))}
}

func (attr *AttributeDefinition) NOT_NULL() AttrCondition {
	return AttrCondition{attr.AttributeName: NOT_NULL()}
}

func (attr *AttributeDefinition) NULL() AttrCondition {
	return AttrCondition{attr.AttributeName: NULL()}
}

func (attr *AttributeDefinition) CONTAINS(value interface{}) AttrCondition {
	return AttrCondition{attr.AttributeName: CONTAINS(EncodeAttributeValue(*attr, value))}
}

func (attr *AttributeDefinition) NOT_CONTAINS(value interface{}) AttrCondition {
	return AttrCondition{attr.AttributeName: NOT_CONTAINS(EncodeAttributeValue(*attr, value))}
}

func (attr *AttributeDefinition) BEGINS_WITH(value interface{}) AttrCondition {
	return AttrCondition{attr.AttributeName: BEGINS_WITH(EncodeAttributeValue(*attr, value))}
}

func (attr *AttributeDefinition) IN(values []interface{}) AttrCondition {
	return AttrCondition{attr.AttributeName: IN(EncodeAttributeValues(*attr, values))}
}

func (attr *AttributeDefinition) BETWEEN(value1, value2 interface{}) AttrCondition {
	return AttrCondition{attr.AttributeName: BETWEEN(EncodeAttributeValue(*attr, value1), EncodeAttributeValue(*attr, value2))}
}

func (attr *AttributeDefinition) Condition(op string, values ...interface{}) AttrCondition {
	return AttrCondition{attr.AttributeName: Condition{op, EncodeAttributeValues(*attr, values...)}}
}
