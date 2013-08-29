package dynago

const (
	_EQ           = "EQ"
	_NE           = "NE"
	_LE           = "LE"
	_LT           = "LT"
	_GE           = "GE"
	_GT           = "GT"
	_BEGINS_WITH  = "BEGINS_WITH"
	_BETWEEN      = "BETWEEN"
	_NULL         = "NULL"
	_NOT_NULL     = "NOT_NULL"
	_CONTAINS     = "CONTAINS"
	_NOT_CONTAINS = "NOT_CONTAINS"
	_IN           = "IN"
)

type Condition struct {
	ComparisonOperator string
	AttributeValueList []AttributeValue
}

type AttrCondition map[string]Condition

var (
	NO_CONDITION = AttrCondition{}
)

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
	return Condition{"NOT_NULL", []AttributeValue{}}
}

func NULL() Condition {
	return Condition{"NULL", []AttributeValue{}}
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
