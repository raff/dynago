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
	AttributeValueList []AttributeValue
	ComparisonOperator string
}

type ConditionFunc func(...AttributeValue) Condition

func EQ(v AttributeValue) Condition {
	return Condition{[]AttributeValue{v}, "EQ"}
}

func NE(v AttributeValue) Condition {
	return Condition{[]AttributeValue{v}, "NE"}
}

func LE(v AttributeValue) Condition {
	return Condition{[]AttributeValue{v}, "LE"}
}

func LT(v AttributeValue) Condition {
	return Condition{[]AttributeValue{v}, "LT"}
}

func GE(v AttributeValue) Condition {
	return Condition{[]AttributeValue{v}, "GE"}
}

func GT(v AttributeValue) Condition {
	return Condition{[]AttributeValue{v}, "GT"}
}

func NOT_NULL() Condition {
	return Condition{[]AttributeValue{}, "NOT_NULL"}
}

func NULL() Condition {
	return Condition{[]AttributeValue{}, "NULL"}
}

func CONTAINS(v AttributeValue) Condition {
	return Condition{[]AttributeValue{v}, "CONTAINS"}
}

func NOT_CONTAINS(v AttributeValue) Condition {
	return Condition{[]AttributeValue{v}, "NOT_CONTAINS"}
}

func BEGINS_WITH(v AttributeValue) Condition {
	return Condition{[]AttributeValue{v}, "BEGINS_WITH"}
}

func IN(v []AttributeValue) Condition {
	return Condition{v, "IN"}
}

func BETWEEN(v1, v2 AttributeValue) Condition {
	return Condition{[]AttributeValue{v1, v2}, "BETWEEN"}
}
