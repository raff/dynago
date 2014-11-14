package dynago

const (
	AT_SEQUENCE    = "AT_SEQUENCE_NUMBER"
	AFTER_SEQUENCE = "AFTER_SEQUENCE_NUMBER"
	LAST           = "TRIM_HORIZON"
	LATEST         = "LATEST"
)

type SequenceNumberRange struct {
	StartingSequenceNumber string
	EndingSequenceNumber   string
}

type ShardDescription struct {
	ShardId             string
	ParentShardId       string
	SequenceNumberRange SequenceNumberRange
}

type StreamDescription struct {
	TableName               string
	KeySchema               []KeySchemaElement
	CreationRequestDateTime EpochTime
	StreamARN               string
	StreamId                string
	StreamStatus            string
	StreamViewType          string
	LastEvaluatedShardId    string
	Shards                  []ShardDescription
}

type StreamRecord struct {
	Keys           Item
	NewImage       Item
	OldImage       Item
	SequenceNumber string
	SizeBytes      int64
	StreamViewType string
}

type Record struct {
	AwsRegion    string       `json:"awsRegion"`
	Dynamodb     StreamRecord `json:"dynamodb"`
	EventID      string       `json:"eventID"`
	EventName    string       `json:"eventName"`
	EventSource  string       `json:"eventSource"`
	EventVersion string       `json:"eventVersion"`
}

//////////////////////////////////////////////////////////////////////////////
//
// ListStreams
//

type ListStreamsRequest struct {
	TableName          string `json:",omitempty"`
	Limit              int    `json:",omitempty"`
	ExclusiveStartItem string `json:",omitempty"`
}

type ListStreamsResult struct {
	LastEvaluatedStreamId string
	StreamIds             []string
}

type ListStreamsOption func(*ListStreamsRequest)

func LsTable(tableName string) ListStreamsOption {
	return func(req *ListStreamsRequest) {
		req.TableName = tableName
	}
}

func LsLimit(limit int) ListStreamsOption {
	return func(req *ListStreamsRequest) {
		req.Limit = limit
	}
}

func LsStartItem(startItem string) ListStreamsOption {
	return func(req *ListStreamsRequest) {
		req.ExclusiveStartItem = startItem
	}
}

func (db *DBClient) ListStreams(options ...ListStreamsOption) ([]string, error) {
	var req ListStreamsRequest
	var res ListStreamsResult

	for _, option := range options {
		option(&req)
	}

	if err := db.Query("ListStreams", &req).Decode(&res); err != nil {
		return nil, err
	} else {
		return res.StreamIds, nil
	}
}

//////////////////////////////////////////////////////////////////////////////
//
// DescribeStream
//

type DescribeStreamRequest struct {
	StreamId              string
	Limit                 int    `json:",omitempty"`
	ExclusiveStartShardId string `json:",omitempty"`
}

type DescribeStreamResult struct {
	StreamDescription StreamDescription
}

type DescribeStreamOption func(*DescribeStreamRequest)

func DsStart(startId string) DescribeStreamOption {
	return func(req *DescribeStreamRequest) {
		req.ExclusiveStartShardId = startId
	}
}

func DsLimit(limit int) DescribeStreamOption {
	return func(req *DescribeStreamRequest) {
		req.Limit = limit
	}
}

func (db *DBClient) DescribeStream(streamId string, options ...DescribeStreamOption) (*StreamDescription, error) {
	var req = DescribeStreamRequest{StreamId: streamId}
	var res DescribeStreamResult

	for _, option := range options {
		option(&req)
	}

	if err := db.Query("DescribeStream", &req).Decode(&res); err != nil {
		return nil, err
	} else {
		return &res.StreamDescription, nil
	}
}

//////////////////////////////////////////////////////////////////////////////
//
// GetShardIterator
//

type GetShardIteratorRequest struct {
	StreamId          string
	ShardId           string
	ShardIteratorType string // TRIM_HORIZON | LATEST | AT_SEQUENCE_NUMBER | AFTER_SEQUENCE_NUMBER
	SequenceNumber    string `json:",omitempty"`
}

type GetShardIteratorResult struct {
	ShardIterator string
}

func (db *DBClient) GetShardIterator(streamId, shardId, shardIteratorType, sequenceNumber string) (string, error) {
	var req = GetShardIteratorRequest{
		StreamId:          streamId,
		ShardId:           shardId,
		ShardIteratorType: shardIteratorType,
		SequenceNumber:    sequenceNumber}

	var res GetShardIteratorResult

	if err := db.Query("GetShardIterator", &req).Decode(&res); err != nil {
		return "", err
	} else {
		return res.ShardIterator, nil
	}
}

//////////////////////////////////////////////////////////////////////////////
//
// GetRecords
//

type GetRecordsRequest struct {
	ShardIterator string
	Limit         int `json:",omitempty"`
}

type GetRecordsResult struct {
	NextShardIterator string
	Records           []Record
}

func (db *DBClient) GetRecords(shardIterator string, limit int) (*GetRecordsResult, error) {
	var req = GetRecordsRequest{ShardIterator: shardIterator, Limit: limit}
	var res GetRecordsResult

	if err := db.Query("GetRecords", &req).Decode(&res); err != nil {
		return nil, err
	} else {
		return &res, err
	}
}
