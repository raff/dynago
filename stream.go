package dynago

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
	return func(lsReq *ListStreamsRequest) {
		lsReq.TableName = tableName
	}
}

func LsLimit(limit int) ListStreamsOption {
	return func(lsReq *ListStreamsRequest) {
		lsReq.Limit = limit
	}
}

func LsStartItem(startItem string) ListStreamsOption {
	return func(lsReq *ListStreamsRequest) {
		lsReq.ExclusiveStartItem = startItem
	}
}

func (db *DBClient) ListStreams(options ...ListStreamsOption) ([]string, error) {
	var listReq ListStreamsRequest
	var listRes ListStreamsResult

	for _, option := range options {
		option(&listReq)
	}

	if err := db.Query("ListStreams", &listReq).Decode(&listRes); err != nil {
		return nil, err
	} else {
		return listRes.StreamIds, nil
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
	return func(descReq *DescribeStreamRequest) {
		descReq.ExclusiveStartShardId = startId
	}
}

func DsLimit(limit int) DescribeStreamOption {
	return func(descReq *DescribeStreamRequest) {
		descReq.Limit = limit
	}
}

func (db *DBClient) DescribeStream(streamId string, options ...DescribeStreamOption) (*StreamDescription, error) {
	var descReq = DescribeStreamRequest{StreamId: streamId}
	var descRes DescribeStreamResult

	for _, option := range options {
		option(&descReq)
	}

	if err := db.Query("DescribeStream", &descReq).Decode(&descRes); err != nil {
		return nil, err
	} else {
		return &descRes.StreamDescription, nil
	}
}

//////////////////////////////////////////////////////////////////////////////
//
// GetShardIterator
//

type GetShardIteratorRequest struct {
	StreamId          string
	ShardId           string
	ShardIteratorType string
	SequenceNumber    string `json:",omitempty"`
}

type GetShardIteratorResult struct {
	ShardIterator string
}

func (db *DBClient) GetShardIterator(streamId, shardId, shardIteratorType, sequenceNumber string) (string, error) {
	var siReq = GetShardIteratorRequest{
		StreamId:          streamId,
		ShardId:           shardId,
		ShardIteratorType: shardIteratorType,
		SequenceNumber:    sequenceNumber}

	var siRes GetShardIteratorResult

	if err := db.Query("GetShardIterator", &siReq).Decode(&siRes); err != nil {
		return "", err
	} else {
		return siRes.ShardIterator, nil
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
	Records           []map[string]interface{}
}

func (db *DBClient) GetRecords(shardIterator string, limit int) (*GetRecordsResult, error) {
	var rReq = GetRecordsRequest{ShardIterator: shardIterator, Limit: limit}
	var rRes GetRecordsResult

	if err := db.Query("GetRecords", &rReq).Decode(&rRes); err != nil {
		return nil, err
	} else {
		return &rRes, err
	}
}
