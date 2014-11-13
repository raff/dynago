package dynago

type SequenceNumberRange struct {
	EndingSequenceNumber   string
	StartingSequenceNumber string
}

type ShardDescription struct {
	ParentShardId       string
	SequenceNumberRange SequenceNumberRange
	ShardId             string
}

type StreamDescription struct {
	CreationRequestDateTime EpochTime
	KeySchema               []KeySchemaElement
	LastEvaluatedShardId    string
	Shards                  []ShardDescription
	StreamARN               string
	StreamId                string
	StreamStatus            string
	StreamViewType          string
	TableName               string
}

//////////////////////////////////////////////////////////////////////////////
//
// ListStreams
//

type ListStreamsRequest struct {
	ExclusiveStartItem string `json:",omitempty"`
	Limit              int    `json:",omitempty"`
	TableName          string `json:",omitempty"`
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
	ExclusiveStartShardId string `json:",omitempty"`
	Limit                 int    `json:",omitempty"`
	StreamId              string
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
