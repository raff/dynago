package dynago

import "log"

type ListStreamsRequest struct {
	ExclusiveStartItem string `json:",omitempty"`
	Limit              int    `json:",omitempty"`
	TableName          string `json:",omitempty"`
}

type ListStreamsResult struct {
	LastEvaluatedStreamId string
	StreamIds             []string
}

func ListStreams() *ListStreamsRequest {
	return &ListStreamsRequest{}
}

func (lsReq *ListStreamsRequest) WithTable(tableName string) *ListStreamsRequest {
	lsReq.TableName = tableName
	return lsReq
}

func (lsReq *ListStreamsRequest) WithLimit(limit int) *ListStreamsRequest {
	lsReq.Limit = limit
	return lsReq
}

func (lsReq *ListStreamsRequest) WithStartItem(startItem string) *ListStreamsRequest {
	lsReq.ExclusiveStartItem = startItem
	return lsReq
}

func (db *DBClient) ListStreams() ([]string, error) {
	var listRes ListStreamsResult
	if err := db.Query("ListStreams", nil).Decode(&listRes); err != nil {
		return nil, err
	} else {
		return listRes.StreamIds, nil
	}
}

func (lsReq *ListStreamsRequest) Exec(db *DBClient) ([]string, error) {
	var listRes ListStreamsResult

	log.Printf("ListStreams %#v", lsReq)

	if err := db.Query("ListStreams", lsReq).Decode(&listRes); err != nil {
		return nil, err
	} else {
		return listRes.StreamIds, nil
	}
}
