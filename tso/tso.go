package tso

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"go.uber.org/zap"
)

type TSO struct {
	physicalTime time.Time
	logical      uint64
}

//Saves data parsed from the checkpoint JSON string obtained from the database
type MysqlCheckPoint struct {
	ConsistentSaved bool             `toml:"consistent" json:"consistent"`
	CommitTS        uint64           `toml:"commitTS" json:"commitTS"`
	TsMap           map[string]int64 `toml:"ts-map" json:"ts-map"`
	Version         int64            `toml:"schema-version" json:"schema-version"`
}

const (
	physicalShiftBits = 18
	logicalBits       = (1 << physicalShiftBits) - 1
)

//get physicalTime from tso struct
func (tso *TSO) GetPhysicalTime() time.Time {
	return tso.physicalTime
}

//Parse physicalTime and logical from TSO(uint64)
func (tso *TSO) ParseTS(ts uint64) {
	tso.logical = ts & logicalBits
	physical := ts >> physicalShiftBits
	tso.physicalTime = time.Unix(int64(physical/1000),
		int64(physical)%1000*time.Millisecond.Nanoseconds())
}

//Get checkpoint TSO from DB
func (tso *TSO) GetTSOFromDB(ctx context.Context, conn *sql.Conn, log *zap.Logger) (uint64, error) {
	query := "select checkPoint  from tidb_binlog.checkpoint; "
	var strCheckPoint string
	var err error
	var rows *sql.Rows
	var mysqlCheckPoint = new(MysqlCheckPoint)

	//get checkpoint string from tidb
	rows, err = conn.QueryContext(ctx, query)
	defer func() {
		if rows != nil {
			res := rows.Close()
			if res != nil {
				log.Error("close row error ," + res.Error())
			}
		}
	}()
	if err != nil {
		log.Error("query checkpoint tso fail ," + err.Error())
		return 0, err
	}
	for rows.Next() {
		err = rows.Scan(&strCheckPoint)
		if err != nil {
			log.Error("Scan failed,err:" + err.Error())
			return 0, err
		}
		log.Info("get checkpoint from db success ," + strCheckPoint)
	}

	//get checkpoint from json string
	if err = json.Unmarshal([]byte(strCheckPoint), mysqlCheckPoint); err != nil {
		log.Error("unmarshal checkpoint fail," + err.Error())
		return 0, err
	}

	return mysqlCheckPoint.CommitTS, nil
}
