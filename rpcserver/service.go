package rpcserver

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	pb "github.com/fandypeng/e2cdatabus/proto"
	"github.com/go-redis/redis"
	"github.com/jmoiron/sqlx"
	"regexp"
	"strconv"
	"time"
)

type Service struct {
	redis *redis.Client
	db    *sqlx.DB
}

// NewService return a DatabusServer
func NewService() *Service {
	return &Service{}
}

// SetRedisConnect setup redis client
// addr example: "127.0.0.1:6379"
func (s *Service) SetRedisConnect(addr, password string) error {
	if len(addr) == 0 {
		return errors.New("error mysql dsn")
	}
	s.redis = redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		PoolSize: 100,
	})
	stat := s.redis.Ping()
	return stat.Err()
}

// SetMyqlConnect setup mysql client
// mysqlDsn example: "username:password@tcp(172.2.1.88:3306)/dbname?charset=utf8mb4"
func (s *Service) SetMyqlConnect(dsn string) error {
	if len(dsn) == 0 {
		return errors.New("error mysql dsn")
	}
	s.db = sqlx.MustConnect("mysql", dsn)
	s.db.SetMaxOpenConns(100)
	s.db.SetMaxIdleConns(10)
	err := s.db.Ping()
	return err
}

func (s *Service) UpdateConfig(ctx context.Context, req *pb.UpdateConfigReq) (resp *pb.UpdateConfigResp, err error) {
	resp = &pb.UpdateConfigResp{
		Status: 0,
		ErrMsg: "",
	}
	if s.db != nil {
		err = s.db.Ping()
		if err != nil {
			return
		}
		testTableName := req.GetName() + "_" + strconv.Itoa(int(time.Now().Unix()))
		err = s.exportTableToMysql(ctx, s.db, req, testTableName)
		if err == nil {
			err = s.exportTableToMysql(ctx, s.db, req, req.GetName())
		}
		if err == nil {
			tx, txerr := s.db.Begin()
			if txerr != nil {
				err = txerr
				return
			}
			s.dropTable(tx, testTableName)
			err = tx.Commit()
		}
		if err != nil {
			return
		}
	} else if s.redis != nil {
		_, err = s.redis.Ping().Result()
		if err != nil {
			return
		}
		err = s.redis.Set(req.Name, req.Content, 0).Err()
	}
	return
}

func (s *Service) GetConfig(ctx context.Context, req *pb.GetConfigReq) (resp *pb.GetConfigResp, err error) {
	resp = &pb.GetConfigResp{}
	if s.db != nil {
		err = s.db.Ping()
		if err != nil {
			return
		}
		res := make([]interface{}, 0)
		rows, connErr := s.db.Unsafe().Query("select * from " + req.Name)
		reg, _ := regexp.Compile(`Table.*?doesn't exist`)
		if connErr != nil && reg.Match([]byte(connErr.Error())) {
			err = nil
			return
		}
		if connErr == nil {
			cols, _ := rows.Columns()
			for rows.Next() {
				var row = make([]interface{}, len(cols))
				var rowp = make([]interface{}, len(cols))
				for i, _ := range row {
					rowp[i] = &row[i]
				}
				connErr = rows.Scan(rowp...)
				if connErr != nil {
					break
				}
				data := make(map[string]interface{})
				for i := 0; i < len(cols); i++ {
					columnName := cols[i]
					columnValue := *rowp[i].(*interface{})
					strval := string(columnValue.([]byte))
					data[columnName] = strval
					if intval, err := strconv.Atoi(strval); err == nil {
						data[columnName] = intval
					}
				}
				res = append(res, data)
			}
		}
		if connErr == nil {
			connErr = rows.Err()
		}
		if connErr != nil && connErr != sql.ErrNoRows {
			err = connErr
			return
		}
		bytes, _ := json.Marshal(res)
		resp.Content = string(bytes)
	} else if s.redis != nil {
		_, err = s.redis.Ping().Result()
		if err != nil {
			return
		}
		cmd := s.redis.Get(req.Name)
		err = cmd.Err()
		resp.Content = cmd.Val()
	}
	return
}

func (s *Service) SayHello(ctx context.Context, req *pb.SayHelloReq) (resp *pb.SayHelloResp, err error) {
	resp = &pb.SayHelloResp{Response: "server response to: " + req.Greet}
	return
}

func (s *Service) exportTableToMysql(ctx context.Context, db *sqlx.DB, formatInfo *pb.UpdateConfigReq, tableName string) (err error) {
	tx, err := db.Begin()
	if err != nil {
		return
	}
	err = s.dropTable(tx, tableName)
	if err == nil {
		err = s.createTable(tx, formatInfo, tableName)
	}
	if err == nil {
		err = s.insertToTable(tx, formatInfo, tableName)
	}
	if err == nil {
		err = tx.Commit()
	}
	if err != nil {
		tx.Rollback()
		return
	}
	return
}

func (s *Service) createTable(tx *sql.Tx, formatInfo *pb.UpdateConfigReq, tableName string) (err error) {
	createSql := "CREATE TABLE `" + tableName + "` ("
	for index, row := range formatInfo.Head.Fields {
		fieldTy := "bigint(20)"
		if formatInfo.Head.Types[index] == "string" {
			fieldTy = "text"
		}
		createSql += "`" + row + "` " + fieldTy + " NOT NULL COMMENT '" + formatInfo.Head.Descs[index] + "',"
	}
	createSql += "PRIMARY KEY (`sid`) ) DEFAULT CHARSET=utf8mb4"
	_, err = tx.Exec(createSql)
	return
}

func (s *Service) insertToTable(tx *sql.Tx, formatInfo *pb.UpdateConfigReq, tableName string) (err error) {
	insertSql := "INSERT INTO `" + tableName + "` ("
	for index, field := range formatInfo.Head.Fields {
		insertSql += "`" + field + "`"
		if index < len(formatInfo.Head.Fields)-1 {
			insertSql += ","
		} else {
			insertSql += ")"
		}
	}
	content := make([]map[string]interface{}, 0)
	err = json.Unmarshal([]byte(formatInfo.Content), &content)
	if err != nil {
		return
	}
	insertSql += "VALUES("
	for rowIndex, row := range content {
		for index, field := range formatInfo.Head.Fields {
			var val = ""
			if index < len(row) {
				if tmp, ok := row[field]; ok {
					if tmpVal, ok := tmp.(string); !ok {
						val = strconv.FormatFloat(tmp.(float64), 'g', -1, 64)
					} else {
						val = tmpVal
					}
				}
			}
			insertSql += "'" + val + "'"
			if index < len(formatInfo.Head.Fields)-1 {
				insertSql += ","
			} else {
				insertSql += ")"
			}
		}
		if rowIndex < len(content)-1 {
			insertSql += ",("
		}
	}
	_, err = tx.Exec(insertSql)
	return
}

func (s *Service) dropTable(tx *sql.Tx, tableName string) (err error) {
	dropSql := "drop table if exists " + tableName
	_, err = tx.Exec(dropSql)
	if err != nil {
		return err
	}
	return
}
