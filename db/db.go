package db

import (
	"errors"
	"github.com/siddontang/go-mysql/client"
	"github.com/siddontang/go-mysql/mysql"
	"huajuan_compare_table_desc/base"
	"huajuan_mysql/hjmysql/pool"
	"huajuan_utils/log"
)

var (
	hj_client *pool.PoolClient
	table_field_name = "Tables_in_"
)

func execute(connect_type, sql string) *mysql.Result {
	hj_client, _ := pool.GetNewClient(base.DBName)

	connect, err := choseClient(connect_type, hj_client) // 选择读写链接

	checkErr(err)

	result, err := connect.Execute(sql)

	checkSqlErr(err, sql)

	err = hj_client.Close()

	checkErr(err)

	return result
}

// 选择链接
func choseClient(connect_type string, hj_client *pool.PoolClient) (*client.Conn, error) {
	switch connect_type {
	case "test":
		return hj_client.GetWriteClient(), nil
	case "normal":
		return hj_client.GetReadClient(), nil
	}

	return nil, errors.New("error connecnt type")
}

func checkSqlErr(err error, sql string) {
	if(err != nil) {
		log.Instance().Debug("sql error : %s", sql)
		checkErr(err)
	}
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}