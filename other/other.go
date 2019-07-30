package other

import (
"errors"
"huajuan_mysql/hjmysql/pool"
"github.com/siddontang/go-mysql/client"
"github.com/siddontang/go-mysql/mysql"
. "huajuan_compare_table_desc/base"
)

var (
	hj_client *pool.PoolClient
	table_field_name = "Tables_in_"
)

func execute(connect_type, sql string) *mysql.Result {
	hj_client, _ := pool.GetNewClient(DBName)

	connect, err := choseClient(connect_type, hj_client) // 选择读写链接

	checkErr(err)

	result, err := connect.Execute(sql)

	checkErr(err)

	err = hj_client.Close()

	checkErr(err)

	return result
}

func Execute(connect_type, sql string) *mysql.Result {
	hj_client, _ := pool.GetNewClient(DBName)

	connect, err := choseClient(connect_type, hj_client) // 选择读写链接

	checkErr(err)

	result, err := connect.Execute(sql)

	checkErr(err)

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

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}
