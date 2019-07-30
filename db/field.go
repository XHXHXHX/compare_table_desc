package db

import (
	"fmt"
	"huajuan_compare_table_desc/base"
	"github.com/siddontang/go-mysql/mysql"
	"huajuan_utils/log"
	"sync"
)

var (
	sql  = "SHOW CREATE TABLE %s"
)

func GetTableInfo(table_name string, connect_type string, table *sync.Map) {

	tmp_sql := fmt.Sprintf(sql, table_name)

	log.Instance().Info("get %s's field", string(table_name))

	res := execute(connect_type, tmp_sql)

	var tableInfo base.TableInfo

	for i := range res.Resultset.Values {
		tableInfo = autoReflectField(res, i)
	}

	tableInfo.Name = table_name

	table.Store(tableInfo.Name, tableInfo)
	log.Instance().Info("get %s's table info over", tableInfo.Name)
	base.Wait_group.Done()
}

func autoReflectField(res *mysql.Result, i int) base.TableInfo {
	sql, _ := res.GetStringByName(i, "Create Table")

	return base.ProcessingString(sql)
}

