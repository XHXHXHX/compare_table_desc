package db

import (
	"huajuan_compare_table_desc/base"
)

var (
	table_sql = "SHOW TABLES"
)

func GetTables(connect_type string) []string {

	table_field_name = table_field_name + base.DBName

	result := execute(connect_type, table_sql)

	var table []string

	for i := range result.Resultset.Values {
		table_name, _ := result.GetStringByName(i, table_field_name)
		if ok := base.ExceptTables(table_name); ok {
			continue
		}
		table = append(table, table_name)
	}

	return table
}


