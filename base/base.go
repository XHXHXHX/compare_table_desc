package base

import (
	"fmt"
	"io/ioutil"
	"encoding/json"
	"regexp"
	"strings"
	"sync"
	"huajuan_utils/log"
)

type TableInfo struct {
	Name string			// 表名
	Comment string		// 注释
	Engine string		// 引擎
	AutoIncrement string // 自增量
	Charset string		// 字符集
	FieldsIndex [] string // 字段排序
	FieldsCopy map[string] int	// 字段拷贝
	Fields map[string] FieldInfo	// 字段
	Indexs map[string] string 	// 索引
	IndexsCopy map[string] int 	// 索引拷贝
	CreateSql string  	// 建表sql
}

type FieldInfo struct {
	Sql string
	Index int
}

type Except struct {
	Except_tables [] string
	Max_cap	int
	DBName string
	except_map map[string] int
	except_any map[string] string
}

const except_filename = "config.json"

var (
	except_tables Except
	Max_cap int
	DBName string
	Wait_group sync.WaitGroup

	reg_field_and_index = `(\(\s([\s\S]*)\s\))`
	reg_field 			= "`(.*)`"
	reg_index 			= `,\s*PRIMARY.*`
	reg_other 			= `ENGINE.*`
	reg_other_field 	= `%s=(.*?) `
	reg_enter 			= `[\n|\r]`
)

func init() {
	getExceptTable(except_filename)

	Max_cap = except_tables.Max_cap
	DBName = except_tables.DBName

	except_tables.except_map = make(map[string] int)
	except_tables.except_any = make(map[string] string)

	for i := range except_tables.Except_tables {
		tmp_name := except_tables.Except_tables[i]
		except_tables.except_map[tmp_name] = 0
		if index := strings.Index(tmp_name, "*"); index != -1 {
			except_tables.except_any[tmp_name[0:index]] = tmp_name
		}
	}

	fmt.Println("Max_cap", Max_cap)
	fmt.Println("DBName", DBName)

	log.Instance().Info("Init: %v", except_tables)
}

func getExceptTable(filename string) {
	bytes, err := ioutil.ReadFile(filename)
	CheckErr(err)

	if err := json.Unmarshal(bytes, &except_tables); err != nil {
		panic(err)
	}
}

func ExceptTables(table_name string) bool {
	// 查找全程
	if num, ok := except_tables.except_map[table_name]; ok {
		except_tables.except_map[table_name] = num + 1
		return true
	}

	for except_table_name := range except_tables.except_any {
		if index := strings.Index(table_name, except_table_name); index == 0 {
			except_tables.except_map[except_tables.except_any[except_table_name]]++
			return true
		}
	}

	return false
}

func CheckErr(err error) {
	if err != nil {
		panic(err)
	}
}

// 分析 sql
func ProcessingString(sql string) TableInfo {
	var tableInfo TableInfo
	tableInfo.Fields = make(map[string] FieldInfo)
	tableInfo.Indexs = make(map[string] string)

	tableInfo.CreateSql = sql

	field_index_string := preg_match(reg_field_and_index, sql, 1)
	index_string := preg_match(reg_index, field_index_string, 1)
	position_index := strings.Index(field_index_string, index_string)
	field_string := field_index_string[:position_index]
	index_string = strings.Trim(field_index_string[position_index + 1:], ` `)
	other_string := preg_match(reg_other, sql, 1)

	tableInfo.Fields, tableInfo.FieldsIndex, tableInfo.FieldsCopy = getFieldFromSqlString(field_string)
	tableInfo.Indexs, tableInfo.IndexsCopy = getIndexFromSqlString(index_string)
	tableInfo.Engine = preg_match_other(`ENGINE`, other_string)
	tableInfo.Charset = preg_match_other(`CHARSET`, other_string)
	tableInfo.AutoIncrement = preg_match_other(`AUTO_INCREMENT`, other_string)
	tableInfo.Comment = preg_match_other(`Comment`, other_string)

	return tableInfo
}

// 遍历字段字符串
func getFieldFromSqlString(str string) (map[string] FieldInfo, [] string, map[string] int) {
	var tmp = make(map[string] FieldInfo)
	var tmp_arr_copy = make(map[string] int)
	var tmp_arr [] string
	var fieldInfo FieldInfo
	arr := strings.Split(str, `,  `)

	for i := range arr {
		name := preg_match_field(arr[i])
		name = strings.Replace(name, "`", ``, 2)
		fieldInfo.Sql = arr[i]
		fieldInfo.Index = i + 1
		tmp[name] = fieldInfo
		tmp_arr_copy[name] = i + 1
		tmp_arr = append(tmp_arr, name)
	}

	return tmp, tmp_arr, tmp_arr_copy
}

// 遍历索引字符串 变为map   field => sql
func getIndexFromSqlString(str string) (map[string] string, map[string] int) {
	var tmp = make(map[string] string)
	var tmp_copy = make(map[string] int)
	arr := strings.Split(str, `,  `)

	for i := range arr {
		name := preg_match_field(arr[i])
		name = strings.Replace(name, "`", ``, 2)
		tmp[name] = arr[i]
		tmp_copy[name] = i + 1
	}

	return tmp, tmp_copy
}

// 正则匹配
func preg_match(reg, str string, num int) string {
	var result_string string
	re := regexp.MustCompile(reg)

	result := re.FindAllString(str, num)

	if(len(result) == 0) {
		reg_panic(reg, str, num)
	}

	result_string = strings.Trim(result[0], `(`)
	result_string = strings.Trim(result_string, `)`)

	re = regexp.MustCompile(reg_enter)
	result_string = re.ReplaceAllLiteralString(result_string, ``)
	result_string = strings.Trim(result_string, ` `)

	return result_string
}

// 获取字段名
func preg_match_field(str string) string {
	re := regexp.MustCompile(reg_field)

	result := re.FindAllString(str, 1)

	if(len(result) == 0) {
		reg_panic(reg_field, str, 1)
	}

	return strings.Replace(result[0], "`", ``, 2)
}

// 正则获取其他值  ENGINE AUTO_INCREMENT CHARSET
func preg_match_other(index, str string) string {
	var tmp [] string
	reg := fmt.Sprintf(reg_other_field, index)

	re := regexp.MustCompile(reg)

	result := re.FindAllString(str, 1)

	if(len(result) == 0) {
		return ``
	}

	tmp = strings.Split(result[0], `=`)

	return tmp[1]
}

// 正则报错
func reg_panic(reg, str string, num int) {
	panic(fmt.Sprintf(`no switch string : {reg : %s, str : %s, num : %d}`, reg, str, num))
}