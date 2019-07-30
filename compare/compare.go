package compare

import (
	"fmt"
	"huajuan_compare_table_desc/base"
	"strings"
	"sync"
)

// 需要添加表
type needAddTables struct {
	tables [] string
	alter_sql sync.Map
}
// 需要弃用表
type needAbandonTables struct {
	tables [] string
	alter_sql sync.Map
}
// 需要改变字段表
type needChangeTables struct {
	tables [] string
	change sync.Map
	add sync.Map
	abandon sync.Map
}

// 需要新增字段
type needAddField struct {
	name [] string
	info map[string] needFieldInfo
}

// 需要弃用字段
type needAbandonField struct {
	name [] string
	info map[string] needFieldInfo
}

// 修改字段集
type needAlterField struct {
	name [] string
	info map[string] needAlterFieldInfo
}
// 修改字段属性
type needAlterFieldInfo struct {
	field_name string
	field_type string
	test_value string
	normal_value string
	sql string
}

type needFieldInfo struct {
	field_name string
	sql string
}

type DiffTables struct {
	need_add_tables needAddTables
	need_abandon_tables needAbandonTables
	need_change_tables needChangeTables
}

var DiffTable DiffTables
var need_add_tables *needAddTables
var need_abandon_tables *needAbandonTables
var need_change_tables *needChangeTables

func init() {
	need_add_tables =  &DiffTable.need_add_tables
	need_abandon_tables =  &DiffTable.need_abandon_tables
	need_change_tables =  &DiffTable.need_change_tables
}

func CompareField(testInfo, normalInfo base.TableInfo) {

	var num int = 0

	// 比较字段
	num = compareField(testInfo, normalInfo, num)

	// 比较索引
	num = compareIndex(testInfo, normalInfo, num)

	// 比较其他字段
	num = compareOtherField(testInfo, normalInfo, num)

	if num > 0 {
		need_change_tables.tables = append(need_change_tables.tables, testInfo.Name)
	}

	base.Wait_group.Done()
}

func AppendNeedAddTables(table base.TableInfo) {
	need_add_tables.tables = append(need_add_tables.tables, table.Name)
	need_add_tables.alter_sql.Store(table.Name, table.CreateSql)
}

func AppendNeedAbandonTables(table_name string) {
	need_abandon_tables.tables = append(need_abandon_tables.tables, table_name)
	need_abandon_tables.alter_sql.Store(table_name, fmt.Sprintf(`DROP TABLE %s`, table_name))
}

// 比较字段
func compareField(testInfo, normalInfo base.TableInfo, num int) int {

	var needAddField needAddField
	var needAlterField needAlterField
	var needAbandonField needAbandonField
	needAlterField.info = make(map[string] needAlterFieldInfo)
	needAbandonField.info = make(map[string] needFieldInfo)

	for i := range testInfo.FieldsIndex {
		field_name := testInfo.FieldsIndex[i]

		// 正式中不存在该字段
		if _, ok := normalInfo.Fields[field_name]; !ok {

			needaddFieldInfo := needFieldInfo {
				field_name,
				addFieldPosition(testInfo.Name, field_name, testInfo),
			}
			needAddField.name = append(needAddField.name, field_name)
			needAddField.info[field_name] = needaddFieldInfo
			num++

		} else if !strings.EqualFold(testInfo.Fields[field_name].Sql, normalInfo.Fields[field_name].Sql) {

			var needAlterFieldInfo needAlterFieldInfo

			needAlterFieldInfo.field_name = field_name
			needAlterFieldInfo.field_type = `field`
			needAlterFieldInfo.test_value = testInfo.Fields[field_name].Sql
			needAlterFieldInfo.normal_value = normalInfo.Fields[field_name].Sql
			needAlterFieldInfo.sql = fmt.Sprintf(`ALERT TABLE %s modify column %s`, testInfo.Name, testInfo.Fields[field_name].Sql)

			needAlterField.name = append(needAlterField.name, field_name)
			needAlterField.info[field_name] = needAlterFieldInfo
			num++

			delete(normalInfo.FieldsCopy, field_name)	// 删除 normalInfo 与 testInfo 都存在的字段  用户计算差集
		}
	}

	// 弃用字段
	if len(normalInfo.FieldsCopy) > 0 {

		for field_name := range normalInfo.FieldsCopy {
			needAbandonField.name = append(needAbandonField.name, field_name)
			needAbandonField.info[field_name] = needFieldInfo{
				field_name,
				fmt.Sprintf(`ALTER TABLE %s DROP COLUMN %s`, testInfo.Name, field_name),
			}
		}
	}

	if 0 != len(needAddField.name){
		need_change_tables.add.Store(testInfo.Name, needAddField)
	}
	if 0 != len(needAlterField.name) {
		need_change_tables.change.Store(testInfo.Name, needAlterField)
	}
	if 0 != len(needAbandonField.name) {
		need_change_tables.abandon.Store(testInfo.Name, needAbandonField)
	}

	return num
}

// 比较索引
func compareIndex(testInfo, normalInfo base.TableInfo, num int) int {

	var needAddField needAddField
	var needAlterField needAlterField
	var needAbandonField needAbandonField
	needAlterField.info = make(map[string] needAlterFieldInfo)
	needAbandonField.info = make(map[string] needFieldInfo)

	for index_name := range testInfo.Indexs {
		if _, ok := normalInfo.Indexs[index_name]; !ok {
			needaddFieldInfo := needFieldInfo {
				index_name,
				fmt.Sprintf(`ALTER TABLE %s ADD %s`, testInfo.Name, testInfo.Indexs[index_name]),
			}
			needAddField.name = append(needAddField.name, index_name)
			needAddField.info[index_name] = needaddFieldInfo
			num++
		} else if 0 != strings.Compare(testInfo.Indexs[index_name], normalInfo.Indexs[index_name]) {
			var needAlterFieldInfo needAlterFieldInfo

			needAlterFieldInfo.field_name = index_name
			needAlterFieldInfo.field_type = `index`
			needAlterFieldInfo.test_value = testInfo.Indexs[index_name]
			needAlterFieldInfo.normal_value = normalInfo.Indexs[index_name]
			needAlterFieldInfo.sql = fmt.Sprintf(`DROP INDEX %s ON %s; ALERT TABLE %s modify column %s`, index_name, testInfo.Name, testInfo.Name, testInfo.Fields[index_name])

			needAlterField.name = append(needAlterField.name, index_name)
			needAlterField.info[index_name] = needAlterFieldInfo
			num++

			delete(normalInfo.IndexsCopy, index_name)  // 删除 normalInfo 与 testInfo 都存在的字段  用户计算差集
		}
	}

	if len(normalInfo.IndexsCopy) > 0 {
		for index_name := range normalInfo.IndexsCopy {
			needAbandonField.name = append(needAbandonField.name, index_name)
			needAbandonField.info[index_name] = needFieldInfo{
				index_name,
				fmt.Sprintf(`DROP INDEX %s ON %s`, index_name, testInfo.Name),
			}
		}
	}

	if 0 != len(needAddField.name){
		need_change_tables.add.Store(testInfo.Name, needAddField)
	}
	if 0 != len(needAlterField.name) {
		need_change_tables.change.Store(testInfo.Name, needAlterField)
	}
	if 0 != len(needAbandonField.name) {
		need_change_tables.abandon.Store(testInfo.Name, needAbandonField)
	}

	return num
}

// 比较其他字段
func compareOtherField(testInfo, normalInfo base.TableInfo, num int) int {
	// 注释
	if !strings.EqualFold(testInfo.Comment, normalInfo.Comment) {
		num++
		setAlterForOtherField(testInfo, normalInfo, `Comment`)
	}
	// 引擎
	if 0 != strings.Compare(testInfo.Engine, normalInfo.Engine) {
		num++
		setAlterForOtherField(testInfo, normalInfo, `Engine`)
	}
	// 自增量
	if 0 != strings.Compare(testInfo.AutoIncrement, normalInfo.AutoIncrement) {
		num++
		setAlterForOtherField(testInfo, normalInfo, `AutoIncrement`)
	}
	// 字符集
	if 0 != strings.Compare(testInfo.Charset, normalInfo.Charset) {
		num++
		setAlterForOtherField(testInfo, normalInfo, `Charset`)
	}

	return num
}

// 找寻位置
func addFieldPosition(table_name, field_name string, table_info base.TableInfo) string {

	var position string

	field_info := table_info.Fields[field_name]

	if field_info.Index == 1 {
		position = `FIRST`
	} else {
		before_field := table_info.FieldsIndex[field_info.Index - 1]
		position = fmt.Sprintf("AFTER `%s`", before_field)
	}

	return fmt.Sprintf(`ALTER TABLE %s add %s %s`, table_name, field_info.Sql, position)
}

func setAlterForOtherField(testInfo, normalInfo base.TableInfo, field_name string) {
	var needAlterField needAlterField
	needAlterField.info = make(map[string] needAlterFieldInfo)
	var needAlterFieldInfo needAlterFieldInfo

	needAlterFieldInfo.field_name = field_name
	needAlterFieldInfo.field_type = `other`

	switch field_name {
		case `Comment`:
			needAlterFieldInfo.test_value = testInfo.Comment
			needAlterFieldInfo.normal_value = normalInfo.Comment
			needAlterFieldInfo.sql = fmt.Sprintf(`ALTER TABLE %s Comment '%s'`, testInfo.Name, testInfo.Comment)
		case `Engine`:
			needAlterFieldInfo.test_value = testInfo.Engine
			needAlterFieldInfo.normal_value = normalInfo.Engine
			needAlterFieldInfo.sql = fmt.Sprintf(`ALTER TABLE %s Engine '%s'`, testInfo.Name, testInfo.Engine)
		case `AutoIncrement`:
			needAlterFieldInfo.test_value = testInfo.AutoIncrement
			needAlterFieldInfo.normal_value = normalInfo.AutoIncrement
			needAlterFieldInfo.sql = fmt.Sprintf(`ALTER TABLE %s AutoIncrement %s`, testInfo.Name, testInfo.AutoIncrement)
		case `Charset`:
			needAlterFieldInfo.test_value = testInfo.Charset
			needAlterFieldInfo.normal_value = normalInfo.Charset
			needAlterFieldInfo.sql = fmt.Sprintf(`ALTER TABLE %s DEFAULT CHARACTER SET '%s'`, testInfo.Name, testInfo.Charset)
	}

	needAlterField.name = append(needAlterField.name, field_name)
	needAlterField.info[field_name] = needAlterFieldInfo
}
