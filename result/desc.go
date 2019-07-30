package result

import (
	"huajuan_compare_table_desc/db"
	"huajuan_compare_table_desc/base"
	"huajuan_utils/log"
	"sync"
)

type MyDatabase struct {
	Tables_name [] string
	Tables_copy map[string] int
	Tables_num int
	Tables_info sync.Map
}

// 获取所有表
func (this *MyDatabase) GetTables(connect_type string) {
	log.Instance().Info("get all tables")
	this.Tables_name = db.GetTables(connect_type)
	this.Tables_num = len(this.Tables_name)

	this.Tables_copy = make(map[string] int)

	for i := range this.Tables_name {
		this.Tables_copy[this.Tables_name[i]] = 1
	}

	log.Instance().Info("all tables %v", this.Tables_name)
}

// 获取表字段
func (this *MyDatabase) GetTableFields(connect_type string) {

	count, add_num := 0, 0

	log.Instance().Info("start get table field")

	for {
		if this.Tables_num - count > base.Max_cap {
			add_num = add_num + base.Max_cap
		} else {
			add_num = this.Tables_num
		}
		base.Wait_group.Add(add_num - count)
		for ;count < add_num; count++ {
			go db.GetTableInfo(this.Tables_name[count], connect_type, &this.Tables_info)
		}
		base.Wait_group.Wait()
		if count >= this.Tables_num {
			break
		}
	}

	log.Instance().Info("END")
}
