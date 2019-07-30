package result

import (
	"huajuan_compare_table_desc/base"
	"huajuan_compare_table_desc/compare"
)

func Run() (compare.DiffTables) {
	var TestDatabase MyDatabase
	var NormalDatabase MyDatabase

	TestDatabase.GetTables("test")
	NormalDatabase.GetTables("normal")

	if TestDatabase.Tables_num == 0 && NormalDatabase.Tables_num == 0 {
		panic(`Are you kiding me ?`)
	}

	TestDatabase.GetTableFields("test")
	NormalDatabase.GetTableFields("normal")

	TestDatabase.Tables_info.Range(func(table_name, value interface{}) bool {
		testInfo := value.(base.TableInfo)

		normalInfoInterface, ok := NormalDatabase.Tables_info.Load(table_name)

		if !ok {
			compare.AppendNeedAddTables(testInfo)
		}

		normalInfo := normalInfoInterface.(base.TableInfo)

		base.Wait_group.Add(1)
		go compare.CompareField(testInfo, normalInfo)

		delete(NormalDatabase.Tables_copy, table_name.(string))

		return true
	})

	if len(NormalDatabase.Tables_copy) > 0 {
		for table_name := range NormalDatabase.Tables_copy {
			compare.AppendNeedAbandonTables(table_name)
		}
	}

	base.Wait_group.Wait()

	return compare.DiffTable
}
