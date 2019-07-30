package main

import (
	"fmt"
	"huajuan_compare_table_desc/result"
	"runtime/debug"
	"huajuan_utils/log"
)

func main() {
	defer MainPanicHandler()

	res := result.Run()

	fmt.Println(res)

}



func MainPanicHandler() {
	if err := recover(); err != nil {
		log.Instance().Error("main panic: %v", err)
		log.Instance().Error("main panic: debug statck %s", string(debug.Stack()))
	}
}
