package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/util"
	"os"
	"strings"
)

var (
	h    bool
	path string
)

func keys(db *leveldb.DB, cmd_args []string) {

	var iter iterator.Iterator

	if len(cmd_args) == 0 || cmd_args[0] == "*" {
		//查询全部
		iter = db.NewIterator(nil, nil)
	} else {
		//根据通配符查询
		iter = db.NewIterator(util.BytesPrefix([]byte(cmd_args[0])), nil)
	}
	defer iter.Release()

	for iter.Next() {
		key := iter.Key()
		fmt.Println(string(key))
	}
}

func get(db *leveldb.DB, cmd_args []string) {
	if len(cmd_args) == 0 {
		fmt.Println("key required")
		return
	}

	key := cmd_args[0]
	exist, err := db.Has([]byte(key), nil)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	if !exist {
		fmt.Println("key not found")
		return
	}
	value, err := db.Get([]byte(key), nil)
	if err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Println(string(value))
	}
}

func set(db *leveldb.DB, cmd_args []string) {
	if len(cmd_args) < 2 {
		fmt.Println("parameter error")
		return
	}

	key := cmd_args[0]
	value := cmd_args[1]

	err := db.Put([]byte(key), []byte(value), nil)
	if err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Println("OK")
	}
}

func delete(db *leveldb.DB, cmd_args []string) {
	if len(cmd_args) == 0 {
		fmt.Println("key required")
		return
	}

	key := cmd_args[0]

	err := db.Delete([]byte(key), nil)
	if err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Println("OK")
	}
}

func exist(db *leveldb.DB, cmd_args []string) {
	if len(cmd_args) == 0 {
		fmt.Println("key required")
		return
	}

	key := cmd_args[0]

	exist, err := db.Has([]byte(key), nil)
	if err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Println(exist)
	}
}

func init() {
	flag.BoolVar(&h, "h", false, "leveldb-cli usage")
	flag.StringVar(&path, "p", "", "leveldb database absolute path")
}

/**
leveldb 命令行工具
*/
func main() {
	flag.Parse()

	if h {
		flag.Usage()
		return
	}

	if len(path) == 0 {
		flag.Usage()
		os.Exit(0)
	}

	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	defer db.Close()

	fmt.Println("Welcome to the leveldb cli!")
	fmt.Println("Enter '?' for a list of commands.")

	buf := bufio.NewReader(os.Stdin)
	for {

		fmt.Print("> ")

		read, err := buf.ReadString('\n')
		if err != nil {
			println()
			break
		}

		line := strings.Trim(read[0:len(read)-1], " ")
		if len(line) == 0 {
			continue
		}

		args := strings.Split(line, " ")
		if len(args) == 0 {
			continue
		}
		cmd := args[0]
		cmd_args := args[1:]

		cmd = strings.ToUpper(cmd)

		switch cmd {
		case "?":
			fmt.Println("Symbol Commands:")
			fmt.Println("\t?                \thelp menu")
			fmt.Println("\texit             \texit")
			fmt.Println("\tpath             \tprint leveldb path")
			fmt.Println("\tkeys             \tprint all keys")
			fmt.Println("\tget key          \tprint key value")
			fmt.Println("\tset key value    \tset key value")
			fmt.Println("\tdelete key       \tdelete key")
			fmt.Println("\texist key        \texist key")
			break
		case "EXIT":
			os.Exit(0)
		case "PATH":
			fmt.Println("leveldb path: ", path)
			break
		case "KEYS":
			keys(db, cmd_args)
			break
		case "GET":
			get(db, cmd_args)
			break
		case "SET":
			set(db, cmd_args)
			break
		case "DELETE":
			delete(db, cmd_args)
			break
		case "EXIST":
			exist(db, cmd_args)
			break
		default:
			fmt.Println("unknown command")
			break
		}
	}
}
