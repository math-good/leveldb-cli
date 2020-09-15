package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/util"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"syscall"
	"time"
)

var (
	h           bool
	path        string
	r           bool
	clearOnExit bool
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

	if r {
		fmt.Println("readonly mode")
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

	if r {
		fmt.Println("readonly mode")
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
	flag.BoolVar(&r, "r", false, "open leveldb in readonly mode")
	flag.StringVar(&path, "p", "", "leveldb database absolute path")
}

/**
leveldb不能以多进程方式访问
拷贝数据库，以只读方式打开
*/
func openReadonly() *leveldb.DB {
	newDbPath := filepath.Join(os.TempDir(), fmt.Sprintf("leveldb-%v", time.Now().UnixNano()))
	//拷贝数据库到临时目录
	fmt.Println("copy ", path, " to ", newDbPath)
	CopyDir(path, newDbPath)

	db, err := leveldb.OpenFile(newDbPath, nil)

	if err != nil {
		fmt.Println(err.Error())
		os.Exit(0)
	}

	r = true
	//退出时清除
	clearOnExit = true
	path = newDbPath

	return db
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
		sysErr, isSysErr := err.(syscall.Errno)
		if isSysErr {
			fmt.Println(sysErr.Error())
			if runtime.GOOS == "linux" {
				//linux
				if match, _ := regexp.MatchString(".*resource temporarily unavailable.*", sysErr.Error()); match {
					db = openReadonly()
				}
			} else if runtime.GOOS == "windows" {
				//internal/syscall/windows.ERROR_SHARING_VIOLATION (32)
				//windows
				if match, _ := regexp.MatchString(".*it is being used by another process.*", sysErr.Error()); match {
					db = openReadonly()
				}
			}
		} else {
			fmt.Println(err.Error())
			return
		}
	}
	defer func() {
		db.Close()
		if clearOnExit {
			os.RemoveAll(path)
		}
	}()

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
			return
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
