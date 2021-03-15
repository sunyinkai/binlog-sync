package main

import (
	"binlog-sync/table"
	"fmt"
	"io"
	"log"
	"reflect"
	"strings"
	"time"
)

type BinLogSync struct {
	dataSqlFile string
	posSqlFile  string
	csvFile     string
	logReader   *LogReader
	dbHelper    *DBHelper
	mp          map[int]table.User
	nowPos      int
}

const (
	linesPerRead = 1000
)

func NewBinLogSync(dataSqlFile, posSqlFile, dbPath, csvFile string) *BinLogSync {
	logReader := NewLogReader(csvFile)
	if logReader == nil {
		log.Fatal("NewLogReader error")
	}
	dbHelper := NewDBHelper(dbPath)
	if dbHelper == nil {
		log.Fatal("NewDBHelper error")
	}
	dbHelper.CreateTable(dataSqlFile)
	dbHelper.CreateTable(posSqlFile)
	getPosSql := fmt.Sprintf("select * FROM `%s` WHERE table_name = '%s'", posTable, dataTable)
	rows, err := dbHelper.DB.Query(getPosSql) //CheckAndCreate
	if err == nil {
		log.Fatal("GetPosSql error")
	}
	var lineCnt int
	for rows != nil && rows.Next() {
		var tableName string
		err = rows.Scan(&lineCnt, &tableName)
		if err != nil {
			log.Fatal("Read LineCnt From DB error")
		}
	}

	return &BinLogSync{
		dataSqlFile: dataSqlFile,
		posSqlFile:  posSqlFile,
		csvFile:     csvFile,
		logReader:   logReader,
		dbHelper:    dbHelper,
		mp:          make(map[int]table.User),
		nowPos:      lineCnt}
}

func (bls *BinLogSync) flush(t string) bool {
	if len(bls.mp) == 0 {
		return true
	}
	var sqlStr string
	if t == "I" {
		var userList []interface{}
		for _, v := range bls.mp {
			userList = append(userList, v)
		}
		sqlStr = bls.dbHelper.GenBatchInsertSql(dataTable, userList)
	} else {
		var keys []int
		for k, _ := range bls.mp {
			keys = append(keys, k)
		}
		sqlStr = bls.dbHelper.GenBatchDeleteSql(dataTable, keys)
	}
	//清空cache
	bls.mp = make(map[int]table.User)

	//生成更新pos表的sql语句
	updatePosSql := fmt.Sprintf("UPDATE `%s` SET line_cnt=%d WHERE table_name='%s';", posTable, bls.nowPos, dataTable)
	//更新数据库以及pos，使用事务，原子写入
	tx, err := bls.dbHelper.DB.Begin()
	if err != nil {
		return false
	}
	_, err = tx.Exec(sqlStr)
	if err != nil {
		return false
	}
	_, err = tx.Exec(updatePosSql)
	if err != nil {
		return false
	}
	err = tx.Commit()
	if err != nil {
		return false
	} else {
		return true
	}
}
func (bls *BinLogSync) Resume() {
	nowLine := 0
	for nowLine+linesPerRead < bls.nowPos {
		lines, err := bls.logReader.ReadLines(linesPerRead)
		if err != nil && err != io.EOF {
			log.Fatal("Resume error,exit")
		}
		nowLine += len(lines)
	}
	lines, err := bls.logReader.ReadLines(bls.nowPos - nowLine)
	if err != nil && err != io.EOF {
		log.Fatal("Resume error,exit")
	}
	nowLine += len(lines)
}
func (bls *BinLogSync) Start() {
	bls.Resume() //恢复上次的进度

	bContinue := true
	prevModify := ""
	for {
		//读取csv文件并解析
		lines, err := bls.logReader.ReadLines(linesPerRead)
		bls.nowPos += len(lines)
		if err != nil {
			bContinue = false
		}
		var tmp table.User
		colCnt := reflect.TypeOf(tmp).NumField()
		for _, line := range lines {
			elements := strings.Split(line, ",")
			n := len(elements)
			if n != colCnt+1 {
				continue
			}
			var user table.User
			ok := bls.logReader.ParseLine(line, &user)
			if !ok {
				log.Printf("ParseLine %s return false", line)
				continue
			}
			bls.mp[user.Uid] = user
			if prevModify == "" { //第一次进入循环
				prevModify = elements[n-1]
			}
			if elements[n-1] == prevModify {
				bls.mp[user.Uid] = user
			} else {
				ok := false
				for tryTime := 0; tryTime < 3; tryTime++ {
					ok = bls.flush(prevModify)
					if !ok {
						time.Sleep(5 * time.Second)
					} else {
						break
					}
				}
				if !ok {
					log.Fatal("flush DB error 3 times,exit")
				}
				prevModify = elements[n-1]
			}
		}

		if !bContinue {
			if err == io.EOF {
				log.Println("BinLogSync Success!Exit 0")
			} else {
				log.Fatalf("BinLogSync Error %s", err.Error())
			}
		}
	}
}
