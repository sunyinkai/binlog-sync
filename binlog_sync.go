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
	//当作缓存
	mp map[int]table.User
	//用于记录同步的进度
	nowPos     int
	beforePos  int
	failedLine int
}

func NewBinLogSync(dataSqlFile, posSqlFile, dbPath, csvFile string) *BinLogSync {
	logReader := NewLogReader(csvFile)
	if logReader == nil {
		log.Fatal("NewLogReader error")
	}
	dbHelper := NewDBHelper(dbPath)
	if dbHelper == nil {
		log.Fatal("NewDBHelper error")
	}
	//CheckAndCreate
	if !dbHelper.HasTable(dataTable) {
		dbHelper.CreateTable(dataSqlFile)
	}
	if !dbHelper.HasTable(posTable) {
		dbHelper.CreateTable(posSqlFile)
	}

	//获取上次的同步进度
	func() { //如果db中还没有这个表的数据,那么就插入一条记录
		getPosSql := fmt.Sprintf("select * FROM `%s` WHERE table_name = '%s'", posTable, dataTable)
		rows, err := dbHelper.DB.Query(getPosSql)
		defer rows.Close()
		if err != nil {
			log.Fatal("GetPosSql error,%s", err.Error())
		}
		if rows == nil || !rows.Next() {
			s := make([]interface{}, 0)
			s = append(s, table.SyncPos{TableName: dataTable, LineCnt: 0})
			sqlStr := dbHelper.GenBatchInsertSql(posTable, s)
			dbHelper.DB.Exec(sqlStr)
		}
	}()
	getDataSyncPos := func() int {
		var nowPos int
		getPosSql := fmt.Sprintf("select * FROM `%s` WHERE table_name = '%s'", posTable, dataTable)
		rows, err := dbHelper.DB.Query(getPosSql)
		defer rows.Close()
		if err != nil {
			log.Fatal("GetPosSql error,%s", err.Error())
		}
		for rows != nil && rows.Next() {
			var tableName string
			err = rows.Scan(&tableName, &nowPos)
			if err != nil {
				log.Fatal("Read LineCnt From DB error %s", err.Error())
			}
			log.Printf("Read %s from pos table ok,nowPos:%d", tableName, nowPos)
		}
		return nowPos
	}
	nowPos := getDataSyncPos()

	return &BinLogSync{
		dataSqlFile: dataSqlFile,
		posSqlFile:  posSqlFile,
		csvFile:     csvFile,
		logReader:   logReader,
		dbHelper:    dbHelper,
		mp:          make(map[int]table.User),
		nowPos:      nowPos,
		beforePos:   nowPos,
		failedLine:  0}
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
		tx.Rollback()
		return false
	}
	_, err = tx.Exec(updatePosSql)
	if err != nil {
		tx.Rollback()
		return false
	}
	err = tx.Commit()
	if err != nil {
		return false
	} else {
		return true
	}
}

const (
	LinesPerRead = 1000 //每次从csv文件中读取多少行数据
)

func (bls *BinLogSync) Resume() {
	nowLine := 0
	for nowLine+LinesPerRead < bls.nowPos {
		lines, err := bls.logReader.ReadLines(LinesPerRead)
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
	prevType := ""
	const (
		MaxLineOneOperate = 100 //限制一次操作最多能处理多少行,达到最大行后强行flush
	)
	forceFlush := MaxLineOneOperate
	for {
		//读取csv文件并解析
		lines, err := bls.logReader.ReadLines(LinesPerRead)
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
				bls.failedLine++
				continue
			}
			var user table.User
			ok := bls.logReader.ParseLine(line, &user)
			if !ok {
				bls.failedLine++
				log.Printf("ParseLine %s return false", line)
				continue
			}
			if prevType == "" { //第一次进入循环
				prevType = elements[n-1]
			}
			forceFlush--
			if elements[n-1] != prevType || forceFlush == 0 {
				ok := false
				for tryTime := 0; tryTime < 3; tryTime++ {
					ok = bls.flush(prevType)
					if !ok {
						time.Sleep(5 * time.Second)
					} else {
						break
					}
				}
				if !ok {
					log.Fatal("flush DB error 3 times,exit")
				}
				prevType = elements[n-1]
				forceFlush = MaxLineOneOperate
			}
			bls.mp[user.Uid] = user
		}

		if !bContinue {
			if err == io.EOF {
				bls.flush(prevType) //退出循环前强制刷新一次
				break
			} else {
				log.Fatalf("BinLogSync Error %s", err.Error())
			}
		}
	}
	log.Printf("BinLogSync Finish! success:%d line,failed:%d line!", bls.nowPos-bls.beforePos-bls.failedLine, bls.failedLine)
}
