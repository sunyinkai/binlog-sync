package main

import (
	"binlog-sync/table"
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"reflect"
	"strconv"
	"strings"
)

type LogReader struct {
	rowCnt int
	br     *bufio.Reader
}

func NewLogReader(file string) *LogReader {
	fr, err := os.Open(file)
	if err != nil {
		log.Printf("open %s failed", file)
		return nil
	}
	br := bufio.NewReader(fr)
	return &LogReader{
		br: br}
}

//一次读取row行
func (l *LogReader) ReadLines(row int) ([]string, error) {
	var res []string
	for i := 0; i < row; i++ {
		line, _, err := l.br.ReadLine()
		if err == io.EOF {
			return res, err
		} else if err != nil {
			return nil, err
		}
		res = append(res, string(line))
	}
	return res, nil
}

//将line转化为结构体
func (l *LogReader) ParseLine(line string, obj *table.User) bool {
	objT := reflect.TypeOf(*obj)
	values := strings.Split(line, ",")
	if len(values) < objT.NumField() {
		return false
	}
	fmt.Println(reflect.ValueOf(*obj).CanAddr())
	for i := 0; i < objT.NumField(); i++ {
		s := reflect.ValueOf(obj).Elem()
		typeName := objT.Field(i).Type.Name()
		if typeName == "int" {
			if num, err := strconv.Atoi(values[i]); err == nil {
				s.Field(i).SetInt(int64(num))
			}
		} else if typeName == "string" {
			s.Field(i).SetString(values[i])
		}
	}
	return true
}
