package main

import (
	"bufio"
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
func isStructPtr(t reflect.Type) bool {
	return t.Kind() == reflect.Ptr && t.Elem().Kind() == reflect.Struct
}

func (l *LogReader) ParseLine(line string, obj interface{}) bool {
	objT := reflect.TypeOf(obj)
	objV := reflect.ValueOf(obj)
	if !isStructPtr(objT) {
		log.Fatal("%v must be  a struct pointer", obj)
	}
	values := strings.Split(line, ",")
	if len(values) < objT.Elem().NumField() {
		return false
	}
	for i := 0; i < objT.Elem().NumField(); i++ {
		typeName := objT.Elem().Field(i).Type.Name()
		if typeName == "int" {
			if num, err := strconv.Atoi(values[i]); err == nil {
				objV.Elem().Field(i).SetInt(int64(num))
			}
		} else if typeName == "string" {
			objV.Elem().Field(i).SetString(values[i])
		}
	}
	return true
}
