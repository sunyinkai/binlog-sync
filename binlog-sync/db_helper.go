package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"io/ioutil"
	"log"
	"reflect"
	"strconv"
	"strings"
)

type DBHelper struct {
	DB *sql.DB
}

func NewDBHelper(path string) *DBHelper {
	DB, err := sql.Open("mysql", path)
	DB.SetConnMaxLifetime(200)
	DB.SetMaxIdleConns(20)
	if err != nil {
		log.Fatal("open my sql error,%s", err.Error())
		return nil
	}
	if err := DB.Ping(); err != nil {
		log.Fatal("connect to db,error", err.Error())
	}
	log.Println("connect to db success")
	return &DBHelper{DB: DB}
}

func (d *DBHelper) CreateTable(sqlFilePath string) error {
	sqlBytes, err := ioutil.ReadFile(sqlFilePath)
	if err != nil {
		return err
	}
	sqlTable := string(sqlBytes)
	_, err = d.DB.Exec(sqlTable)
	log.Printf("CreateTable %s error %v", sqlTable, err)
	if err != nil {
		return err
	}
	return nil
}
func (d *DBHelper) formattedValue(objV reflect.Value, index int, t string) string {
	v := ""
	if t == "string" {
		v = fmt.Sprintf("'%s'", objV.Field(index).String())
	} else if t == "int" {
		return fmt.Sprintf("%v", objV.Field(index).Int())
	} else if t == "uint" {
		return fmt.Sprintf("%v", objV.Field(index).Uint())
	}
	return v
}
func (d *DBHelper) getFieldName(objT reflect.Type, index int) string {
	jsonName := objT.Field(index).Tag.Get("gorm")
	for _, name := range strings.Split(jsonName, ";") {
		if strings.Index(name, "column") == -1 {
			continue
		}
		return strings.Replace(name, "column:", "", 1)
	}
	return objT.Field(index).Name
}

func (d *DBHelper) GenBatchInsertSql(tableName string, values []interface{}) string {
	if len(values) == 0 {
		return ""
	}
	objT := reflect.TypeOf(values[0])
	var tableByte []byte
	var valueTypeList []string
	for i := 0; i < objT.NumField(); i++ { //假设结构体的字段名就是表中的列名
		valueTypeList = append(valueTypeList, objT.Field(i).Type.Name())
		if i != 0 {
			tableByte = append(tableByte, ',')
		}
		fieldName := d.getFieldName(objT, i)
		tableByte = append(tableByte, '`')
		tableByte = append(tableByte, fieldName...)
		tableByte = append(tableByte, '`')
	}
	tableStr := string(tableByte)

	var valueList []string
	for _, k := range values {
		v := "("
		objV := reflect.ValueOf(k)
		for i, j := range valueTypeList {
			if i != 0 {
				v += ","
			}
			v += d.formattedValue(objV, i, j)
		}
		v += ")"
		valueList = append(valueList, v)
	}
	sqlStr := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES %s", tableName, tableStr, strings.Join(valueList, ",")+";")
	log.Printf("GenInsertSql:%s", sqlStr)
	return sqlStr
}

//删除tableName中,主键为keys的数据
func (d *DBHelper) GenBatchDeleteSql(tableName string, keys []int) string {
	if len(keys) == 0 {
		return ""
	}
	var vals []byte
	vals = append(vals, '(')
	for i, v := range keys {
		if i != 0 {
			vals = append(vals, ',')
		}
		vals = append(vals, strconv.Itoa(v)...)
	}
	vals = append(vals, ')')
	sqlStr := fmt.Sprintf("DELETE FROM %s WHERE uid in %s)", tableName, string(vals))
	log.Printf("GenDeleteSql:%s", sqlStr)
	return sqlStr
}

func (d *DBHelper) HasTable(tableName string) bool {
	return true
}
