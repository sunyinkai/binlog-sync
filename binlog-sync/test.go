package main

const (
	CSVFile     = "./data/log.csv"
	dataSqlFile = "./data/user.sql"
	posSqlFile  = "./data/pos.sql"
	DBPath      = "root:123456@tcp(localhost:3306)/userInfo"
	dataTable   = "user" //表名
	posTable    = "pos"
)

func main() {
	s := NewBinLogSync(dataSqlFile, posSqlFile, DBPath, CSVFile)
	s.Start()
}
