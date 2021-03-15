package main

const (
	CSVFile     = "~/log.csv"
	dataSqlFile = "~/user.sql"
	posSqlFile  = "~/pos.sql"
	DBPath      = "root:123456@tcp(127.0.0.1:3306)/userInfo"
	dataTable   = "user" //表名
	posTable    = "pos"
)

func main() {
	s := NewBinLogSync(dataSqlFile, posSqlFile, DBPath, CSVFile)
	s.Start()
}
