package table

type User struct {
	Uid   int    `gorm:"column:uid"`
	Score int    `gorm:"column:score"`
	Name  string `gorm:"column:name"`
	Phone string `gorm:"column:phone"`
}

type SyncPos struct {
	TableName string `gorm:"column:table_name"`
	LineCnt   int    `gorm:"column:line_cnt"`
}
