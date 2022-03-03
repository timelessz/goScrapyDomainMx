package orm

import "database/sql"

type MadeInChinaCustomer struct {
	ID           int
	IndustryID   sql.NullInt64
	Name         sql.NullString
	EnName       sql.NullString
	Address      sql.NullString
	City         sql.NullString
	Province     sql.NullString
	Contact      sql.NullString
	Dept         sql.NullString
	Position     sql.NullString
	Telephone    sql.NullString
	Mobile       sql.NullString
	Fax          sql.NullString
	Showroom     sql.NullString
	Website      sql.NullString
	Domain       sql.NullString
	WebsiteTitle sql.NullString
	Type         sql.NullString
	MxBrandID    sql.NullInt64
	MxBrandName  sql.NullString
	Mxrecord     sql.NullString
	CreatedAt    sql.NullInt64
	UpdatedAt    sql.NullInt64
	IsSync       int
}

// 自定义表名
func (MadeInChinaCustomer) TableName() string {
	return "customer"
}
