package mysql

import (
	"errors"
	"fmt"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"log"
	"scrapyDomain/orm"
	"sync"
)

/*
* MysqlCrmConnectiPool
* 数据库连接操作库
* 基于gorm封装开发
 */

type MysqlConnectPool struct {
	Db *gorm.DB
}

var Instance *MysqlConnectPool
var Once sync.Once

var ErrDb error

// 单例模式
func InitInstance(databaseConfig map[string]string) {
	Once.Do(func() {
		Instance = &MysqlConnectPool{}
		if status := Instance.initDataPool(databaseConfig); !status {
			panic("数据库连接失败")
		}
	})
}

/*
* @fuc 初始化数据库连接(可在mail()适当位置调用)
 */
func (pool *MysqlConnectPool) initDataPool(databaseConfig map[string]string) (issucc bool) {
	//var MysqlHost string = "rdsfjnifbfjnifbo.mysql.rds.aliyuncs.com"
	//var MysqlDbname string = "salesmenbeta2"
	//var MysqLUser string = "salesmen"
	//var MysqlPasswd string = "qiangbi123"
	linkStr := fmt.Sprintf("%s:%s@tcp(%s:3306)/%s?charset=utf8&parseTime=True&loc=Local", databaseConfig["MysqLUser"], databaseConfig["MysqlPasswd"], databaseConfig["MysqlHost"], databaseConfig["MysqlDbname"])
	Db, ErrDb := gorm.Open("mysql", linkStr)
	Db.SingularTable(true)
	Db.LogMode(true)
	fmt.Println(ErrDb)
	if ErrDb != nil {
		log.Fatal(ErrDb)
		return false
	}
	pool.Db = Db
	//关闭数据库，db会被多个goroutine共享，可以不调用
	//defer db.Close()
	return true
}

// 获取客户信息
func (pool *MysqlConnectPool) GetLimitCustomer(limit int, offset int) []orm.Customer {
	//pool.Db.Where()
	var customers []orm.Customer
	if err := pool.Db.Where("industry_id!= ?", 0).Preload("Industry").Order("id desc").Offset(offset).Limit(limit).Find(&customers).Error; err != nil {
		// 数据报错
		if errors.Is(err, gorm.ErrRecordNotFound) {
			// 返回空数组
			return customers
		}
		fmt.Println("获取数据异常")
	}
	return customers
}

// 获取 MadeInChina
func (pool *MysqlConnectPool) GetMadeInChinaLimitCustomer(limit int, offset int) []orm.MadeInChinaCustomer {
	//pool.Db.Where()
	var customers []orm.MadeInChinaCustomer
	if err := pool.Db.Order("id desc").Offset(offset).Limit(limit).Find(&customers).Error; err != nil {
		// 数据报错
		if errors.Is(err, gorm.ErrRecordNotFound) {
			// 返回空数组
			return customers
		}
		fmt.Println("获取数据异常")
	}
	return customers
}

//mx 后缀对应品牌数据
type MxSuffix struct {
	BId    string `json:"b_id"`
	Suffix string `json:"suffix"`
	Name   string `json:"name"`
}

// map
// 获取limit 客户数据
/*func (pool *MysqlConnectPool) GetCrmSuffixData() map[string]MxSuffix {
	suffixMap := make(map[string]MxSuffix)
	rows, err := pool.Db.Table("sm_mx_suffix as s").Select("s.mxsuffix as suffix,s.brand_id as b_id, b.name as name").Joins("left join sm_mx_brand as b on b.id=s.brand_id").Rows()
	if err != nil {
		fmt.Println(err)
	}
	defer rows.Close()
	for rows.Next() {
		var suffix, b_id, name string
		rows.Scan(&suffix, &b_id, &name)
		s := MxSuffix{
			BId:    b_id,
			Suffix: suffix,
			Name:   name,
		}
		suffixMap[suffix] = s
	}
	return suffixMap
}*/
