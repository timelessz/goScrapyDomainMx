package importData

import "C"
import (
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	gomongo "scrapyDomain/mongodb"
	"scrapyDomain/mysql"
	"scrapyDomain/orm"
	"scrapyDomain/steplock"
	"strconv"
	"strings"
	"sync"
	"time"
)

//初始化
func InitMData() {
	var wg sync.WaitGroup
	consumerCount := 50
	wg.Add(consumerCount)
	var ch = make(chan orm.MadeInChinaCustomer, consumerCount)
	initData()
	go produceMadeInChina(ch, &wg)
	for i := 0; i < consumerCount; i++ {
		go consumerMadeInChina(ch, &wg, i)
	}
	wg.Wait()
}

func initData() {
	bDatabaseConfigMap := map[string]string{
		"MysqlHost":   "rdsfjnifbfjnifbo.mysql.rds.aliyuncs.com",
		"MysqlDbname": "madeinchina",
		"MysqLUser":   "madeinchina",
		"MysqlPasswd": "LiuRui123$%^",
	}
	mysql.InitInstance(bDatabaseConfigMap)
	const uri = "mongodb://admin:qiangbi123@144.123.173.6:27017"
	gomongo.MustConnect(uri, "bigbusiness")
}

func produceMadeInChina(ch chan<- orm.MadeInChinaCustomer, wg *sync.WaitGroup) {
	fileName := "madeinchinalock.txt"
	step := steplock.Step{
		fileName,
	}
	for true {
		offset, limit := step.GetScrapyFlag()
		Customers := mysql.Instance.GetMadeInChinaLimitCustomer(limit, offset)
		if len(Customers) == 0 {
			// 表示未获取到数据
			fmt.Println("mx 数据爬取生产者，未获取到数据")
			//setScrapyFlag(fileName, 0, 10)
			continue
		}
		for _, Customer := range Customers {
			fmt.Println("mx 数据爬取 生产者：" + strconv.Itoa(Customer.ID) + Customer.Name.String + Customer.Domain.String)
			ch <- Customer
		}
		// 设置已经爬取到的数据
		step.SetScrapyFlag(offset+limit, limit)
	}
	wg.Done()
}

func consumerMadeInChina(ch chan orm.MadeInChinaCustomer, wg *sync.WaitGroup, i int) {
	const uri = "mongodb://admin:qiangbi123@144.123.173.6:27017"
	gomongo.MustConnect(uri, "bigbusiness")
	for true {
		v := <-ch
		fmt.Println(strconv.Itoa(i) + "号码 mx 消费者：" + strconv.Itoa(v.ID) + v.Name.String + v.Domain.String)
		formInsertMadeData(v)
	}
	wg.Done()
}

func formInsertMadeData(c orm.MadeInChinaCustomer) {
	customers := bson.D{
		{"name", ""},
		{"en_name", c.Name.String},
		{"description", ""},
		{"artificial_person", ""},
		{"business_scope", ""},
		{"insured_number", ""},
		{"registered_capital", ""},
		{"found_time", ""},
		{"social_credi_code", ""},
		{"fax", ""},
		{"is_importexport", true},
		{"is_sync", ""},
		{"crm_id", ""},
		{"from", "madeinchina"},
		{"created_at", time.Now()},
		{"updated_at", time.Now()},
	}
	customers = append(customers, bson.E{
		"contact",
		bson.A{
			bson.D{
				{"name", c.Contact.String},
				{"dept", c.Dept.String},
				{"phone", bson.A{
					c.Telephone.String,
					c.Mobile.String,
				}},
				{"email", bson.A{}},
			},
		},
	})
	customers = append(customers, bson.E{"location",
		bson.D{
			{"address", c.Address.String},
			{"province", ""},
			{"province_en", strings.ToLower(c.Province.String)},
			{"city", ""},
			{"city_en", ""},
			{"district", ""},
			{"district_en", ""},
			{"post", ""},
		},
	})

	customers = append(customers, bson.E{"industry",
		bson.D{
			{"industry_id", ""},
			{"industry_name", ""},
		},
	})
	website := bson.A{
		c.Showroom.String,
	}
	if c.Website.String != "" {
		website = append(website, c.Website.String)
	}
	customers = append(customers, bson.E{"website",
		website,
	})
	if c.Domain.String != "" {
		customers = append(customers, bson.E{
			"domain", bson.A{
				bson.D{
					{"domain", c.Domain.String},
					{"title", c.WebsiteTitle.String},
					{"mail_title", ""},
					{"mx_brand_id", strconv.FormatInt(c.MxBrandID.Int64, 10)},
					{"mx_brand_name", c.MxBrandName.String},
					{"mxrecord", c.Mxrecord.String},
					{"selfbuild_brand_id", 0},
					{"selfbuild_brand_name", ""},
					{"contacttool_brand_id", 0},
					{"contacttool_brand_name", ""},
				},
			},
		})
	} else {
		customers = append(customers, bson.E{
			"domain", bson.A{},
		})
	}
	result := gomongo.Instance.FindOne("customer", bson.D{{"en_name", c.EnName.String}})
	if result != nil {
		fmt.Println(result)
		return
	}
	gomongo.Instance.InsertOne("customer", customers)

	/*
		crmDatabaseConfigMap := map[string]string{
			"MysqlHost":   "rdsfjnifbfjnifbo.mysql.rds.aliyuncs.com",
			"MysqlDbname": "salesmenbeta2",
			"MysqLUser":   "salesmen",
			"MysqlPasswd": "qiangbi123",
		}
	*/
	/*
		bDatabaseConfigMap := map[string]string{
			"MysqlHost":   "rdsfjnifbfjnifbo.mysql.rds.aliyuncs.com",
			"MysqlDbname": "bigbusiness",
			"MysqLUser":   "bigbusiness",
			"MysqlPasswd": "LiuRui123$%^",
		}
		mysql.InitInstance(bDatabaseConfigMap)
		Customers := mysql.Instance.GetLimitCustomer(10, 10)
		for _, Customer := range Customers {
			fmt.Println("mx 数据爬取 生产者：" + strconv.Itoa(Customer.ID) + Customer.Name.String + Customer.Domain.String)
		}
		return
	*/
}
