package importData

import "C"
import (
	"fmt"
	"github.com/mozillazg/go-pinyin"
	"go.mongodb.org/mongo-driver/bson"
	gomongo "scrapyDomain/mongodb"
	"scrapyDomain/mysql"
	"scrapyDomain/orm"
	"scrapyDomain/steplock"
	"scrapyDomain/tool"
	"strconv"
	"strings"
	"sync"
	"time"
)

//初始化
func Init() {
	var wg sync.WaitGroup
	consumerCount := 500
	wg.Add(consumerCount)
	var ch = make(chan orm.Customer, consumerCount)
	initDb()
	go produce(ch, &wg)
	for i := 0; i < consumerCount; i++ {
		go consumer(ch, &wg, i)
	}
	wg.Wait()
}

func initDb() {
	bDatabaseConfigMap := map[string]string{
		"MysqlHost":   "rdsfjnifbfjnifbo.mysql.rds.aliyuncs.com",
		"MysqlDbname": "bigbusiness",
		"MysqLUser":   "bigbusiness",
		"MysqlPasswd": "LiuRui123$%^",
	}
	mysql.InitInstance(bDatabaseConfigMap)
	const uri = "mongodb://admin:qiangbi123@144.123.173.6:27017"
	gomongo.MustConnect(uri, "bigbusiness")
}

func produce(ch chan<- orm.Customer, wg *sync.WaitGroup) {
	fileName := "lock.txt"
	step := steplock.Step{
		fileName,
	}
	for true {
		offset, limit := step.GetScrapyFlag()
		Customers := mysql.Instance.GetLimitCustomer(limit, offset)
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

func consumer(ch <-chan orm.Customer, wg *sync.WaitGroup, i int) {
	const uri = "mongodb://admin:qiangbi123@144.123.173.6:27017"
	gomongo.MustConnect(uri, "bigbusiness")
	for true {
		v := <-ch
		fmt.Println(strconv.Itoa(i) + "号码 mx 消费者：" + strconv.Itoa(v.ID) + v.Name.String + v.Domain.String)
		formInsertData(v)
	}
	wg.Done()
}

func formInsertData(c orm.Customer) {
	customers := bson.D{
		{"name", c.Name.String},
		{"en_name", ""},
		{"description", c.BusinessScope.String},
		{"artificial_person", c.ArtificialPerson.String},
		{"business_scope", c.BusinessScope.String},
		{"insured_number", strconv.FormatInt(c.InsuredNumber.Int64, 10)},
		{"registered_capital", c.RegisteredCapital.String},
		{"found_time", c.FoundTime},
		{"social_credi_code", c.SocialCrediCode.String},
		{"fax", ""},
		{"is_importexport", false},
		{"is_sync", ""},
		{"crm_id", ""},
		{"created_at", time.Now()},
		{"updated_at", time.Now()},
	}
	contact := c.Contact.String
	contacts := strings.Split(contact, "；")
	phones := bson.A{}
	for _, phone := range contacts {
		phones = append(phones, phone)
	}
	customers = append(customers, bson.E{
		"contact",
		bson.A{
			bson.D{
				{"name", ""},
				{"dept", ""},
				{"phone", phones},
				{"email",
					bson.A{
						c.Email.String,
					},
				},
			},
		},
	})
	customers = append(customers, bson.E{"location",
		bson.D{
			{"address", c.Address.String},
			{"province", c.Province.String},
			{"province_en", strings.Join(pinyin.LazyConvert(c.Province.String, nil), "")},
			{"city", c.City.String},
			{"city_en", strings.Join(pinyin.LazyConvert(c.City.String, nil), "")},
			{"district", c.District.String},
			{"district_en", strings.Join(pinyin.LazyConvert(c.District.String, nil), "")},
			{"post", ""},
		},
	})

	customers = append(customers, bson.E{"industry",
		bson.D{
			{"industry_id", strconv.Itoa(c.Industry.ID)},
			{"industry_name", c.Industry.Name.String},
		},
	})

	if c.URL.String != "" {
		customers = append(customers, bson.E{"website",
			bson.A{
				c.URL.String,
			},
		})
	} else {
		customers = append(customers, bson.E{"website",
			bson.A{},
		})
	}
	domains := tool.SubDomain(c.URL.String)
	domainA := bson.A{}
	for _, d := range domains {
		domainA = append(domainA, bson.D{
			{"domain", d},
			{"title", ""},
			{"mail_title", ""},
			{"mx_brand_id", ""},
			{"mx_brand_name", ""},
			{"mxrecord", ""},
			{"selfbuild_brand_id", "0"},
			{"selfbuild_brand_name", ""},
			{"contacttool_brand_id", "0"},
			{"contacttool_brand_name", ""},
		})
	}
	// 正则匹配客户信息
	customers = append(customers, bson.E{
		"domain", domainA,
	})
	result := gomongo.Instance.FindOne("customer", bson.D{{"name", c.Name.String}})
	if result != nil {
		fmt.Println(result)
		return
	}
	gomongo.Instance.InsertOne("customer", customers)
}
