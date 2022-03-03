package importData

import (
	"fmt"
	"github.com/xuri/excelize/v2"
	"go.mongodb.org/mongo-driver/bson"
	gomongo "scrapyDomain/mongodb"
	"scrapyDomain/tool"
	"strconv"
	"sync"
	"time"
)

//初始化
func InitGData() {
	var wg sync.WaitGroup
	consumerCount := 200
	wg.Add(consumerCount)
	var ch = make(chan map[string]string, consumerCount)
	initGDb()
	go readGExcel(ch, &wg)
	for i := 0; i < consumerCount; i++ {
		go writeGDb(ch, &wg, i)
	}
	wg.Wait()
}

func initGDb() {
	//bDatabaseConfigMap := map[string]string{
	//	"MysqlHost":   "rdsfjnifbfjnifbo.mysql.rds.aliyuncs.com",
	//	"MysqlDbname": "bigbusiness",
	//	"MysqLUser":   "bigbusiness",
	//	"MysqlPasswd": "LiuRui123$%^",
	//}
	//mysql.InitInstance(bDatabaseConfigMap)
	const uri = "mongodb://admin:qiangbi123@144.123.173.6:27017"
	gomongo.MustConnect(uri, "bigbusiness")
}

func readGExcel(ch chan<- map[string]string, wg *sync.WaitGroup) {
	// 读取文件
	//files, _ := ioutil.ReadDir("D:\\广交会数据汇总\\2021年阿里国际站名录")
	var files []string
	files, _ = tool.GetAllFile("C:\\Users\\qiangbi\\Desktop\\data", files)
	for _, f := range files {
		f, err := excelize.OpenFile(f)
		if err != nil {
			fmt.Println(err)
			return
		}
		// Get all the rows in the Sheet1.
		rows, err := f.GetRows("Sheet1")
		pd := make(map[string]string)
		header := make(map[int]string)
		for rowindex, row := range rows {
			if rowindex == 0 {
				pd = map[string]string{}
				for cellindex, colCell := range row {
					header[cellindex] = colCell
				}
				fmt.Println(header)
			} else {
				pd = map[string]string{}
				for cellindex, colCell := range row {
					pd[header[cellindex]] = colCell
				}
				fmt.Println("数据读取：" + pd["CompanyName"])
				ch <- pd
			}
		}
	}
	wg.Done()
}

func writeGDb(ch <-chan map[string]string, wg *sync.WaitGroup, i int) {
	const uri = "mongodb://admin:qiangbi123@144.123.173.6:27017"
	gomongo.MustConnect(uri, "bigbusiness")
	for true {
		v := <-ch
		fmt.Println(strconv.Itoa(i) + "号 消费者插入数据：" + v["公司名称"])
		//fmt.Println(ch)
		formGInsertMData(v)
	}
	wg.Done()
}

func formGInsertMData(c map[string]string) {
	customers := bson.D{
		{"name", c["公司名称"]},
		{"en_name", c["公司英文名"]},
		{"description", c["公司介绍"]},
		{"artificial_person", c["公司法人"]},
		{"business_scope", ""},
		{"insured_number", ""},
		{"registered_capital", c["注册资本"]},
		{"found_time", c["成立日期"]},
		{"social_credid_code", ""},
		{"fax", c["传真"]},
		{"is_importexport", true},
		{"is_sync", ""},
		{"crm_id", ""},
		{"from", "guangjiaohui"},
		{"created_at", time.Now()},
		{"updated_at", time.Now()},
	}
	phones := bson.A{
		c["固定电话"],
		c["联系电话"],
	}
	// 性别
	customers = append(customers, bson.E{
		"contact",
		bson.A{
			bson.D{
				{"name", c["联系人"]},
				{"dept", ""},
				{"phone", phones},
				{"email",
					bson.A{
						c["邮箱"],
					},
				},
				{"fax", c["传真"]},
			},
		},
	})

	customers = append(customers, bson.E{"location",
		bson.D{
			{"address", c["地址"]},
			{"province", c["所属省份"]},
			{"province_en", c["所属省份"]},
			{"city", c["所属城市"]},
			{"city_en", c["所属城市"]},
			{"district", c["所属区县"]},
			{"district_en", c["所属区县"]},
			{"post", c["PostCode"]},
		},
	})
	customers = append(customers, bson.E{"industry",
		bson.D{
			{"industry_id", ""},
			{"industry_name", ""},
		},
	})
	// 切分url
	//--------------------------------------------------------

	if c["网址"] != "" {
		website := bson.A{
			c["网址"],
		}
		customers = append(customers, bson.E{"website",
			website,
		})
	} else {
		customers = append(customers, bson.E{"website",
			bson.A{},
		})
	}
	//--------------------------------------------------------
	//截取域名信息
	//fmt.Println(c["SiteUrl"])
	//fmt.Println(siteUrls)
	domainA := bson.A{}
	if c["网址"] != "" {
		domains := tool.SubDomain(c["网址"])
		for _, d := range domains {
			if d != "" {
				domainA = append(domainA, bson.D{
					{"domain", d},
					{"title", ""},
					{"mail_title", ""},
					{"mx_brand_id", ""},
					{"mx_brand_name", ""},
					{"mxrecord", ""},
					{"selfbuild_brand_id", ""},
					{"selfbuild_brand_name", ""},
					{"contacttool_brand_id", ""},
					{"contacttool_brand_name", ""},
				})
			}
		}
	}
	//-------------------------------------------------------
	customers = append(customers, bson.E{
		"domain", domainA,
	})
	//fmt.Println(c)
	//fmt.Println(customers)
	result := gomongo.Instance.FindOne("cuatomer", bson.D{{"name", c["公司名称"]}})
	if result != nil {
		fmt.Println(result)
		return
	}
	fmt.Println(customers)
	gomongo.Instance.InsertOne("customer", customers)
}
