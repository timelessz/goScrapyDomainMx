package importData

import (
	"fmt"
	"github.com/xuri/excelize/v2"
	"go.mongodb.org/mongo-driver/bson"
	gomongo "scrapyDomain/mongodb"
	"scrapyDomain/tool"
	"strconv"
	"strings"
	"sync"
	"time"
)

//初始化
func InitData() {
	var wg sync.WaitGroup
	consumerCount := 200
	wg.Add(consumerCount)
	var ch = make(chan map[string]string, consumerCount)
	initUserDb()
	go readExcel(ch, &wg)
	for i := 0; i < consumerCount; i++ {
		go writeDb(ch, &wg, i)
	}
	wg.Wait()
}

func initUserDb() {
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

func readExcel(ch chan<- map[string]string, wg *sync.WaitGroup) {
	// 读取文件
	//files, _ := ioutil.ReadDir("D:\\广交会数据汇总\\2021年阿里国际站名录")
	var files []string
	files, _ = tool.GetAllFile("D:\\广交会数据汇总\\2021年阿里国际站名录", files)
	for _, f := range files {
		f, err := excelize.OpenFile(f)
		if err != nil {
			fmt.Println(err)
			return
		}
		// Get all the rows in the Sheet1.
		rows, err := f.GetRows("DataList")
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

func writeDb(ch <-chan map[string]string, wg *sync.WaitGroup, i int) {
	const uri = "mongodb://admin:qiangbi123@144.123.173.6:27017"
	gomongo.MustConnect(uri, "bigbusiness")
	for true {
		v := <-ch
		fmt.Println(strconv.Itoa(i) + "号 消费者插入数据：" + v["CompanyName"])
		formInsertMData(v)
	}
	wg.Done()
}

func formInsertMData(c map[string]string) {
	customers := bson.D{
		{"name", ""},
		{"en_name", c["CompanyName"]},
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
		{"from", "alibaba"},
		{"created_at", time.Now()},
		{"updated_at", time.Now()},
	}
	phones := bson.A{
		c["Mobile"],
		c["Phone"],
	}
	// 性别
	sex := bson.E{}
	if c["Sex"] == "mr" {
		sex = bson.E{"sex", "男"}
	} else {
		sex = bson.E{"sex", "女"}
	}
	customers = append(customers, bson.E{
		"contact",
		bson.A{
			bson.D{
				{"name", c["LinkMan"]},
				{"dept", c["Duty"]},
				sex,
				{"phone", phones},
				{"email",
					bson.A{
						c["Email"],
					},
				},
				{"fax", c["Fax"]},
			},
		},
	})

	customers = append(customers, bson.E{"location",
		bson.D{
			{"address", c["Address"]},
			{"province", ""},
			{"province_en", c["Province"]},
			{"city", ""},
			{"city_en", c["City"]},
			{"district", ""},
			{"district_en", ""},
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
	urls := strings.Split(c["SiteUrl"], "https://")
	var siteUrls = make([]string, 5)
	for _, url := range urls {
		us := strings.Split(url, "http://")
		siteUrls = append(siteUrls, us...)
	}
	website := bson.A{}
	for _, url := range urls {
		website = append(website, url)
	}
	customers = append(customers, bson.E{"website",
		website,
	})
	customers = append(customers, bson.E{"website",
		bson.A{},
	})

	//--------------------------------------------------------
	//截取域名信息
	//fmt.Println(c["SiteUrl"])
	//fmt.Println(siteUrls)
	domainA := bson.A{}
	for _, url := range siteUrls {
		if url != "" {
			domains := tool.SubDomain(url)
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
	}
	//-------------------------------------------------------
	customers = append(customers, bson.E{
		"domain", domainA,
	})
	//fmt.Println(c)
	//fmt.Println(customers)
	result := gomongo.Instance.FindOne("customer", bson.D{{"en_name", c["CompanyName"]}})
	if result != nil {
		fmt.Println(result)
		return
	}
	gomongo.Instance.InsertOne("customer", customers)
}
