package main

import (
	"errors"
	"fmt"
	"github.com/jinzhu/gorm"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
	gomongo "scrapyDomain/mongodb"
	"scrapyDomain/mysql"
	"scrapyDomain/orm"
	"scrapyDomain/scrapy"
	"scrapyDomain/steplock"
	"scrapyDomain/tool"
	"sync"
)

// 获取limit offset 指定数量客户
func getLimitCustomer(limit int, offset int, db *gorm.DB) []orm.Customer {
	var Customers []orm.Customer
	if err := db.Where("URL != ? AND mxrecord = ?", "", "").Order("id desc").Offset(offset).Limit(limit).Find(&Customers).Error; err != nil {
		// 数据报错
		if errors.Is(err, gorm.ErrRecordNotFound) {
			// 返回空数组
			return Customers
		}
		fmt.Println("获取数据异常")
	}
	return Customers
}

func produce(ch chan<- bson.M, wg *sync.WaitGroup) {
	fileName := "scrapylock.txt"
	step := steplock.Step{
		fileName,
	}
	for true {
		offset, limit := step.GetScrapyFlag()
		options := options.Find()
		options.SetSort(bson.M{"created_at": -1})
		options.SetSkip(int64(offset))
		options.SetLimit(int64(limit))
		filter := bson.M{
			"domain": bson.M{"$ne": bson.A{}},
		}
		Customers := gomongo.Instance.FindMany("customer", filter, options)
		for _, Customer := range Customers {
			//v, _ := Customer["name"].(string)
			ch <- Customer
		}
		if len(Customers) == 0 {
			// 表示未获取到数据
			fmt.Println("mx 数据爬取生产者，未获取到数据")
			step.SetScrapyFlag(0, 1000)
			continue
		}
		for _, Customer := range Customers {
			//fmt.Println("mx 数据爬取 生产者：" + strconv.Itoa(Customer) + Customer.Name)
			ch <- Customer
		}
		// 设置已经爬取到的数据
		step.SetScrapyFlag(offset+limit, limit)
	}
	wg.Done()
}

// 消费者
func consumer(ch <-chan bson.M, wg *sync.WaitGroup, suffixMap map[string]mysql.MxSuffix, i int) {
	for true {
		v := <-ch
		mongoId := v["_id"]
		mongoIds := mongoId.(primitive.ObjectID).Hex()
		domains := v["domain"]
		name, _ := v["name"].(string)
		if domainArr, ok := domains.(primitive.A); ok {
			dA := []interface{}(domainArr)
			for _, d := range dA {
				dm, okd := d.(primitive.M)
				if !okd {
					continue
				}
				s := fmt.Sprint(dm["domain"])
				brand_id := fmt.Sprint(dm["brand_id"])
				// 获取mx记录
				fmt.Println(s)
				mxrecord := tool.ExecDigCommand(s)
				if mxrecord == "" {
					// website 获取数据为空
					fmt.Println(name + s + "获取MXRECORD为空")
					continue
				}
				suffix := AnalyseMxRecord(mxrecord)
				if suffix == "" {
					continue
				}
				subsuffix := tool.GetUrlTldDomain("http://" + suffix)
				// 获取mx后缀 对应的品牌
				mss, err := GetMxRecordSuffix(suffixMap, subsuffix)
				if mss == (mysql.MxSuffix{}) {
					// 保存未分类的数据信息
					SaveMxSuffixData(mysql.MxSuffix{
						BId:    "",
						Suffix: subsuffix,
						Name:   "",
					})
				} else {
					SaveMxSuffixData(mss)
				}
				fmt.Println(name + s + "获取MXRECORD为" + mxrecord)
				if err != nil {
					fmt.Println(err.Error())
				}
				SaveCustomerMxInfo(mss, s, brand_id, mongoIds, mxrecord)
			}
		}
	}
	wg.Done()
}

/**
保存 客户mx 信息
*/
func SaveCustomerMxInfo(mss mysql.MxSuffix, domain string, pre_brand_id string, mongodbId string, mxrecord string) {
	oid, _ := primitive.ObjectIDFromHex(mongodbId)
	filter := bson.D{{"_id", oid}, {"domain.domain", domain}}
	update := bson.M{"domain.$.mxrecord": mxrecord}
	if mss != (mysql.MxSuffix{}) && pre_brand_id != mss.BId {
		update["domain.$.mx_brand_id"] = mss.BId
		update["domain.$.mx_brand_name"] = mss.Name
	} else {
		fmt.Println("MX品牌未变化")

	}
	result := gomongo.Instance.UpdateOne("customer", filter, bson.M{"$set": update})
	fmt.Println(result)
}

/**
保存并更新未分类的mx后缀
*/
func SaveMxSuffixData(mss mysql.MxSuffix) {
	subsuffix := mss.Suffix
	collection := "mxsuffix"
	filter := bson.D{{"suffix", subsuffix}}
	result := gomongo.Instance.FindOne(collection, filter)
	fmt.Println(result)
	if result != nil {
		filter := bson.D{{"suffix", subsuffix}}
		update := bson.M{"$inc": bson.M{"count": 1}}
		result := gomongo.Instance.UpdateOne(collection, filter, update)
		fmt.Println(result)
	} else {
		suffix := bson.D{
			{"suffix", subsuffix},
			{"count", 1},
			{"brand_id", mss.BId},
			{"name", mss.Name},
		}
		result := gomongo.Instance.InsertOne(collection, suffix)
		fmt.Println(result)
	}
}

func initDb() {
	bDatabaseConfigMap := map[string]string{
		"MysqlHost":   "rdsfjnifbfjnifbo.mysql.rds.aliyuncs.com",
		"MysqlDbname": "salesmenbeta2",
		"MysqLUser":   "salesmen",
		"MysqlPasswd": "qiangbi123",
	}
	mysql.InitInstance(bDatabaseConfigMap)
	const uri = "mongodb://admin:qiangbi123@144.123.173.6:27017"
	gomongo.MustConnect(uri, "bigbusiness")
}

func StartScrapy() {
	initDb()
	var wg sync.WaitGroup
	consumerCount := 200
	wg.Add(consumerCount)
	var ch = make(chan bson.M, consumerCount)
	go produce(ch, &wg)
	suffixMap := mysql.Instance.GetCrmSuffixData()
	for i := 0; i < consumerCount; i++ {
		go consumer(ch, &wg, suffixMap, i)
	}
	//网站 www 爬取
	scrapyConsumerCount := 20
	wg.Add(scrapyConsumerCount)
	var scrapych = make(chan bson.M, scrapyConsumerCount)
	go scrapy.ScrapyProduce(scrapych, &wg)
	for i := 0; i < scrapyConsumerCount; i++ {
		go scrapy.ScrapyConsumer(scrapych, &wg, i)
	}
	// 等待程序都结束才停止执行
	wg.Wait()
}
