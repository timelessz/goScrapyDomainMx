package main

import (
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
	gomongo "scrapyDomain/mongodb"
	"scrapyDomain/mysql"
	"scrapyDomain/scrapy"
	"scrapyDomain/steplock"
	"scrapyDomain/tool"
	"strconv"
	"sync"
	"time"
)

func produce(ch chan<- bson.M, wg *sync.WaitGroup) {
	fileName := "scrapy.txt"
	step := steplock.Step{
		File: fileName,
	}
	for true {
		offset, limit := step.GetScrapyFlag()
		if limit == 0 {
			limit = 500
		}
		options := options.Find()
		options.SetSort(bson.M{"_id": 1})
		options.SetSkip(int64(offset))
		options.SetLimit(int64(limit))
		options.SetAllowDiskUse(true)
		filter := bson.M{
			//"domain": bson.M{"$ne": bson.A{}},
		}
		Customers := gomongo.Instance.FindMany("customer", filter, options)
		if len(Customers) == 0 {
			// 表示未获取到数据
			fmt.Println("mx 数据爬取生产者，未获取到数据")
			//step.SetScrapyFlag(0, limit)
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
		// 工作日不处理数据
		if tool.CheckIsWorking() {
			tool.Logs{File: "sleep.txt"}.AddLog(strconv.Itoa(i) + "号MX爬取进程，工作时间暂停执行")
			time.Sleep(1 * time.Minute)
			continue
		}
		v := <-ch
		mongoId := v["_id"]
		mongoIds := mongoId.(primitive.ObjectID).Hex()
		domains := v["domain"]
		name := ""
		cn_name, _ := v["name"].(string)
		en_name, _ := v["en_name"].(string)
		if cn_name != "" {
			name = cn_name
		} else {
			name = en_name
		}
		if domainArr, ok := domains.(primitive.A); ok {
			dA := []interface{}(domainArr)
			for _, d := range dA {
				dm, okd := d.(primitive.M)
				if !okd {
					continue
				}
				s := fmt.Sprint(dm["domain"])
				brand_id := fmt.Sprint(dm["brand_id"])
				mx_record := fmt.Sprint(dm["mx_record"])
				// 获取mx记录
				fmt.Println(s)
				mxrecord := tool.ExecDigCommand(s)
				if mxrecord == "" {
					// website 获取数据为空
					//fmt.Println(name + s + "获取MXRECORD为空")
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
				//fmt.Println(name + s + "获取MXRECORD为" + mxrecord)
				if err != nil {
					fmt.Println(err.Error())
				}
				SaveCustomerMxInfo(mss, s, brand_id, mx_record, mongoIds, mxrecord, name)
			}
		}
	}
	wg.Done()
}

/**
保存 客户mx 信息
*/
func SaveCustomerMxInfo(mss mysql.MxSuffix, domain string, pre_brand_id string, pre_mx_record string, mongodbId string, mxrecord string, companyName string) {
	oid, _ := primitive.ObjectIDFromHex(mongodbId)
	filter := bson.D{{"_id", oid}, {"domain.domain", domain}}
	update := bson.M{}
	if pre_mx_record != mxrecord {
		update["domain.$.mxrecord"] = mxrecord
		logsr := companyName + " " + domain + "的mx 记录从" + pre_mx_record + "变更为：" + mxrecord
		//tool.SendRequest(logsr)
		tool.Logs{File: "changerecord.txt"}.AddLog(logsr)
	}
	if mss != (mysql.MxSuffix{}) && pre_brand_id != mss.BId {
		update["domain.$.mx_brand_id"] = mss.BId
		update["domain.$.mx_brand_name"] = mss.Name
		// 品牌变化
		loginfo := companyName + " " + domain + "的邮箱品牌变更为" + mss.Name + " mx 记录变更为：" + mxrecord
		//tool.SendRequest(loginfo)
		tool.Logs{File: "changerecord.txt"}.AddLog(loginfo)
	} else {
		//fmt.Println("MX品牌未变化")
	}
	if update != nil {
		result := gomongo.Instance.UpdateOne("customer", filter, bson.M{"$set": update})
		fmt.Println(result)
	} else {
		//无需更新
	}
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

// 初始化数据库链接信息
func initDb() {
	bDatabaseConfigMap := map[string]string{
		"MysqlHost":   "rdsfjnifbfjnifbo.mysql.rds.aliyuncs.com",
		"MysqlDbname": "salesmenbeta2",
		"MysqLUser":   "salesmen",
		"MysqlPasswd": "qiangbi123",
	}
	mysql.InitInstance(bDatabaseConfigMap)
	//const uri = "mongodb://admin:qiangbi123@144.123.173.6:27017"
	const uri = "mongodb://admin:qiangbi123@192.168.2.155:27017"
	gomongo.MustConnect(uri, "bigbusiness")
}

func StartScrapy() {
	initDb()
	var wg sync.WaitGroup
	consumerCount := 100
	wg.Add(consumerCount)
	var ch = make(chan bson.M, consumerCount)
	go produce(ch, &wg)
	suffixMap := gomongo.GetCrmSuffixData()
	for i := 0; i < consumerCount; i++ {
		go consumer(ch, &wg, suffixMap, i)
	}
	//网站 www 爬取
	scrapyConsumerCount := 10
	wg.Add(scrapyConsumerCount)
	var scrapych = make(chan bson.M, scrapyConsumerCount)
	go scrapy.ScrapyProduce(scrapych, &wg)
	for i := 0; i < scrapyConsumerCount; i++ {
		go scrapy.ScrapyConsumer(scrapych, &wg, i)
	}
	//等待程序都结束才停止执行
	wg.Wait()
}
