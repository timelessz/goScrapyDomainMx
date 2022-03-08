package scrapy

import (
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
	gomongo "scrapyDomain/mongodb"
	"scrapyDomain/selenium"
	"scrapyDomain/steplock"
	"strconv"
	"strings"
	"sync"
)

// 聊天方式
var ConactMap = map[string]map[int]string{
	"qiyukf.com":    {1: "七鱼智能客服"},
	"53kf.com":      {2: "53kf"},
	"udesk.cn":      {3: "U-desk"},
	"easemob.com":   {4: "环信"},
	"meiqia.com":    {5: "美洽"},
	"sobot.com":     {6: "智齿"},
	"xiaoneng.cn":   {7: "小能"},
	"youkesdk.com":  {8: "有客云"},
	"live800.com":   {9: "Live800"},
	"b.qq.com":      {10: "营销QQ"},
	"bizapp.qq.com": {10: "营销QQ2"},
	"workec.com":    {11: "EC企信"},
	"looyu.com":     {12: "乐语"},
	"tq.cn":         {13: "TQ洽谈通"},
	"zoosnet.net":   {14: "网站商务通"},
	"talk99.cn":     {15: "Talk99"},
	"kf5.com":       {16: "逸创云客服"},
	"easyliao.com":  {17: "易聊"},
}

// 域名自建相关数据
var MailSelfBuildMap = map[string]map[int]string{
	"coremail":        {1: "盈世"},
	"fangmail":        {2: "方向标"},
	"winmail":         {3: "winmail"},
	"anymacro":        {4: "安宁"},
	"turbomail":       {5: "TurboMail"},
	"u-mail":          {6: "U-Mail"},
	"exchange":        {7: "Exchange"},
	"microsoftonline": {8: "微软Office365"},
	"NiceWebMail":     {9: "NiceWebMail"},
	"/owa/auth.owa":   {10: "微软outlook"},
}

// 获取 使用 selenium 的数据
func ScrapyProduce(ch chan<- bson.M, wg *sync.WaitGroup) {
	fileName := "scrapylock.txt"
	step := steplock.Step{
		File: fileName,
	}
	for true {
		offset, limit := step.GetScrapyFlag()
		if limit == 0 {
			limit = 50
		}
		options := options.Find()
		//options.SetSort(bson.M{"": 1})
		options.SetSkip(int64(offset))
		options.SetLimit(int64(limit))
		options.SetAllowDiskUse(true)
		filter := bson.M{
			"domain": bson.M{"$ne": bson.A{}},
		}
		Customers := gomongo.Instance.FindMany("customer", filter, options)
		fmt.Println(len(Customers))
		if len(Customers) == 0 {
			// 表示未获取到数据
			fmt.Println("mx 数据爬取生产者，未获取到数据")
			step.SetScrapyFlag(0, limit)
			continue
		}
		for _, Customer := range Customers {
			ch <- Customer
		}
		// 设置已经爬取到的数据
		step.SetScrapyFlag(offset+limit, limit)
	}
	wg.Done()
}

func ScrapyConsumer(ch <-chan bson.M, wg *sync.WaitGroup, i int) {
	//db := getDB()
	service, wd := selenium.GetWebDriver()
	defer service.Stop()
	defer wd.Quit()
	for true {
		v := <-ch
		mongoId := v["_id"]
		mongoIds := mongoId.(primitive.ObjectID).Hex()
		domains := v["domain"]
		if domainArr, ok := domains.(primitive.A); ok {
			dA := []interface{}(domainArr)
			for _, d := range dA {
				dm, okd := d.(primitive.M)
				if !okd {
					continue
				}
				s := fmt.Sprint(dm["domain"])
				///////////////////////////////////////////////
				// 爬取mail 网站信息  自建邮箱数据获取
				mailTitle, mailSource := selenium.Scrapy(wd, "http://mail."+s)
				selfBuildBrandId, selfbuildBrandName := matchSelfBuild(&mailSource)
				fmt.Println(mailTitle, selfBuildBrandId, selfbuildBrandName)
				// 爬取www 网站标题  加获取 咨询工具 记录
				domainTitle, domainSource := selenium.Scrapy(wd, "http://"+s)
				contactBrandId, contactBrandName := matchSelfBuild(&domainSource, "contacttool")
				fmt.Println(domainTitle, contactBrandId, contactBrandName)
				saveCustomerInfo(dm, mongoIds, s, mailTitle, selfBuildBrandId, selfbuildBrandName, domainTitle, contactBrandId, contactBrandName)
				///////////////////////////////////////////
			}
		}
	}
	wg.Done()
}

//
func saveCustomerInfo(dm primitive.M, mongodbId string, domain string, mailTitle string, selfBuildBrandId int, selfbuildBrandName string, domainTitle string, contactBrandId int, contactBrandName string) {
	oid, _ := primitive.ObjectIDFromHex(mongodbId)
	filter := bson.D{{"_id", oid}, {"domain.domain", domain}}
	title := fmt.Sprint(dm["title"])
	mail_title := fmt.Sprint(dm["mail_title"])
	selfbuild_brand_id := fmt.Sprint(dm["selfbuild_brand_id"])
	contacttool_brand_id := fmt.Sprint(dm["contacttool_brand_id"])
	update := bson.M{}
	if domainTitle != "" && domainTitle != title {
		update["domain.$.title"] = domainTitle
	}
	if mailTitle != "" && mailTitle != mail_title {
		update["domain.$.mail_title"] = mailTitle
	}
	// 联系方式
	if selfbuild_brand_id != "" && selfbuild_brand_id != strconv.Itoa(selfBuildBrandId) {
		update["domain.$.selfbuild_brand_id"] = selfBuildBrandId
		update["domain.$.selfbuild_brand_name"] = selfbuildBrandName
	}
	// 联系方式
	if contacttool_brand_id != "" && contacttool_brand_id != strconv.Itoa(contactBrandId) {
		update["domain.$.contacttool_brand_id"] = contactBrandId
		update["domain.$.contacttool_brand_name"] = contactBrandName
	}
	if update != nil {
		fmt.Println(domain + "更新数据")
		fmt.Println(update)
		result := gomongo.Instance.UpdateOne("customer", filter, bson.M{"$set": update})
		fmt.Println(result)
	}
}

// 匹配邮箱自建客户数据
func matchSelfBuild(pageSource *string, flag ...string) (int, string) {
	var f string = "mailself" // 默认值
	if len(flag) == 1 {
		f = flag[0] // 非默认值
	}
	Smap := map[string]map[int]string{}
	if f == "mailself" {
		Smap = MailSelfBuildMap
	} else {
		Smap = ConactMap
	}
	for domains, brandInfo := range Smap {
		if find := strings.Contains(*pageSource, domains); find {
			for key, value := range brandInfo {
				return key, value
			}
		}
	}
	return 0, ""
}
