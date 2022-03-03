package main

// 追加元素  https://cloud.tencent.com/developer/ask/sof/402516
func main() {
	//tools := tool.SubDomain("山东锦艺装饰有限公司。sdjybh.com。wfjybh.com")
	//fmt.Println(tools)
	//importData.Init()
	// pinyinConvert()
	//importData.InitData()
	//importData.InitMData()
	//importData.InitGData()
	//return
	StartScrapy()
	//gomongo.Instance.InsertOne("customer", customers)
	/*
		crmDatabaseConfigMap := map[string]string{
			"MysqlHost":   "rdsfjnifbfjnifbo.mysql.rds.aliyuncs.com",
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
	*/
}

//

//func pinyinConvert() {
//	s := "赵兴壮"
//	t := pinyin.LazyConvert(s, nil)
//	fmt.Println(strings.Join(t, ""))
//}
