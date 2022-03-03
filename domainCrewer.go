package main

import (
	"errors"
	"scrapyDomain/mysql"
	"strconv"
	"strings"
)

func AnalyseMxRecord(mxrecord string) string {
	arr := strings.Split(mxrecord, "\r\n")
	var mx string
	prority := 0
	for _, v := range arr {
		if v == "" {
			continue
		}
		arr := strings.Split(v, " ")
		if len(arr) == 2 {
			cprority, _ := strconv.Atoi(arr[0])
			if cprority < prority || prority == 0 {
				mx = arr[1]
			}
		}
	}
	if mx != "" {
		return mx[0 : len(mx)-1]
	}
	return mx
}

func GetMxRecordSuffix(suffixMap map[string]mysql.MxSuffix, suffix string) (mysql.MxSuffix, error) {
	if _, ok := suffixMap[suffix]; ok {
		mss := suffixMap[suffix]
		return mss, nil
	}
	return mysql.MxSuffix{}, errors.New("未匹配到MX数据")
}

//func saveCustomerMxInfo(db *gorm.DB, mss mysql.MxSuffix, domain string, v orm.Customer, mxrecord string, i int) {
//	fmt.Println(strconv.Itoa(i) + "号消费者：" + v.Name.String + " 域名：" + domain + " 获取mx:" + mxrecord)
//	if mss != (mysql.MxSuffix{}) {
//		// 判断非空 struct 表示匹配到mx 情况
//		BId, _ := strconv.ParseInt(mss.BId, 10, 64)
//		if v.MxBrandID.Int64 != BId {
//			//更新数据
//			v.MxBrandID.Int64 = BId
//			v.MxBrandName.String = mss.Name
//			v.Mxrecord.String = mxrecord
//			fmt.Println(strconv.Itoa(i) + "消费者：" + v.Domain.String + "保存mx信息，匹配到邮箱品牌，品牌：" + mss.Name)
//			db.Save(v)
//		}
//	} else {
//		// 判断 struct 为空 未匹配到品牌
//		if v.Mxrecord.String != mxrecord {
//			v.Mxrecord.String = mxrecord
//			fmt.Println(strconv.Itoa(i) + "消费者：" + v.Domain.String + "保存mx信息，未匹配到邮箱品牌。")
//			db.Save(v)
//		}
//	}
//}

//func saveCustomerInfo(db *gorm.DB, v orm.Customer, mailTitle string, selfBuildBrandId int, selfbuildBrandName string, domainTitle string, contactBrandId int, contactBrandName string) {
//	changeSatus := false
//	if v.MailTitle.String != mailTitle {
//		v.MailTitle.String = mailTitle
//		changeSatus = true
//	}
//	if v.Title.String != domainTitle {
//		v.Title.String = domainTitle
//		changeSatus = true
//	}
//	if v.SelfbuildBrandID.Int64 != int64(selfBuildBrandId) {
//		v.SelfbuildBrandID.Int64 = int64(selfBuildBrandId)
//		v.SelfbuildBrandName.String = selfbuildBrandName
//		changeSatus = true
//	}
//	if v.ContacttoolBrandID.Int64 != int64(contactBrandId) {
//		v.ContacttoolBrandID.Int64 = int64(contactBrandId)
//		v.ContacttoolBrandName.String = contactBrandName
//		changeSatus = true
//	}
//	if changeSatus {
//		db.Save(v)
//	}
//}

// 保存客户域名
//func saveCustomerDomain(db *gorm.DB, domain string, v orm.Customer, i int) {
//	fmt.Println(strconv.Itoa(i) + "号消费者：" + v.Name.String + "更新域名：" + domain)
//	db.Save(v)
//}
