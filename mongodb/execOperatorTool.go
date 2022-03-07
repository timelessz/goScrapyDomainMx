package gomongo

import (
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
	"scrapyDomain/mysql"
)

// map
// 获取limit 客户数据
func GetCrmSuffixData() map[string]mysql.MxSuffix {
	options := options.Find()
	filter := bson.M{
		"brand_id": bson.M{
			"$ne": "",
		},
	}
	mxsuffix := Instance.FindMany("mxsuffix", filter, options)
	suffixMap := make(map[string]mysql.MxSuffix)
	for _, s := range mxsuffix {
		//v, _ := Customer["name"].(string)
		suffix := s["suffix"].(string)
		if suffix == "" {
			continue
		}
		sd := mysql.MxSuffix{
			BId:    s["brand_id"].(string),
			Suffix: s["suffix"].(string),
			Name:   s["name"].(string),
		}
		suffixMap[suffix] = sd
	}
	return suffixMap
}
