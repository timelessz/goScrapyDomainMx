package tool

import (
	"fmt"
	"github.com/forease/gotld"
	"io/ioutil"
	"net/http"
	"net/url"
	"os/exec"
	"regexp"
	"sort"
	"time"
)

// 截取出域名
func SubDomain(url string) []string {
	if url == "" {
		return []string{}
	}
	domains := ExecSubDmmain(url)
	black := []string{"1688.com", "made-in-china.com", "tmall.com", "taobao.com", "58.com", "alibaba.com", "madeinchina"}
	rdomains := []string{}
	for _, d := range domains {
		if !IsHave(d, black) {
			rdomains = append(rdomains, d)
		}
	}
	return rdomains
}

// 截取域名
func ExecSubDmmain(url string) []string {
	if url == "" {
		return nil
	}
	pattern := "[a-zA-Z0-9][-a-zA-Z0-9]{0,62}(\\.[a-zA-Z0-9][-a-zA-Z0-9]{0,62})+\\.?"
	r, _ := regexp.Compile(pattern)
	urls := r.FindAllString(url, -1)
	domains := make([]string, 0)
	tempMap := map[string]byte{} // 存放不重复主键
	for _, value := range urls {
		domain := GetDomainTldDomain(value)
		if domain == "" {
			continue
		}
		l := len(tempMap)
		tempMap[domain] = 0    //当e存在于tempMap中时，再次添加是添加不进去的，，因为key不允许重复
		if len(tempMap) != l { // 加入map后，map长度变化，则元素不重复
			domains = append(domains, domain)
		}
	}
	return domains
}

// 是否包含
func IsHave(target string, str_array []string) bool {
	sort.Strings(str_array)
	index := sort.SearchStrings(str_array, target)
	//index的取值：0 ~ (len(str_array)-1)
	return index < len(str_array) && str_array[index] == target
}

// 截取 mx 后缀的主域名
func GetUrlTldDomain(urls string) string {
	u, err := url.Parse(urls)
	if err != nil {
		return ""
	}
	_, domain, err := gotld.GetTld("http://" + u.Host)
	if nil != err {
		fmt.Println(err)
		return ""
	}
	return domain
}

// 截取url 对应的 域名
func GetDomainTldDomain(urls string) string {
	_, domain, err := gotld.GetTld(urls)
	if nil != err {
		fmt.Println(err)
		return ""
	}
	return domain
}

// 执行dig 命令
func ExecDigCommand(domain string) string {
	cmd := exec.Command("dig", "-t", "mx", "+short", domain)
	out, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Println(err)
	}
	return string(out)
}

// 读取目录文件
func GetAllFile(pathname string, s []string) ([]string, error) {
	rd, err := ioutil.ReadDir(pathname)
	if err != nil {
		fmt.Println("read dir fail:", err)
		return s, err
	}
	for _, fi := range rd {
		if fi.IsDir() {
			fullDir := pathname + "/" + fi.Name()
			s, err = GetAllFile(fullDir, s)
			if err != nil {
				fmt.Println("read dir fail:", err)
				return s, err
			}
		} else {
			fullName := pathname + "/" + fi.Name()
			s = append(s, fullName)
		}
	}
	return s, nil
}

// 发送 get 请求
func SendRequest(msg string) {
	Url, err := url.Parse("https://salesman.cc/index.php/Shuaidan_ceshi/PublicTry/push/string/" + url.QueryEscape(msg))
	if err != nil {
		panic(err.Error())
	}
	//如果参数中有中文参数,这个方法会进行URLEncode
	urlPath := Url.String()
	resp, err := http.Get(urlPath)
	defer resp.Body.Close()
	s, err := ioutil.ReadAll(resp.Body)
	fmt.Println(string(s))
}

// 日期转换为 周几
func ZellerFunction2Week(year, month, day uint16) (string, int) {
	var y, m, c uint16
	if month >= 3 {
		m = month
		y = year % 100
		c = year / 100
	} else {
		m = month + 12
		y = (year - 1) % 100
		c = (year - 1) / 100
	}
	week := y + (y / 4) + (c / 4) - 2*c + ((26 * (m + 1)) / 10) + day - 1
	if week < 0 {
		week = 7 - (-week)%7
	} else {
		week = week % 7
	}
	which_week := int(week)
	var weekday = [7]string{"周日", "周一", "周二", "周三", "周四", "周五", "周六"}
	return weekday[which_week], which_week
}

// 验证是否是工作日
func CheckIsWorking() bool {
	now := time.Now()
	hour := now.Hour()
	Weekday := int(now.Weekday())
	if (Weekday == 0 || Weekday == 6) || !(hour > 9 && hour < 18) {
		return false
	}
	return true
}
