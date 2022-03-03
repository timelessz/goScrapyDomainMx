package tool

import (
	"fmt"
	"github.com/forease/gotld"
	"io/ioutil"
	"net/url"
	"os/exec"
	"regexp"
	"sort"
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
