package steplock

type Step struct {
	File string
}

// 获取爬取标记
func (step *Step) GetScrapyFlag() (int, int) {
	// 文件不存在则创建
	sinfo := readFile(step.File)
	return sinfo.Offset, sinfo.Limit
}

// 设置爬取标记
func (step *Step) SetScrapyFlag(offset int, limit int) bool {
	sInfo := ScrapyInfo{
		Offset: offset,
		Limit:  limit,
	}
	return writeFile(step.File, sInfo)
}
