package config

import (
	"encoding/json"
	"fmt"
	"github.com/go-errors/errors"
	"github.com/hpcloud/tail"
	"io/ioutil"
	"os"
	"regexp"
	"strings"
	"time"
	"path/filepath"
	"log"
)

type Config struct {
	Metric     string      `json:"metric"` //度量名称,比如log.console 或者log
	Timer      int         `json:"timer"` // 每隔多长时间（秒）上报
	Host       string      `json:"host"` //主机名称
	Agent      string      `json:"agent"` //agent api url
	WatchFiles []WatchFile `json:"files"`
	LogLevel   string
}

type resultFile struct {
	FileName string
	ModTime  time.Time
	LogTail  *tail.Tail
}

type WatchFile struct {
	Path       string 	`json:"path"`//路径
	FilePattern  string		`json:"filepattern"`
	FilePatternExp *regexp.Regexp `json:"-"`
	Keywords   []keyWord	`json:"keywords"`
	PathIsFile bool       //path 是否是文件
	ResultFile resultFile `json:"-"`
	Close_chan chan bool `json:"-"`
}


type keyWord struct {
	Exp      string		`json:"exp"`
	Tag      string		`json:"tag"`
	Type     string		`json:"type"`
	FixedExp string         `json:"-"` //替换
	Regex    *regexp.Regexp `json:"-"`
}

//说明：这7个字段都是必须指定
type PushData struct {
	Metric    string  `json:"metric"`    //统计纬度
	Endpoint  string  `json:"endpoint"`  //主机
	Timestamp int64   `json:"timestamp"` //unix时间戳,秒
	Value     float64 `json:"value"`     // 代表该metric在当前时间点的值
	Step      int     `json:"step"`      //  表示该数据采集项的汇报周期，这对于后续的配置监控策略很重要，必须明确指定。
	//COUNTER：指标在存储和展现的时候，会被计算为speed，即（当前值 - 上次值）/ 时间间隔
	//COUNTER：指标在存储和展现的时候，会被计算为speed，即（当前值 - 上次值）/ 时间间隔

	CounterType string `json:"counterType"` //只能是COUNTER或者GAUGE二选一，前者表示该数据采集项为计时器类型，后者表示其为原值 (注意大小写)
	//GAUGE：即用户上传什么样的值，就原封不动的存储
	//COUNTER：指标在存储和展现的时候，会被计算为speed，即（当前值 - 上次值）/ 时间间隔
	Tags string `json:"tags"` //一组逗号分割的键值对, 对metric进一步描述和细化, 可以是空字符串. 比如idc=lg，比如service=xbox等，多个tag之间用逗号分割
	Count int `json:"-"`  // 辅助变量  用于求平均数
}

const ConfigFile = "./cfg.json"

var (
	Cfg         *Config
	fixExpRegex = regexp.MustCompile(`[\W]+`)
	Tem_cfg		*Config
)


func Init_config() error {
	var err error

	if Tem_cfg, err = ReadConfig(ConfigFile); err != nil {
		log.Println("ERROR: ", err)
		return err
	}
	log.Println("read cfg success")

	if err = CheckConfig(Tem_cfg); err != nil {
		log.Println(err)
		return err
	}
	log.Println("check cfg success")

	if err = SetLogFile(Tem_cfg); err != nil {
		log.Println(err)
		return err
	}
	log.Println("set cfg success")
	Cfg = Tem_cfg

	fmt.Printf("INFO: config: %+v", *Cfg)
	return nil
}

func ReadConfig(configFile string) (*Config, error) {
	var config *Config
	//config = new(Config)
	bytes, err := ioutil.ReadFile(configFile)
	if err != nil {
		return config, err
	}

	if err := json.Unmarshal(bytes, &config); err != nil {
		return config, err
	}

	fmt.Println(config.LogLevel)

	log.Println("config init success, start to work ...")
	return config, nil
}

// 检查配置项目是否正确
func CheckConfig(config *Config) error {
	var err error
	//检查 host
	if config.Host == "" {
		if config.Host, err = os.Hostname(); err != nil {
			return err
		}

		log.Println("host not set will use system's name:", config.Host)

	}

	for i, v := range config.WatchFiles {
		//检查路径
		fInfo, err := os.Stat(v.Path)
		if err != nil {
			return err
		}
		log.Println(v.Path)
		if os.IsNotExist(err) {
			return err
		}

		if !fInfo.IsDir() {
			config.WatchFiles[i].PathIsFile = true
		}

		config.WatchFiles[i].Close_chan = make(chan bool)


		if config.WatchFiles[i].FilePattern == "" {
			log.Println("file pre ", config.WatchFiles[i].Path, "filematch is no set, will use \\.*")
			config.WatchFiles[i].FilePattern = "\\.*"
			// errors.New("ERROR: filematch must set ")
		}
		if config.WatchFiles[i].FilePatternExp, err = regexp.Compile(config.WatchFiles[i].FilePattern); err != nil {
			return err
		}

		//检查keywords
		if len(v.Keywords) == 0 {
			return errors.New("ERROR: keyword list not set")
		}

		for i, keyword := range v.Keywords {
			if keyword.Exp == "" || keyword.Tag == "" {
				return errors.New("ERROR: keyword's exp and tag are requierd")
			}
			if keyword.Type == "" {
				v.Keywords[i].Type = "count"
			}
			keyword.Type = v.Keywords[i].Type
			if keyword.Type != "count" && keyword.Type != "avg" && keyword.Type != "min" && keyword.Type != "max" && keyword.Type != "sum" {
				return errors.New("ERROR: keyword Type must in count avg min max")
			}
		}

		// 设置正则表达式
		for j, keyword := range v.Keywords {

			if config.WatchFiles[i].Keywords[j].Regex, err = regexp.Compile(keyword.Exp); err != nil {
				return err
			}

			log.Println("INFO: tag:", keyword.Tag, "regex", config.WatchFiles[i].Keywords[j].Regex.String())

			config.WatchFiles[i].Keywords[j].FixedExp = string(fixExpRegex.ReplaceAll([]byte(keyword.Exp), []byte(".")))
		}
	}

	return nil
}

func SetLogFile(c *Config) error {
	for i, v := range c.WatchFiles {
		if v.PathIsFile {
			c.WatchFiles[i].ResultFile.FileName = v.Path
			continue
		}

		filepath.Walk(v.Path, func(path string, info os.FileInfo, err error) error {
			cfgPath := v.Path
			if strings.HasSuffix(cfgPath, "/") {
				cfgPath = string([]rune(cfgPath)[:len(cfgPath)-1])
			}
			log.Println(path)

			//只读取root目录的log
			if filepath.Dir(path) != cfgPath && info.IsDir() {
				log.Println(path, "not in root path, ignoring , Dir:", path, "cofig path:", cfgPath)
				return err
			}

			if v.FilePatternExp.MatchString(filepath.Base(path)) && !info.IsDir() {
				if c.WatchFiles[i].ResultFile.FileName == "" || info.ModTime().After(c.WatchFiles[i].ResultFile.ModTime) {
					c.WatchFiles[i].ResultFile.FileName = path
					c.WatchFiles[i].ResultFile.ModTime = info.ModTime()
				}
				return err
			}
			return err
		})

	}
	return nil
}


