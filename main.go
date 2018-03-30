package main

import (
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"time"
	"encoding/json"
	"strconv"

	"github.com/fsnotify/fsnotify"
	"github.com/hpcloud/tail"
	"github.com/streamrail/concurrent-map"

	"./config"
	"./log"
	"./config_server"
)

var (
	workers  chan bool
	keywords cmap.ConcurrentMap
)

func main() {
	if err := config.Init_config(); err != nil {
		return
	}
	workers = make(chan bool, runtime.NumCPU()*2)
	keywords = cmap.New()
	runtime.GOMAXPROCS(runtime.NumCPU())
	go func() {
		for {
			old_tick := config.Cfg.Timer
			ticker := time.NewTicker(time.Second * time.Duration(int64(config.Cfg.Timer)))
			for range ticker.C {
				if config.Cfg.Timer != old_tick {
					old_tick = config.Cfg.Timer
					break
				}
				fillData()
				postData()
			}
		}
	}()
	go func() {
		for i := 0; i < len(config.Cfg.WatchFiles); i++ {
			readFileAndSetTail(&(config.Cfg.WatchFiles[i]))
			go logFileWatcher(&(config.Cfg.WatchFiles[i]))
		}
	}()
	go func() {
		ConfigFileWatcher()
	}()
	config_server.Push_handler()
}

//配置文件监控,可以实现热更新
func ConfigFileWatcher() {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}
	defer watcher.Close()
	done := make(chan bool)
	go func() {
		for {
			select {
			case event := <-watcher.Events:
				if event.Name == config.ConfigFile && event.Op == fsnotify.Write {
					log.Debug("event : modified config file", event.Name, "will reaload config", event.Op)
					old_cfg := config.Cfg
					if new_config, err := config.ReadConfig(config.ConfigFile); err != nil {
						log.Debug("ERROR: event: config has error, will not use old config", err)
					} else if config.CheckConfig(new_config) != nil {
						log.Debug("ERROR: event: config has error, will not use old config", err)
					} else if config.SetLogFile(new_config) != nil {
						log.Debug("ERROR: event: config has error, will not use old config", err)
					} else {
						log.Debug("event: config reload success", )
						log.Debug("event: new config:", new_config)
						log.Debug("event: old watcher all killed success")
						for _, v := range old_cfg.WatchFiles {
							if v.ResultFile.LogTail != nil {
								log.Debug("event: try to stop old tail")
								v.Close_chan <- true
								v.ResultFile.LogTail.Stop()
							}
						}
						log.Debug("event: stop all old tail -f ")

						log.Debug("use new config")
						config.Cfg = new_config
						for i := 0; i < len(config.Cfg.WatchFiles); i++ {
							log.Debug("event: try to start new tail")
							readFileAndSetTail(&(config.Cfg.WatchFiles[i]))
							go logFileWatcher(&(config.Cfg.WatchFiles[i]))

						}
						log.Debug("event: satrt new file tail success")

					}

				}
			case err := <-watcher.Errors:
				log.Fatal(err)
			}
		}
	}()

	err = watcher.Add(".")
	if err != nil {
		log.Fatal(err)
	}
	<-done
}

func logFileWatcher(file *config.WatchFile) {
	logTail := file.ResultFile.LogTail
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}
	defer watcher.Close()

	done := make(chan bool)

	go func() {
		log.Debug("event: log file watcher start --- ", file.ResultFile.FileName)
		for {
			select {
			case <- file.Close_chan:
				log.Debug("event: log file watcher stoped --- ", file.ResultFile.FileName)
				break
			case event := <-watcher.Events:
				if file.PathIsFile && event.Op == fsnotify.Create && event.Name == file.Path {
					log.Info("continue to watch file:", event.Name)
					if file.ResultFile.LogTail != nil {
						logTail.Stop()
					}
					readFileAndSetTail(file)
				} else {
					if file.ResultFile.FileName == event.Name && (event.Op == fsnotify.Remove || event.Op == fsnotify.Rename) {
						log.Warn(event, "stop to tail")
						if file.ResultFile.LogTail != nil {
							file.ResultFile.LogTail.Stop()
						}
					} else if event.Op == fsnotify.Create {
						log.Infof("created file %v, basePath:%v", event.Name, path.Base(event.Name))
						//if strings.HasSuffix(event.Name, file.Suffix) && strings.HasPrefix(path.Base(event.Name), file.Prefix) {
						if file.FilePatternExp.MatchString(filepath.Base(event.Name)) {
							if file.ResultFile.LogTail != nil {
								file.ResultFile.LogTail.Stop()
							}
							file.ResultFile.FileName = event.Name
							readFileAndSetTail(file)
						}
					}
				}
			case err := <-watcher.Errors:
				log.Error(err)
			}
		}
	}()

	watchPath := file.Path
	if file.PathIsFile {
		watchPath = filepath.Dir(file.Path)
	}
	err = watcher.Add(watchPath)
	if err != nil {
		log.Fatal(err)

	}
	<-done
}

func readFileAndSetTail(file *config.WatchFile) {
	if file.ResultFile.FileName == "" {
		return
	}
	_, err := os.Stat(file.ResultFile.FileName)
	if err != nil {
		log.Error(file.ResultFile.FileName, err)
		return
	}

	log.Info("event:  read file", file.ResultFile.FileName, file)
	tail_end, err := tail.TailFile(file.ResultFile.FileName, tail.Config{Follow: true, Location: &tail.SeekInfo{Offset: 0, Whence: os.SEEK_END}})
	if err != nil {
		log.Fatal(err)
	}

	file.ResultFile.LogTail = tail_end
	log.Debug("event: will start tail", file.ResultFile.FileName)
	go func() {
		for line := range tail_end.Lines {
			handleKeywords(*file, line.Text)
		}
	}()

}

// 查找关键词
func handleKeywords(file config.WatchFile, line string) {
	for _, p := range file.Keywords {
		switch p.Type {
		case "count":
			value := 0.0
			if p.Regex.MatchString(line) {
				value = 1.0
			}
			key := file.Path + file.FilePattern + p.Tag
			var data config.PushData
			if v, ok := keywords.Get(key); ok {
				d := v.(config.PushData)
				d.Value += value
				data = d
			} else {
				data = config.PushData{Metric: config.Cfg.Metric,
					Endpoint:    config.Cfg.Host,
					Timestamp:   time.Now().Unix(),
					Value:       value,
					Step:        config.Cfg.Timer,
					CounterType: "GAUGE",
					Tags:		"path="+file.Path+",filepattern="+file.FilePattern+",tag="+p.Tag,
				}
			}
			keywords.Set(key, data)
		case "min":
			new_value_array :=  p.Regex.FindStringSubmatch(line)
			if len(new_value_array) > 2 {
				new_value := new_value_array[1]
				new_value_float, err := strconv.ParseFloat(new_value, 64)
				if err != nil {
					log.Error("")
					continue
				}
				key := file.Path + file.FilePattern + p.Tag
				var data config.PushData
				if v, ok := keywords.Get(key); ok {
					d := v.(config.PushData)
					if new_value_float < d.Value {
						d.Value = new_value_float
					}
					data = d
				} else {
					data = config.PushData{Metric: config.Cfg.Metric,
						Endpoint:    config.Cfg.Host,
						Timestamp:   time.Now().Unix(),
						Value:       new_value_float,
						Step:        config.Cfg.Timer,
						CounterType: "GAUGE",
						Tags:		"path="+file.Path+",filepattern="+file.FilePattern+",tag="+p.Tag,
					}
				}
				keywords.Set(key, data)
			} else {
				log.Debug("no match")
			}
		case "max":
			new_value_array :=  p.Regex.FindStringSubmatch(line)
			if len(new_value_array) > 2 {
				new_value := new_value_array[1]
				new_value_float, err := strconv.ParseFloat(new_value, 64)
				if err != nil {
					log.Error("")
					continue
				}
				key := file.Path + file.FilePattern + p.Tag
				var data config.PushData
				if v, ok := keywords.Get(key); ok {
					d := v.(config.PushData)
					if new_value_float > d.Value {
						d.Value = new_value_float
					}
					data = d
				} else {
					data = config.PushData{Metric: config.Cfg.Metric,
						Endpoint:    config.Cfg.Host,
						Timestamp:   time.Now().Unix(),
						Value:       new_value_float,
						Step:        config.Cfg.Timer,
						CounterType: "GAUGE",
						Tags:		"path="+file.Path+",filepattern="+file.FilePattern+",tag="+p.Tag,
					}
				}
				keywords.Set(key, data)
			} else {
				log.Debug("no match")
			}
		case "avg":
			new_value_array :=  p.Regex.FindStringSubmatch(line)
			if len(new_value_array) > 2 {
				new_value := new_value_array[1]
				new_value_float, err := strconv.ParseFloat(new_value, 64)
				if err != nil {
					log.Error("")
					continue
				}
				key := file.Path + file.FilePattern + p.Tag
				var data config.PushData
				if v, ok := keywords.Get(key); ok {
					d := v.(config.PushData)
					d.Value = (d.Value*float64(d.Count)+new_value_float)/(1.0+float64(d.Count))
					d.Count += 1
					data = d
				} else {
					data = config.PushData{Metric: config.Cfg.Metric,
						Endpoint:    config.Cfg.Host,
						Timestamp:   time.Now().Unix(),
						Value:       new_value_float,
						Step:        config.Cfg.Timer,
						CounterType: "GAUGE",
						Tags:		"path="+file.Path+",filepattern="+file.FilePattern+",tag="+p.Tag,
						Count: 1,
					}
				}
				keywords.Set(key, data)
			} else {
				log.Debug("no match")
			}
		case "sum":
			new_value_array :=  p.Regex.FindStringSubmatch(line)
			if len(new_value_array) > 2 {
				new_value := new_value_array[1]
				new_value_float, err := strconv.ParseFloat(new_value, 64)
				if err != nil {
					log.Error("")
					continue
				}
				key := file.Path + file.FilePattern + p.Tag
				var data config.PushData
				if v, ok := keywords.Get(key); ok {
					d := v.(config.PushData)
					d.Value += new_value_float
					data = d
				} else {
					data = config.PushData{Metric: config.Cfg.Metric,
						Endpoint:    config.Cfg.Host,
						Timestamp:   time.Now().Unix(),
						Value:       new_value_float,
						Step:        config.Cfg.Timer,
						CounterType: "GAUGE",
						Tags:		"path="+file.Path+",filepattern="+file.FilePattern+",tag="+p.Tag,
					}
				}
				keywords.Set(key, data)
			} else {
				log.Debug("no match")
			}
		}
	}
}

func postData() {
	c := config.Cfg
	workers <- true

	go func() {
		if len(keywords.Items()) != 0 {
			data := make([]config.PushData, 0, 3000)
			for k, v := range keywords.Items() {
				tem_data := v.(config.PushData)
				tem_data.Timestamp = time.Now().Unix()
				data = append(data, tem_data)
				keywords.Remove(k)
			}

			bytes, err := json.Marshal(data)
			if err != nil {
				log.Error("marshal push data", data, err)
				return
			}

			log.Debug("pushing data:", string(bytes))

			resp, err := http.Post(c.Agent, "plain/text", strings.NewReader(string(bytes)))
			if err != nil {
				log.Error(" post data ", string(bytes), " to agent ", err)
			} else {
				defer resp.Body.Close()
				bytes, _ = ioutil.ReadAll(resp.Body)
				log.Debug("push data", string(bytes))
			}
		}

		<-workers
	}()

}

func fillData() {
	c := config.Cfg
	for _, v := range c.WatchFiles {
		for _, p := range v.Keywords {
			key := v.Path + v.FilePattern + p.Tag
			if _, ok := keywords.Get(key); ok {
				continue
			}

			//不存在要插入一个补全
			data := config.PushData{Metric: c.Metric,
				Endpoint:    c.Host,
				Timestamp:   time.Now().Unix(),
				Value:       0.0,
				Step:        c.Timer,
				CounterType: "GAUGE",
				Tags:		"path="+v.Path+",filepattern="+v.FilePattern+",tag="+p.Tag,
			}
			keywords.Set(key, data)
		}
	}
}
