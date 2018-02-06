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

	"github.com/fsnotify/fsnotify"
	"github.com/hpcloud/tail"
	"github.com/streamrail/concurrent-map"

	"./config"
	"./log"
)

var (
	workers  chan bool
	keywords cmap.ConcurrentMap
)

func main() {

	workers = make(chan bool, runtime.NumCPU()*2)
	keywords = cmap.New()
	runtime.GOMAXPROCS(runtime.NumCPU())
	kill_chan := make(chan bool)

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
		ConfigFileWatcher(kill_chan)
		for i := 0; i < len(config.Cfg.WatchFiles); i++ {
			readFileAndSetTail(&(config.Cfg.WatchFiles[i]))
			go logFileWatcher(&(config.Cfg.WatchFiles[i]), kill_chan)

		}
	}()
	//go func() {
	//	setLogFile()
	//
	//	log.Info("watch file", config.Cfg.WatchFiles)
	//
	//	for i := 0; i < len(config.Cfg.WatchFiles); i++ {
	//		readFileAndSetTail(&(config.Cfg.WatchFiles[i]))
	//		go logFileWatcher(&(config.Cfg.WatchFiles[i]))
	//
	//	}
	//
	//}()

	select {}
}

//配置文件监控,可以实现热更新
func ConfigFileWatcher(kill_chan chan bool) {
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
				if event.Name == config.ConfigFile && (event.Op == fsnotify.Chmod || event.Op == fsnotify.Rename || event.Op == fsnotify.Write || event.Op == fsnotify.Create) {
					log.Debug("modified config file", event.Name, "will reaload config")
					old_cfg := config.Cfg
					if cfg, err := config.ReadConfig(config.ConfigFile); err != nil {
						log.Debug("ERROR: config has error, will not use old config", err)
					} else if config.CheckConfig(config.Cfg) != nil {
						log.Debug("ERROR: config has error, will not use old config", err)
					} else {
						config.SetLogFile()
						log.Debug("config reload success")
						config.Cfg = cfg
						for i:=0; i<len(old_cfg.WatchFiles); i++{
							kill_chan <- true
						}
						for _, v := range old_cfg.WatchFiles {
							if v.ResultFile.LogTail != nil {
								v.ResultFile.LogTail.Stop()
							}
						}

						for i := 0; i < len(config.Cfg.WatchFiles); i++ {
							readFileAndSetTail(&(config.Cfg.WatchFiles[i]))
							go logFileWatcher(&(config.Cfg.WatchFiles[i]), kill_chan)

						}
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

func logFileWatcher(file *config.WatchFile, kill_chan chan bool) {
	logTail := file.ResultFile.LogTail
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}
	defer watcher.Close()

	done := make(chan bool)

	go func() {
		for {
			select {
			case <- kill_chan:
				break
			case event := <-watcher.Events:
				log.Debug("event:", event)

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
						if strings.HasSuffix(event.Name, file.Suffix) && strings.HasPrefix(path.Base(event.Name), file.Prefix) {
							//if logTail != nil {
							//	logTail.Stop()
							//}
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

	log.Info("read file", file.ResultFile.FileName)
	tail, err := tail.TailFile(file.ResultFile.FileName, tail.Config{Follow: true})
	if err != nil {
		log.Fatal(err)
	}

	file.ResultFile.LogTail = tail

	go func() {
		for line := range tail.Lines {
			log.Debug("log line: ", line.Text)
			handleKeywords(*file, line.Text)
		}
	}()

}



// 查找关键词
func handleKeywords(file config.WatchFile, line string) {
	for _, p := range file.Keywords {
		value := 0.0
		if p.Regex.MatchString(line) {
			log.Debugf("exp:%v match ===> line: %v ", p.Regex.String(), line)
			value = 1.0
		}
		key := file.ResultFile.FileName + p.Tag
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
				Tags:        "prefix=" + file.Prefix + ",suffix=" + file.Suffix + "," + p.Tag + "=" + p.FixedExp,
			}
		}

		keywords.Set(key, data)

	}
}

func postData() {
	c := config.Cfg
	workers <- true

	go func() {
		if len(keywords.Items()) != 0 {
			data := make([]config.PushData, 0, 20)
			for k, v := range keywords.Items() {
				data = append(data, v.(config.PushData))
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
				log.Debug(string(bytes))
			}
		}

		<-workers
	}()

}

func fillData() {
	c := config.Cfg
	for _, v := range c.WatchFiles {
		for _, p := range v.Keywords {

			key := v.ResultFile.FileName + p.Tag
			//log.Println("_______", key)
			// key := p.Exp
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
				Tags:        "prefix=" + v.Prefix + ",suffix=" + v.Suffix + "," + p.Tag + "=" + p.FixedExp,
			}

			keywords.Set(key, data)
		}
	}

}
