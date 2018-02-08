package config_server

import (
	"net/http"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"../config"
	"log"
)

//func init() {
//	push_handler()
//}

func Push_handler() {
	http.HandleFunc("/push_config", func(w http.ResponseWriter, req *http.Request) {

		//if req.ContentLength == 0 {
		//	http.Error(w, "body is blank", http.StatusBadRequest)
		//	return
		//}
		fmt.Println(req.Method)
		if req.Method == "POST" {
			result, _:= ioutil.ReadAll(req.Body)
			req.Body.Close()
			fmt.Printf("%s\n", result)
			var cfg *config.Config

			json.Unmarshal(result, &cfg)
			if err := json.Unmarshal(result, &cfg); err != nil {
				fmt.Println(err)
				http.Error(w, "connot decode body", http.StatusBadRequest)
				return
			}

			if err := config.CheckConfig(cfg); err != nil {
				log.Println(err)
				http.Error(w, "config is wrong", http.StatusBadRequest)
				return
			} else {
				fmt.Println(cfg.LogLevel)
				file, err := os.OpenFile("cfg.json", os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0666)
				defer file.Close()
				if err != nil {
					http.Error(w, "open file error", http.StatusBadRequest)
					return
				}
				//new_result, err := json.Marshal(cfg)
				//if err != nil {
				//	fmt.Println(err)
				//}
				bytesWritten, err := file.Write(result)
				if err != nil {
					http.Error(w, "write file error", http.StatusBadRequest)
					return
				}
				fmt.Println(bytesWritten,"bytes writed")
				w.Write([]byte("success"))
				//decoder := json.NewDecoder(req.Body)
				//err := decoder.Decode(&cfg)
				//if err != nil {
				//	http.Error(w, "connot decode body", http.StatusBadRequest)
				//	return
				//}
			}

		} else {
			w.Write([]byte("Only support POST json"))
		}

	})
	fmt.Println("Listen at 0.0.0.0:8008")
	http.ListenAndServe("0.0.0.0:8008", nil)
}



