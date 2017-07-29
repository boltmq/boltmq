package smartgonamesrv

import "time"

import (
	"github.com/coreos/etcd/client"
	"golang.org/x/net/context"
	"log"
	"os"
)

func main() {
	cfg := client.Config{
		Endpoints: []string{"http://127.0.0.1:2379"},
		Transport: client.DefaultTransport,
		// set timeout per request to fail fast when the target endpoint is unavailable
		HeaderTimeoutPerRequest: time.Second,
	}
	c, err := client.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	kapi := client.NewKeysAPI(c)
	// set "/foo" key with "bar" value
	log.Print("Setting '/foo' key with 'bar' value")
	resp, err := kapi.Set(context.Background(), "/foo", "bar", nil)
	if err != nil {
		log.Fatal(err)
	} else {
		// print common key info
		log.Printf("Set is done. Metadata is %q\n", resp)
	}
	// get "/foo" key's value
	log.Print("Getting '/foo' key value")
	resp, err = kapi.Get(context.Background(), "/foo", nil)
	if err != nil {
		log.Fatal(err)
	} else {
		// print common key info
		log.Printf("Get is done. Metadata is %q\n", resp)
		// print value
		log.Printf("%q key has %q value\n", resp.Node.Key, resp.Node.Value)
	}
}

func configPath() string {
	mcfg := os.Getenv("OPENAPI_CONFIG")
	if mcfg == "" {
		return "etc/cfg.json"
	}

	return mcfg
}

//Init 模块初始化
/*func Init() {

	// 1. init cfg1.json Add: jerrylou Since: 2017/7/6
	err := utils.ParseConfig(configPath(), &cfg)
	if err != nil {
		panic(err)
	}
	log.Println("read config file success.")

	// 2. set common logger Add: jerrylou Since: 2017/7/6
	logger.SetConfig(cfg.Log)

	// 3. init db  Add: jerrylou Since: 2017/7/10
	err = mysql.Register(cfg.DBS)
	if err != nil {
		panic(err)
	}

	for _, db := range cfg.DBS {
		log.Println("mysql register success, dbID:", db.ID)
	}

	// 4. config mail  Add: tianyuliang Since: 2017/7/14
	mail.Cfg(&cfg.Mail)
	log.Println("init mail cfg success")
}

// GetConfig 取配置
func GetConfig() *Config {
	return &cfg
}*/
