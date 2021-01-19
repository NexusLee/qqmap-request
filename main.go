package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"qqlocation/util"
	"time"

	"golang.org/x/time/rate"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var (
	db           *gorm.DB
	messageQueue *util.Queue
)

type Community struct {
	ID         int `gorm:"primary_key"`
	Name       string
	Address    string
	DivisionID int
	Latitude   float64
	Longitude  float64
}

type HttpResponse struct {
	Status    interface{} `json:"status"`
	Message   interface{} `json:"message"`
	Count     interface{} `json:"count"`
	Data      []Info      `json:"data"`
	RequestID interface{} `json:"Request_id"`
}

type Info struct {
	ID       string `json:"id"`
	Title    string `json:"title"`
	Address  string `json:"address"`
	Category string `json:"category"`
	Type     int    `json:"type"`
	Location LatLng `json:"location"`
	Adcode   int    `json:"adcode"`
	Province string `json:"province"`
	City     string `json:"city"`
	District string `json:"district"`
}

type LatLng struct {
	Lat float64 `json:"lat"`
	Lng float64 `json:"lng"`
}

func main() {
	var communities []Community
	db = GormMysql()

	messageQueue = util.NewQueue(10000)
	l := rate.NewLimiter(2, 5)
	ctx := context.Background()

	go runQueue()

	err := db.Find(&communities).Error
	if err != nil {
		fmt.Println(err)
	}

	begin := time.Now()
	for _, v := range communities {
		if v.Latitude == 0 {
			l.Wait(ctx)
			var url = "https://apis.map.qq.com/ws/place/v1/suggestion?region=江苏南京&keyword=" + v.Name + "&filter=category=房产小区&key=key"
			res, err := http.Get(url)
			if err != nil {
				fmt.Println("json转义失败")
				continue
			}

			defer res.Body.Close()
			body, _ := ioutil.ReadAll(res.Body)

			resp := HttpResponse{}
			err = json.Unmarshal(body, &resp)
			if err != nil {
				fmt.Println("json转义失败")
				fmt.Printf("end at %s\n", time.Now().Sub(begin))
				continue
			}

			if len(resp.Data) == 0 {
				fmt.Printf("end at %s\n", time.Now().Sub(begin))
				continue
			}

			info := resp.Data[0]
			v.Address = info.Address
			v.Latitude = info.Location.Lat
			v.Longitude = info.Location.Lng

			messageQueue.Put(v)
			fmt.Printf("end at %s\n", time.Now().Sub(begin))
		}
	}

	for {
		time.Sleep(1 * time.Second)
	}
}

// gormConfig 根据配置决定是否开启日志
func gormConfig(mod bool) *gorm.Config {
	if mod {
		return &gorm.Config{
			Logger:                                   logger.Default.LogMode(logger.Info),
			DisableForeignKeyConstraintWhenMigrating: true,
		}
	} else {
		return &gorm.Config{
			Logger:                                   logger.Default.LogMode(logger.Silent),
			DisableForeignKeyConstraintWhenMigrating: true,
		}
	}
}

// GormMysql 初始化Mysql数据库
func GormMysql() *gorm.DB {
	dsn := "user:password@tcp(127.0.0.1:33060)/app?charset=utf8&parseTime=True&loc=Local"
	mysqlConfig := mysql.Config{
		DSN:                       dsn,   // DSN data source name
		DefaultStringSize:         191,   // string 类型字段的默认长度
		DisableDatetimePrecision:  true,  // 禁用 datetime 精度，MySQL 5.6 之前的数据库不支持
		DontSupportRenameIndex:    true,  // 重命名索引时采用删除并新建的方式，MySQL 5.7 之前的数据库和 MariaDB 不支持重命名索引
		DontSupportRenameColumn:   true,  // 用 `change` 重命名列，MySQL 8 之前的数据库和 MariaDB 不支持重命名列
		SkipInitializeWithVersion: false, // 根据版本自动配置
	}
	if db, err := gorm.Open(mysql.New(mysqlConfig), gormConfig(false)); err != nil {
		fmt.Printf("MySQL启动异常,err:%v\n", err)
		os.Exit(0)
		return nil
	} else {
		sqlDB, _ := db.DB()
		sqlDB.SetMaxIdleConns(10)
		sqlDB.SetMaxOpenConns(100)
		return db
	}
}

func runQueue() {
	count := 0
	var community Community
	for {
		val, _, _ := messageQueue.Get()

		if val != nil {
			count++
			if count > 5200 {
				fmt.Println(count)
			}
			community = val.(Community)
			db.Model(&community).Updates(Community{Address: community.Address, Latitude: community.Latitude, Longitude: community.Longitude})
		}
	}
}
