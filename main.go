package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"github.com/mediocregopher/radix.v2/pool"
	"github.com/mgutz/str"
	"github.com/shopspring/decimal"
	"github.com/sirupsen/logrus"
	"io"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const HANDLE_MOVIE = "/movie/"
const HANDLE_LIST = "/list/"
const HANDLE_HTML = ".html"

type cmdParams struct {
	logFilePath string
	routineNum  int
}

type urlData struct {
	url          string //请求的url
	responseTime string //响应时间
	unTime       string //当前访问这个页面的时间(年月日时分秒)
}

type urlNode struct {
	unType       string //movie  /list  /home 这些接口
	unRid        int    //Resource ID 资源ID
	unUrl        string //当前页面的URL
	responseTime string //当前接口的耗时
	unTime       string //当前访问这个页面的时间(年月日时分秒)
}

//存储数据格式
type storageBlock struct {
	counterType  string //统计的类型 max avg
	storageModel string //存储的格式
	unode        urlNode
}

var log = logrus.New()

func init() {
	log.Out = os.Stdout
	log.SetLevel(logrus.DebugLevel)
}

var closeChannel chan byte

func main() {
	//获取参数
	logFilePath := flag.String("logFilePath", "/usr/local/etc/nginx/logs/access.log", "log file path")
	routineNum := flag.Int("routineNum", 5, "consumer number by goroutine")
	l := flag.String("l", "/tmp/log", "this programe runtime log target file path")
	flag.Parse()

	params := cmdParams{
		*logFilePath,
		*routineNum,
	}

	//打日志
	logFd, err := os.OpenFile(*l, os.O_CREATE|os.O_WRONLY, 0644)

	if err == nil {
		log.Out = logFd
		defer logFd.Close()
	}
	log.Infoln("Exec start.")
	log.Infoln("Params: logFilePath=%s,routineNum=%d", params.logFilePath, params.routineNum)

	//初始化一些channel，用于数据传递
	var logChannel = make(chan string, 3*params.routineNum)
	var storageChannel = make(chan storageBlock, params.routineNum)

	//最大、平均耗时
	var maxTimeChannel = make(chan urlNode, params.routineNum)
	var avgTimeChannel = make(chan urlNode, params.routineNum)

	redisPool, err := pool.New("tcp", "127.0.0:6379", 2*params.routineNum)
	if err != nil {
		log.Fatalln("Redis pool created failed.")
		// redis连接创建失败是重大错误
		panic(err)
	} else {
		// 当半夜没有日志时候 连接池会空闲，这时redis会断开连接
		go func() {
			//开启一个goroutine 给redis连接池发送心跳 保证连接不断开
			// 保证连接不断开 在高并发场景下至关重要
			for {
				redisPool.Cmd("PING")
				time.Sleep(3 * time.Second)
			}
		}()
	}

	//日志消费者
	go readFileLinebyLine(params, logChannel)

	//创建一组日志处理
	for i := 0; i < params.routineNum; i++ {
		go logConsumer(logChannel, maxTimeChannel, avgTimeChannel)
	}

	//创建最大、平均耗时 统计器
	go maxCounter(maxTimeChannel, storageChannel)
	go avgCounter(avgTimeChannel, storageChannel)

	//创建存储器
	go dataStorage(storageChannel, redisPool)

	//go dataStorage(storageChannel, redisPool)
	<-closeChannel
	fmt.Println("Server done.")

}

func maxCounter(maxTimeChannel chan urlNode, storagecChannel chan storageBlock) {
	//将数据上传到redis,统计每个接口的最大耗时
	for data := range maxTimeChannel {
		sItem := storageBlock{
			counterType:  "max",
			storageModel: "SET",
			unode:        data,
		}
		storagecChannel <- sItem
	}
}

func dataStorage(storageChannel chan storageBlock, redisPool *pool.Pool) {
	for block := range storageChannel {

		// ZINCRBY key increment member
		// sum_movie_id
		if block.counterType == "avg" {
			rowId := block.unode.unRid
			urType := block.unode.unType                                                                  //接口类型 /movie /home /list
			sumkey := "sum_" + "day_" + getTime(block.unode.unTime, "day") + urType + strconv.Itoa(rowId) //sum_day_2006-01-02_movie

			//SETNX KEY_NAME VALUE
			//SET sum_movie_id  responseTime
			res, _ := redisPool.Cmd("SETNX", sumkey, block.unode.responseTime).Int()
			if res == 0 { //设置失败
				sum, _ := redisPool.Cmd("GET", sumkey).Float64()
				responseTime, _ := strconv.ParseFloat(block.unode.responseTime, 64)
				sum += responseTime
				redisPool.Cmd("SET", sumkey, sum).Int()
			}

			sum, _ := redisPool.Cmd("GET", sumkey).Float64()

			//count_movie_id : 1
			countkey := "count_" + urType + "_" + strconv.Itoa(rowId)
			avgkey := "avg_" + urType + "_" + strconv.Itoa(rowId)
			exist, _ := redisPool.Cmd("EXIST", countkey).Int()

			//设置平均时间
			//avg_movie_id : sum_movie_id / count_movie_id
			if exist == 0 { //不存在
				redisPool.Cmd("SET", countkey, 1).Int()
				redisPool.Cmd("SET", avgkey, block.unode.responseTime).Int()
			} else {
				count, _ := redisPool.Cmd("GET", countkey).Int()
				redisPool.Cmd("SET", countkey, count+1).Int()

				// float64( sum/count+1 )
				div := decimal.NewFromFloat(sum).Div(decimal.NewFromFloat(float64(count + 1)))
				redisPool.Cmd("SET", avgkey, div.String()).Int()
			}
			//

		} else if block.counterType == "max" {
			rowId := block.unode.unRid
			urType := block.unode.unType //接口类型 /movie /home /list
			responseTime := block.unode.responseTime
			maxkey := "max_" + "day_" + getTime(block.unode.unTime, "day") + urType + strconv.Itoa(rowId) //max_day_2006-01-02_movie
			exist, _ := redisPool.Cmd("EXIST", maxkey).Int()
			if exist == 0 { //不存在
				redisPool.Cmd("SET", maxkey, responseTime).Int()
			} else {
				maxtime, _ := redisPool.Cmd("GET", maxkey).Float64()
				resp, _ := strconv.ParseFloat(responseTime, 64)

				//-1 if maxtime <  resp
				// 0 if maxtime ==  resp
				//+1 if maxtime >  resp

				if compare, _ := FloatCompare(maxtime, resp); compare == -1 {
					redisPool.Cmd("SET", maxkey, maxtime)
				}

			}
		}

	}
	close(closeChannel)
}

func FloatCompare(f1, f2 interface{}) (n int, err error) {
	var f1Dec, f2Dec decimal.Decimal
	switch f1.(type) {
	case float64:
		f1Dec = decimal.NewFromFloat(f1.(float64))
		switch f2.(type) {
		case float64:
			f2Dec = decimal.NewFromFloat(f2.(float64))
		case string:
			f2Dec, err = decimal.NewFromString(f2.(string))
			if err != nil {
				return 2, err
			}
		default:
			return 2, errors.New("FloatCompare() expecting to receive float64 or string")
		}
	case string:
		f1Dec, err = decimal.NewFromString(f1.(string))
		if err != nil {
			return 2, err
		}
		switch f2.(type) {
		case float64:
			f2Dec = decimal.NewFromFloat(f2.(float64))
		case string:
			f2Dec, err = decimal.NewFromString(f2.(string))
			if err != nil {
				return 2, err
			}
		default:
			return 2, errors.New("FloatCompare() expecting to receive float64 or string")
		}
	default:
		return 2, errors.New("FloatCompare() expecting to receive float64 or string")
	}
	return f1Dec.Cmp(f2Dec), nil
}

func avgCounter(avgTimeChannel chan urlNode, storagecChannel chan storageBlock) {
	//将数据上传到redis,统计每个接口的平均耗时
	for data := range avgTimeChannel {
		sItem := storageBlock{
			counterType:  "avg",
			storageModel: "SET",
			unode:        data,
		}
		storagecChannel <- sItem
	}
}

func logConsumer(logChannel chan string, maxCounter, avgCounter chan urlNode) error {
	for logStr := range logChannel {
		//切割日志字符串，扣出打点上报的数据
		data := cutLogFetchData(logStr) //urlData类型

		uData := formatUrl(data.url, data.responseTime, data.unTime) //urlNode类型

		maxCounter <- uData
		avgCounter <- uData
	}
	return nil
}

func formatUrl(url string, responseTime string, time string) urlNode {
	pos1 := str.IndexOf(url, HANDLE_MOVIE, 0)
	if pos1 != -1 {
		pos1 += len(HANDLE_MOVIE)
		pos2 := str.IndexOf(url, HANDLE_HTML, 0)
		idStr := str.Substr(url, pos1, pos2-pos1) //截取详情页面ID
		id, _ := strconv.Atoi(idStr)
		return urlNode{
			"movie",
			id,
			url,
			responseTime,
			time,
		}
	} else {
		pos1 = str.IndexOf(url, HANDLE_LIST, 0)
		if pos1 != -1 {
			pos1 += len(HANDLE_LIST)
			pos2 := str.IndexOf(url, HANDLE_HTML, 0)
			idStr := str.Substr(url, pos1, pos2-pos1) //截取列表页ID
			id, _ := strconv.Atoi(idStr)
			return urlNode{
				"list",
				id,
				url,
				responseTime,
				time,
			}
		} else {
			//首页
			return urlNode{
				"home",
				1, //为0时redis不能写入
				url,
				responseTime,
				time,
			} //这里可以拓展别的页面
		}
	}
}

func cutLogFetchData(logStr string) urlData {
	logStr = strings.TrimSpace(logStr)
	pos1 := str.IndexOf(logStr, "/dig?", 0)
	if pos1 == -1 {
		return urlData{}
	}
	pos1 += len("/dig?")
	pos2 := str.IndexOf(logStr, "HTTP/", pos1)
	d := str.Substr(logStr, pos1, pos2-pos1)
	urlInfo, err := url.Parse("http://localhost/?" + d)
	if err != nil {
		return urlData{}
	}
	data := urlInfo.Query()

	pos3 := str.IndexOf(logStr, "`", 0)
	pos4 := str.IndexOf(logStr, "`", pos3+1)
	responseTime := str.Substr(logStr, pos4, pos3)

	return urlData{
		data.Get("url"),
		responseTime,
		data.Get("time"),
	}
}

func readFileLinebyLine(params cmdParams, logChannel chan string) error {
	fd, err := os.Open(params.logFilePath)
	if err != nil {
		log.Warningf("打开日志文件失败:%s", params.logFilePath)
		return err
	}
	defer fd.Close()

	count := 0
	bufferRead := bufio.NewReader(fd)
	for {
		line, err := bufferRead.ReadString('\n') //字符，不是字符串
		logChannel <- line
		count++

		if err != nil {
			//文件已经消费完了，比如半夜 文件没有新增
			if err == io.EOF {
				time.Sleep(3 * time.Second)
				log.Infof("日志没数据了，等待一会。当前行为%d", count)
			} else {
				log.Warningf("日志出错")
			}
		}
	}
	return nil
}

func getTime(logTime, timeType string) string {
	var item string
	switch timeType {
	case "day":
		item = "2006-01-02"
		break
	case "hour":
		item = "2006-01-02 15"
		break
	case "min":
		item = "2006-01-02 15:09"
		break
	}
	t, _ := time.Parse(item, time.Now().Format(item))
	// 把64位Unix时间戳 转成10进制字符串
	return strconv.FormatInt(t.Unix(), 10)
}
