package rpcclient

import (
	"context"
	"github.com/fandypeng/e2cdatabus/proto"
	"github.com/fandypeng/e2cdatabus/rpcserver"
	"testing"
	"time"
)

const (
	testAppKey    = "MPF23Ts0Nu6KBfBn"
	testAppSecret = "UyuC5OaBlW=7jkGL5RgyhPctijHOKh1W"

	serverPort = 10000
	serverAddr = "127.0.0.1:10000"

	testMysqlDsn  = "username:password@tcp(127.0.0.1:3306)/dbname?charset=utf8mb4"
	testRedisAddr = "127.0.0.1:6379"
	testRedisPwd  = "password"
)

func TestMain(m *testing.M) {
	go func() {
		svs := rpcserver.NewService()
		svs.SetMyqlConnect(testMysqlDsn)
		//svs.SetRedisConnect(testRedisAddr, testRedisPwd)
		err := rpcserver.Start(rpcserver.Conf{
			Port:      serverPort,
			AppKey:    testAppKey,
			AppSecret: testAppSecret,
		}, svs)
		if err != nil {
			panic(err)
		}
	}()
	time.Sleep(time.Second * 1)
	m.Run()
}

func TestNewRpcClient(t *testing.T) {
	rc, err := NewRpcClient(Conf{
		ServerAddr: serverAddr,
		AppKey:     testAppKey,
		AppSecret:  testAppSecret,
	})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := rc.SayHello(context.TODO(), &proto.SayHelloReq{Greet: "Fandy Greet"})
	if err != nil {
		t.Fatal(err)
	}
	t.Log("连接测试成功, ", resp.Response)
}

func TestUpdateConfig(t *testing.T) {
	rc, err := NewRpcClient(Conf{
		ServerAddr: serverAddr,
		AppKey:     testAppKey,
		AppSecret:  testAppSecret,
	})
	if err != nil {
		t.Fatal(err)
	}
	tableName := "item_list"
	getReq := &proto.GetConfigReq{Name: tableName}
	getResp, err := rc.GetConfig(context.TODO(), getReq)
	if err != nil {
		t.Fatal(err)
	}
	updateReq := &proto.UpdateConfigReq{
		Name: tableName,
		Head: &proto.TableHead{
			Fields: []string{"sid", "type", "name", "event"},
			Types:  []string{"int", "int", "string", "string"},
			Descs:  []string{"流水ID", "类型", "名称", "事件名称"},
		},
		Content: `[{"event":"事件1","name":"名称1","sid":1,"type":1},{"event":"事件2","name":"名称2","sid":2,"type":1},{"event":"事件4","name":"名称4","sid":3,"type":1}]`,
	}
	resp, err := rc.UpdateConfig(context.TODO(), updateReq)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != 0 {
		t.Errorf("UpdateConfig resp: %v", resp)
		return
	}
	t.Logf("TestUpdateConfig succeed, resp: %v", resp)

	getReq = &proto.GetConfigReq{Name: updateReq.Name}
	getResp, err = rc.GetConfig(context.TODO(), getReq)
	if err != nil {
		t.Fatal(err)
	}
	if getResp.Content != updateReq.Content {
		t.Fatalf("content is diffrent, %v", getResp.Content)
	}
}
