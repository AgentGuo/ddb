package meta

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.etcd.io/etcd/clientv3"
)

// var rootContext context.Context

var (
	SiteMeta     SiteMeta_
	FragmentMeta FragmentMeta_
	FieldMeta    FieldMeta_
	RouterMeta   RouterMeta_
	TableMeta    TableMeta_
	DbMeta       DbMeta_
)

func Connect() *clientv3.Client {
	// rootContext = context.Background()
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"10.77.110.228:12379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		// handle error!
		fmt.Errorf("error connect etcd %v", err)
	}
	return client
}

func Write(client *clientv3.Client, metatype MetaType) {
	kv := clientv3.NewKV(client)
	// ctx, cancel := context.WithTimeout(rootContext, time.Duration(5)*time.Second)
	ctx := context.TODO()
	switch metatype {
	case SiteMetaType:
		{
			v, _ := json.Marshal(SiteMeta)
			_, err := kv.Put(ctx, "site/"+SiteMeta.Name+"/meta", string(v))
			if err != nil {
				fmt.Println(err)
			}
		}
	case FragmentMetaType:
		{
			fmt.Print("To do")
		}
	case TableMetaType:
		{
			v, _ := json.Marshal(TableMeta)
			_, err := kv.Put(ctx, "db/"+DbMeta.Name+"/"+TableMeta.Name+"/meta", string(v))
			if err != nil {
				fmt.Println(err)
			}
		}
	case DbMetaType:
		{
			v, _ := json.Marshal(DbMeta)
			_, err := kv.Put(ctx, "db/"+DbMeta.Name+"/meta", string(v))
			if err != nil {
				fmt.Println(err)
			}
		}
	default:
		{
			fmt.Print("default")
		}
	}
	// cancel()
}

func Read(client *clientv3.Client, metatype MetaType) []byte {
	kv := clientv3.NewKV(client)
	// ctx, cancel := context.WithTimeout(rootContext, time.Duration(5)*time.Second)
	ctx := context.TODO()
	switch metatype {
	case SiteMetaType:
		{
			fmt.Print("To do")
		}
	case FragmentMetaType:
		{
			fmt.Print("To do")
		}
	case TableMetaType:
		{
			response, err := kv.Get(ctx, "db/"+DbMeta.Name+"/"+TableMeta.Name+"/meta")
			if err != nil {
				fmt.Println(err)
			}
			// cancel()
			data := make([]byte, len(response.Kvs[0].Value))
			copy(data, response.Kvs[0].Value)
			return data
		}
	case DbMetaType:
		{
			response, err := kv.Get(ctx, "db/"+DbMeta.Name+"/meta")
			if err != nil {
				fmt.Println(err)
			}
			// cancel()
			data := make([]byte, len(response.Kvs[0].Value))
			copy(data, response.Kvs[0].Value)
			return data
		}
	default:
		{
			fmt.Print("default")
			return nil
		}
	}
	return nil
}

// func Main1() {
// 	rootContext := context.Background()
// 	cli, err := clientv3.New(clientv3.Config{
// 		Endpoints:   []string{"10.77.110.228:12379"},
// 		DialTimeout: 5 * time.Second,
// 	})
// 	if err != nil {
// 		// handle error!
// 		fmt.Errorf("error connect etcd %v", err)
// 	}
// 	defer cli.Close()
// 	testFunc(cli, rootContext)
// }

// // 基本测试（获取值，设置值）
// func testFunc(cli *clientv3.Client, rootContext context.Context) {
// 	kvc := clientv3.NewKV(cli)

// 	//设置值
// 	uuid := uuid.New().String()
// 	fmt.Printf("new value is :%s\n", uuid)
// 	ctx2, cancelFunc2 := context.WithTimeout(rootContext, time.Duration(2)*time.Second)
// 	_, err2 := kvc.Put(ctx2, "cc/a", "a")
// 	cancelFunc2()
// 	if err2 != nil {
// 		fmt.Println(err2)
// 	}
// 	//获取值
// 	ctx, cancelFunc := context.WithTimeout(rootContext, time.Duration(2)*time.Second)
// 	response, err := kvc.Get(ctx, "cc", clientv3.WithPrefix())
// 	cancelFunc()
// 	if err != nil {
// 		fmt.Println(err)
// 	}
// 	kvs := response.Kvs
// 	fmt.Println(kvs)
// 	fmt.Printf("last value is :%s\n", string(kvs[1].Value))
// }
