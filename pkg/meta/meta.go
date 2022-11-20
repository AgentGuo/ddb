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
	SiteMeta      SiteMeta_
	FragmentMeta  FragmentMeta_
	FieldMeta     FieldMeta_
	RouterMeta    RouterMeta_
	TableMeta     TableMeta_
	DbMeta        DbMeta_
	DefaultDbName string = "ddb"
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
			v, _ := json.Marshal(FragmentMeta)
			_, err := kv.Put(ctx, "site/"+SiteMeta.Name+"/"+FragmentMeta.Name+"/meta", string(v))
			if err != nil {
				fmt.Println(err)
			}
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

func ReadPhys(client *clientv3.Client, SiteName string, FragmentName string, metatype MetaType) []byte {
	kv := clientv3.NewKV(client)
	// ctx, cancel := context.WithTimeout(rootContext, time.Duration(5)*time.Second)
	ctx := context.TODO()
	switch metatype {
	case FragmentMetaType:
		{
			response, err := kv.Get(ctx, "site/"+SiteName+"/"+FragmentName+"/meta")
			if err != nil {
				fmt.Println(err)
			}
			// cancel()
			data := make([]byte, len(response.Kvs[0].Value))
			copy(data, response.Kvs[0].Value)
			return data
		}
	case SiteMetaType:
		{
			response, err := kv.Get(ctx, "site/"+SiteName+"/meta")
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
}
func ReadLogi(client *clientv3.Client, DbName string, TableName string, metatype MetaType) []byte {
	kv := clientv3.NewKV(client)
	// ctx, cancel := context.WithTimeout(rootContext, time.Duration(5)*time.Second)
	ctx := context.TODO()
	switch metatype {
	case TableMetaType:
		{
			response, err := kv.Get(ctx, "db/"+DbName+"/"+TableName+"/meta")
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
			response, err := kv.Get(ctx, "db/"+DbName+"/meta")
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
}
