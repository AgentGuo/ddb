/*
@author: panfengguo
@since: 2022/11/19
@desc: desc
*/
package utils

import (
	"fmt"
	"net"
	"reflect"
	"strings"
)

// GetOutBoundIP
//
//	@Description: 获得对外的ip地址
//	@return ip
//	@return err
func GetOutBoundIP() (ip string, err error) {
	conn, err := net.Dial("udp", "8.8.8.8:53")
	if err != nil {
		fmt.Println(err)
		return
	}
	localAddr := conn.LocalAddr().(*net.UDPAddr)
	ip = strings.Split(localAddr.String(), ":")[0]
	return
}

func Interface2Int64(v interface{}, kind reflect.Kind) int64 {
	a := v.(*int)
	fmt.Println(*a)
	switch kind {
	case reflect.Int:
		return int64(v.(int))
	case reflect.Int8:
		return int64(v.(int8))
	case reflect.Int16:
		return int64(v.(int16))
	case reflect.Int32:
		return int64(v.(int32))
	case reflect.Int64:
		return int64(v.(int64))
	case reflect.Uint:
		return int64(v.(uint))
	case reflect.Uint8:
		return int64(v.(uint8))
	case reflect.Uint16:
		return int64(v.(uint16))
	case reflect.Uint32:
		return int64(v.(uint32))
	case reflect.Uint64:
		return int64(v.(uint64))
	default:
		return 0
	}
}
