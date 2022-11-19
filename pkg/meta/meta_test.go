package meta

import (
	"reflect"
	"testing"

	"go.etcd.io/etcd/clientv3"
)

func TestRead(t *testing.T) {
	type args struct {
		client   *clientv3.Client
		metatype MetaType
	}
	tests := []struct {
		name string
		args args
		want []byte
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Read(tt.args.client, tt.args.metatype); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Read() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConnect(t *testing.T) {
	tests := []struct {
		name string
		want *clientv3.Client
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Connect(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Connect() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestWrite(t *testing.T) {
	type args struct {
		client   *clientv3.Client
		metatype MetaType
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			Write(tt.args.client, tt.args.metatype)
		})
	}
}
