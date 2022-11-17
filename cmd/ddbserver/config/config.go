package config

import (
	"gopkg.in/yaml.v3"
	"io/ioutil"
)

type ServerConfig struct {
	ServerPort int `yaml:"server_port"`
	ETCDPort   int `yaml:"etcd_port"`
}

func ReadServerConfig(configPath string) (*ServerConfig, error) {
	// default config value
	config := &ServerConfig{
		ServerPort: 13306,
		ETCDPort:   2379,
	}
	fileContent, err := ioutil.ReadFile(configPath)
	if err != nil {
		return config, err
	}
	// yaml unmarshal
	err = yaml.Unmarshal(fileContent, &config)
	if err != nil {
		return config, err
	}
	return config, nil
}
