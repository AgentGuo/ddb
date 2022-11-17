package config

import (
	"gopkg.in/yaml.v3"
	"io/ioutil"
)

type ClientConfig struct {
	ClientPort int `yaml:"client_port"`
	ETCDPort   int `yaml:"etcd_port"`
}

func ReadClientConfig(configPath string) (*ClientConfig, error) {
	// default config value
	config := &ClientConfig{
		ClientPort: 12345,
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
