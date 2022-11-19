package config

import (
	"gopkg.in/yaml.v3"
	"io/ioutil"
)

type ServerConfig struct {
	ServerPort  int         `yaml:"server_port"`
	ETCDPort    int         `yaml:"etcd_port"`
	MysqlConfig MysqlConfig `yaml:"mysql_config"`
}

type MysqlConfig struct {
	Ip     string `yaml:"ip"`
	Port   string `yaml:"port"`
	User   string `yaml:"user"`
	Passwd string `yaml:"passwd"`
}

func ReadServerConfig(configPath string) (*ServerConfig, error) {
	// default config value
	config := &ServerConfig{
		ServerPort: 13306,
		ETCDPort:   2379,
		MysqlConfig: MysqlConfig{
			Ip:     "127.0.0.1",
			Port:   "23306",
			User:   "root",
			Passwd: "foobar",
		},
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
