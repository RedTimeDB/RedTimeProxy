package backend

import (
	"encoding/json"
	"errors"
	"gopkg.in/yaml.v2"
	"log"
	"os"
	"strings"

	"github.com/RedTimeDB/RedTimeProxy/util"
)

const (
	//Version Tag
	Version = "2.5.5"
)

var (
	ErrEmptyCircles          = errors.New("circles cannot be empty")
	ErrEmptyBackends         = errors.New("backends cannot be empty")
	ErrEmptyBackendName      = errors.New("backend name cannot be empty")
	ErrDuplicatedBackendName = errors.New("backend name duplicated")
	ErrInvalidHashKey        = errors.New("invalid hash_key, require idx, exi, name or url")
)

type Config struct { // nolint:golint
	Name       string `json:"name" yaml:"name"`
	Url        string `json:"url" yaml:"url"` // nolint:golint
	Username   string `json:"username" yaml:"username"`
	Password   string `json:"password" yaml:"password"`
	AuthSecure bool   `json:"auth_secure" yaml:"auth_secure"`
}

type CircleConfig struct {
	Name     string    `json:"name" yaml:"name"`
	Backends []*Config `json:"backends" yaml:"backends"`
}

type MQTTConfig struct {
	Server     string `yaml:"server"`
	TopicPath  string `yaml:"topic_path"`
	User       string `yaml:"user"`
	Password   string `yaml:"password"`
	QoS        byte   `yaml:"qos"`
	CACert     string `yaml:"ca_cert"`
	ClientCert string `yaml:"client_cert"`
	ClientKey  string `yaml:"client_key"`
	ClientID   string `yaml:"client_id"`
	Db         string `yaml:"db"`
	Precision  string `yaml:"precision"`
}

type ProxyConfig struct {
	Circles         []*CircleConfig `json:"circles" yaml:"circles"`
	ListenAddr      string          `json:"listen_addr" yaml:"listen_addr"`
	DBList          []string        `json:"db_list" yaml:"db_list"`
	DataDir         string          `json:"data_dir" yaml:"data_dir"`
	TLogDir         string          `json:"tlog_dir" yaml:"tlog_dir"`
	HashKey         string          `json:"hash_key" yaml:"hash_key"`
	FlushSize       uint64          `json:"flush_size" yaml:"flush_size"`
	FlushTime       int             `json:"flush_time" yaml:"flush_time"`
	CheckInterval   int             `json:"check_interval" yaml:"check_interval"`
	RewriteInterval int             `json:"rewrite_interval" yaml:"rewrite_interval"`
	ConnPoolSize    int             `json:"conn_pool_size" yaml:"conn_pool_size"`
	WriteTimeout    int             `json:"write_timeout" yaml:"write_timeout"`
	IdleTimeout     int             `json:"idle_timeout" yaml:"idle_timeout"`
	Username        string          `json:"username" yaml:"username"`
	Password        string          `json:"password" yaml:"password"`
	AuthSecure      bool            `json:"auth_secure" yaml:"auth_secure"`
	WriteTracing    bool            `json:"write_tracing" yaml:"write_tracing"`
	QueryTracing    bool            `json:"query_tracing" yaml:"query_tracing"`
	HTTPSEnabled    bool            `json:"https_enabled" yaml:"https_enabled"`
	HTTPSCert       string          `json:"https_cert" yaml:"https_cert"`
	HTTPSKey        string          `json:"https_key" yaml:"https_key"`
	UDPEnable       bool            `json:"udp_enable" yaml:"udp_enable"`
	UDPBind         string          `json:"udp_bind" yaml:"udp_bind"`
	UDPDataBase     string          `json:"udp_database" yaml:"udp_database"`
	UDPPoolSize     int             `json:"udp_pool_size" yaml:"udp_pool_size"`
	UDPPrecision    string          `json:"udp_precision" yaml:"udp_precision"`
	MQTTEnable      bool            `yaml:"mqtt_enable"`
	MQTT            *MQTTConfig     `json:"mqtt" yaml:"mqtt"`
}

// NewFileConfig is create a config from file
func NewFileConfig(cfgfile string) (cfg *ProxyConfig, err error) {
	cfg = &ProxyConfig{}
	file, err := os.Open(cfgfile)
	if err != nil {
		return
	}
	defer file.Close()
	dec := json.NewDecoder(file) //根据文件创建json解析体
	err = dec.Decode(cfg)        //解析json数据到结构体数据结构
	if err != nil {
		return
	}
	cfg.setDefault()        //为配置结构体设置默认值
	err = cfg.checkConfig() //检测配置是否正确
	if err != nil {
		return
	}
	return
}

// NewConfigYaml is create a config from file
func NewConfigYaml(cfgfile string) (cfg *ProxyConfig, err error) {
	cfg = &ProxyConfig{}
	file, err := os.Open(cfgfile)
	if err != nil {
		return
	}
	defer file.Close()
	dec := yaml.NewDecoder(file)
	err = dec.Decode(cfg) //解析 yaml 数据到结构体数据结构
	if err != nil {
		return nil, err
	}
	cfg.setDefault()        //为配置结构体设置默认值
	err = cfg.checkConfig() //检测配置是否正确
	if err != nil {
		return nil, err
	}
	return cfg, nil
}

func (cfg *ProxyConfig) setDefault() {
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = ":80"
	}
	if cfg.DataDir == "" {
		cfg.DataDir = "data"
	}
	if cfg.TLogDir == "" {
		cfg.TLogDir = "log"
	}
	if cfg.HashKey == "" {
		cfg.HashKey = "idx"
	}
	if cfg.FlushSize <= 0 {
		cfg.FlushSize = 10000
	}
	if cfg.FlushTime <= 0 {
		cfg.FlushTime = 1
	}
	if cfg.CheckInterval <= 0 {
		cfg.CheckInterval = 1
	}
	if cfg.RewriteInterval <= 0 {
		cfg.RewriteInterval = 10
	}
	if cfg.ConnPoolSize <= 0 {
		cfg.ConnPoolSize = 20
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = 10
	}
	if cfg.IdleTimeout <= 0 {
		cfg.IdleTimeout = 10
	}
}

func (cfg *ProxyConfig) checkConfig() (err error) {
	if len(cfg.Circles) == 0 {
		return ErrEmptyCircles
	}
	set := util.NewSet() //非并发安全：集合数据结构
	for _, circle := range cfg.Circles {
		if len(circle.Backends) == 0 {
			return ErrEmptyBackends
		}
		for _, backend := range circle.Backends {
			if backend.Name == "" {
				return ErrEmptyBackendName
			}
			if set[backend.Name] {
				return ErrDuplicatedBackendName
			}
			set.Add(backend.Name)
		}
	}

	switch strings.ToLower(cfg.HashKey) {
	case "idx":
	case "exi":
	case "name":
	case "url":
	default:
		return ErrInvalidHashKey
	}

	return
}

//PrintSummary is print influxDB cluster summary data
func (cfg *ProxyConfig) PrintSummary() {
	log.Printf("%d circles loaded from file", len(cfg.Circles))
	for id, circle := range cfg.Circles {
		log.Printf("circle %d: %d backends loaded", id, len(circle.Backends))
	}
	log.Printf("hash key: %s", cfg.HashKey)
	if len(cfg.DBList) > 0 {
		log.Printf("db list: %v", cfg.DBList)
	}
}
