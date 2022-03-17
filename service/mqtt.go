package service

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/url"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/influxdata/influxdb1-client/models"
	uuid "github.com/satori/go.uuid"

	"github.com/RedTimeDB/RedTimeProxy/backend"
	"github.com/RedTimeDB/RedTimeProxy/transfer"
)

var (
	ErrEmptyMQTT = errors.New("circles cannot be empty")
	ErrEmptyDB   = errors.New("db cannot find in db list")
)

type MQTTService struct {
	ip        *backend.Proxy
	tx        *transfer.Transfer
	mqtt      paho.Client
	db        string
	precision string
}

func NewMQTTService(cfg *backend.ProxyConfig) (us *MQTTService, err error) {
	ip := backend.NewProxy(cfg) //create influx proxy object by config
	if cfg.MQTT == nil {
		err = ErrEmptyMQTT
		return
	}
	db := cfg.MQTT.Db
	if len(ip.DBSet) > 0 && !ip.DBSet[db] {
		err = ErrEmptyDB
		return
	}
	u, err := url.Parse(cfg.MQTT.Server)
	if err != nil {
		return nil, err
	}
	mqtt := paho.NewClient(&paho.ClientOptions{
		Servers:              []*url.URL{u},
		ClientID:             ClientId(cfg.MQTT.ClientID),
		Username:             cfg.MQTT.User,
		Password:             cfg.MQTT.Password,
		KeepAlive:            30,
		PingTimeout:          10 * time.Second,
		ConnectTimeout:       30 * time.Second,
		MaxReconnectInterval: 10 * time.Minute,
		AutoReconnect:        true,
	})
	if err := check(mqtt.Connect()); err != nil {
		return nil, err
	}
	precision := cfg.MQTT.Precision
	if precision == "" {
		precision = "ns"
	}
	us = &MQTTService{
		ip:        ip,
		tx:        transfer.NewTransfer(cfg, ip.Circles),
		mqtt:      mqtt,
		precision: precision,
		db:        db,
	}
	return
}

func ClientId(clientId string) string {
	if len(clientId) > 0 {
		return clientId
	}
	return uuid.NewV4().String()
}

func check(t paho.Token) error {
	if err := t.Error(); t.Wait() && err != nil {
		return err
	}
	return nil
}

type Message struct {
	Topic string
	Value interface{}
	Time  time.Time
}

func (c *MQTTService) Collect() <-chan Message {
	messages := make(chan Message)

	const allMessages = "#"
	if err := check(c.mqtt.Subscribe(allMessages, 0, func(c paho.Client, m paho.Message) {
		j := map[string]interface{}{}
		if err := json.Unmarshal(m.Payload(), &j); err == nil {
			messages <- Message{
				Topic: m.Topic(),
				Value: j,
				Time:  time.Now(),
			}
		} else {
			log.Printf("Payload parsing error: %s, Payload: %v", err.Error(), m.Payload())
		}
	})); err != nil {
		log.Println(err)
	}
	return messages
}

func (c *MQTTService) WriteMQTT(msg Message) {
	pt, _ := models.NewPoint(msg.Topic,
		models.NewTags(map[string]string{}),
		msg.Value.(map[string]interface{}),
		msg.Time)
	fields, _ := pt.Fields()

	influxmsg := fmt.Sprintf("%s %s %d\n", pt.Key(), string(fields.MarshalBinary()),
		pt.UnixNano()/models.GetPrecisionMultiplier(c.precision))

	err := c.ip.Write([]byte(influxmsg), c.db, c.precision)
	if err != nil {
		log.Println(err)
	}
}

func (c *MQTTService) Shutdown() {
	log.Println("Shutting down MQTT client")
	c.mqtt.Disconnect(200)
}

func (c *MQTTService) OnMessage() {
	log.Println("Shutting down MQTT client")
	c.mqtt.Disconnect(200)
}
