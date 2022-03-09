package service

import (
	"github.com/panjf2000/ants/v2"
	"github.com/RedTimeDB/RedTimeProxy/backend"
	"github.com/RedTimeDB/RedTimeProxy/transfer"
	"log"
	"net"
	"sync/atomic"
	"time"
)

// UDPService is UDP server
type UDPService struct {
	ip           *backend.Proxy
	tx           *transfer.Transfer
	WriteTracing bool
	UDPBind      string // UDP监控地址
	UDPDatabase  string // UDP数据库
	UDPPoolSize  int
	UDPPrecision string
	Count        uint64
}

// NewUDPService is create udp server object
func NewUDPService(cfg *backend.ProxyConfig) (us *UDPService) { // nolint:golint
	ip := backend.NewProxy(cfg) //create influx proxy object by config
	us = &UDPService{
		ip:           ip,
		tx:           transfer.NewTransfer(cfg, ip.Circles),
		UDPBind:      cfg.UDPBind,
		UDPDatabase:  cfg.UDPDataBase,
		WriteTracing: cfg.WriteTracing,
		UDPPoolSize:  cfg.UDPPoolSize,
		UDPPrecision: cfg.UDPPrecision,
	}
	//go us.count()
	return
}

// ListenAndServe is UDP server Bind Handle

// TODO 可以参考 backend.NewBackend 的 方式，创建一个协程池出来
func (us *UDPService) ListenAndServe() (err error) {
	pc, err := net.ListenPacket("udp", us.UDPBind)

	if err != nil {
		return err
	}
	defer pc.Close()
	log.Printf("UDP service start on DB [%s], listen %s", us.UDPDatabase, us.UDPBind)

	// 获取 一个 指定大小的 缓冲池
	poolBuffer := backend.NewPool(2048, 2048)
	// 开始创建 一个 协诚池
	pool, err := ants.NewPool(us.UDPPoolSize)
	if err != nil {
		log.Printf("udp poll err: %s", err)
		return err
	}
	defer pool.Release()
	for {
		//buf := make([]byte, 1024)
		buf := poolBuffer.Get()
		// TODO 调用 ReadFrom 方法接收到数据后，处理数据花费时间太长，再次调用 ReadFrom，两次调用间隔里，发过来的包可能丢失。 通过将接收到数据存入一个缓冲区，并迅速返回继续 ReadFrom
		//n, addr, err := pc.ReadFrom(buf)
		n, _, err := pc.ReadFrom(buf)
		if err != nil {
			log.Println(err)
			poolBuffer.Put(buf) // 如果错误 就归还
			continue
		}
		ants.Submit(func() {
			us.process(poolBuffer, buf[:n])
		})
	}
	return
}

// process 进程执行
func (us *UDPService) process(pool *backend.Pool, buf []byte) {
	atomic.AddUint64(&us.Count, 1)
	defer pool.Put(buf) // 正常执行后 释放 已占用的
	if us.WriteTracing {
		log.Printf("write: [%s %s]\n", us.UDPDatabase, buf)
	}
	err := us.ip.Write(buf, us.UDPDatabase, us.UDPPrecision)
	if err != nil {
		log.Println(err)
	}
}

func (us *UDPService) count() {
	for {
		log.Println(atomic.LoadUint64(&us.Count))
		time.Sleep(time.Second)
	}
}
