package backend

import (
	"bytes"
	"io"
	"log"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/panjf2000/ants/v2"
)

type CacheBuffer struct {
	Buffer  *bytes.Buffer
	Counter uint64
}

type Backend struct {
	*HttpBackend
	fb   *FileBackend
	pool *ants.Pool

	flushSize       uint64
	flushTime       int
	rewriteInterval int
	rewriteTicker   *time.Ticker
	rewriteRunning  bool
	chWrite         chan *LinePoint
	chTimer         <-chan time.Time
	buffers         map[string]*CacheBuffer
	wg              sync.WaitGroup
}

func NewBackend(cfg *Config, pxcfg *ProxyConfig) (ib *Backend) {
	ib = &Backend{
		HttpBackend:     NewHttpBackend(cfg, pxcfg),
		flushSize:       pxcfg.FlushSize,
		flushTime:       pxcfg.FlushTime,
		rewriteInterval: pxcfg.RewriteInterval,
		rewriteTicker:   time.NewTicker(time.Duration(pxcfg.RewriteInterval) * time.Second),
		rewriteRunning:  false,
		chWrite:         make(chan *LinePoint, 16),
		buffers:         make(map[string]*CacheBuffer),
	}

	var err error
	ib.fb, err = NewFileBackend(cfg.Name, pxcfg.DataDir)
	if err != nil {
		panic(err)
	}
	ib.pool, err = ants.NewPool(pxcfg.ConnPoolSize)
	if err != nil {
		panic(err)
	}

	go ib.worker()
	return
}

func NewSimpleBackend(cfg *Config) *Backend {
	return &Backend{HttpBackend: NewSimpleHttpBackend(cfg)}
}

func (ib *Backend) worker() {
	for {
		select {
		case p, ok := <-ib.chWrite:
			if !ok {
				// closed
				ib.Flush()
				ib.wg.Wait()
				ib.HttpBackend.Close()
				ib.fb.Close()
				return
			}
			ib.WriteBuffer(p)

		case <-ib.chTimer:
			ib.Flush()

		case <-ib.rewriteTicker.C:
			ib.RewriteIdle()
		}
	}
}

func (ib *Backend) WritePoint(point *LinePoint) (err error) {
	ib.chWrite <- point
	return
}

func (ib *Backend) WriteBuffer(point *LinePoint) (err error) {
	db, line := point.Db, point.Line
	cb, ok := ib.buffers[db]
	if !ok {
		ib.buffers[db] = &CacheBuffer{Buffer: &bytes.Buffer{}}
		cb = ib.buffers[db]
	}

	atomic.AddUint64(&cb.Counter, 1)
	//cb.Counter++
	if cb.Buffer == nil {
		cb.Buffer = &bytes.Buffer{}
	}
	n, err := cb.Buffer.Write(line)
	if err != nil {
		log.Printf("buffer write error: %s", err)
		return
	}
	if n != len(line) {
		err = io.ErrShortWrite
		log.Printf("buffer write error: %s", err)
		return
	}
	if line[len(line)-1] != '\n' {
		err = cb.Buffer.WriteByte('\n')
		if err != nil {
			log.Printf("buffer write error: %s", err)
			return
		}
	}

	switch {
	case atomic.LoadUint64(&cb.Counter) >= ib.flushSize:
		ib.FlushBuffer(db)
	case ib.chTimer == nil:
		ib.chTimer = time.After(time.Duration(ib.flushTime) * time.Second)
	}
	return
}

func (ib *Backend) FlushBuffer(db string) {
	cb := ib.buffers[db]
	if cb.Buffer == nil {
		return
	}
	p := cb.Buffer.Bytes()
	ib.Lock()
	cb.Buffer = nil
	cb.Counter = 0
	ib.Unlock()
	if len(p) == 0 {
		return
	}

	ib.pool.Submit(func() {
		// 关闭压缩、此处有内存泄漏
		// var buf bytes.Buffer
		// err = Compress(&buf, p)
		// if err != nil {
		// 	log.Print("compress buffer error: ", err)
		// 	return
		// }

		// p = buf.Bytes()

		if ib.IsActive() {
			err := ib.WriteUNCompressed(db, p)
			switch err {
			case nil:
				return
			case ErrBadRequest:
				log.Printf("bad request, drop all data")
				return
			case ErrNotFound:
				log.Printf("bad backend, drop all data")
				return
			default:
				log.Printf("write http error: %s %s, length: %d", ib.Url, db, len(p))
			}
		}

		b := bytes.Join([][]byte{[]byte(url.QueryEscape(db)), p}, []byte{' '})
		err := ib.fb.Write(b)
		if err != nil {
			log.Printf("write db and data to file error with db: %s, length: %d error: %s", db, len(p), err)
		}
	})
}

func (ib *Backend) Flush() {
	ib.chTimer = nil
	for db := range ib.buffers {
		if atomic.LoadUint64(&ib.buffers[db].Counter) > 0 {
			ib.FlushBuffer(db)
		}
	}
}

func (ib *Backend) SetRewriteRunning(running bool) {
	ib.Lock()
	defer ib.Unlock()
	ib.rewriteRunning = running
}

func (ib *Backend) RewriteIdle() {
	if !ib.rewriteRunning && ib.fb.IsData() {
		ib.SetRewriteRunning(true)
		go ib.RewriteLoop()
	}
}

func (ib *Backend) RewriteLoop() {
	for ib.fb.IsData() {
		if !ib.IsActive() {
			time.Sleep(time.Duration(ib.rewriteInterval) * time.Second)
			continue
		}
		err := ib.Rewrite()
		if err != nil {
			time.Sleep(time.Duration(ib.rewriteInterval) * time.Second)
			continue
		}
	}
	ib.rewriteRunning = false
}

func (ib *Backend) Rewrite() (err error) {
	b, err := ib.fb.Read()
	if err != nil && err != io.EOF {
		log.Print("rewrite read file error: ", err)
		return
	}
	if err == io.EOF {
		err = nil
	}

	if b == nil {
		return
	}

	p := bytes.SplitN(b, []byte{' '}, 2)
	if len(p) < 2 {
		log.Print("rewrite read invalid data with length: ", len(p))
		return
	}
	db, err := url.QueryUnescape(string(p[0]))
	if err != nil {
		log.Print("rewrite db unescape error: ", err)
		return
	}
	//此处切换为非压缩写入，压缩有内存泄漏
	err = ib.WriteUNCompressed(db, p[1])

	switch err {
	case nil:
	case ErrBadRequest:
		log.Printf("bad request, drop all data")
		err = nil
	case ErrNotFound:
		log.Printf("bad backend, drop all data")
		err = nil
	default:
		log.Printf("rewrite http error: %s %s, length: %d", ib.Url, db, len(p[1]))

		err = ib.fb.RollbackMeta()
		if err != nil {
			log.Printf("rollback meta error: %s", err)
		}
		return
	}

	err = ib.fb.UpdateMeta()
	if err != nil {
		log.Printf("update meta error: %s", err)
	}
	return
}

func (ib *Backend) Close() {
	ib.pool.Release()
	close(ib.chWrite)
}

func (ib *Backend) GetHealth(ic *Circle) map[string]interface{} {
	var wg sync.WaitGroup
	var smap sync.Map
	dbs := ib.GetDatabases()
	for _, db := range dbs {
		wg.Add(1)
		go func(db string) {
			defer wg.Done()
			inplace, incorrect := 0, 0
			measurements := ib.GetMeasurements(db)
			for _, meas := range measurements {
				key := GetKey(db, meas)
				nb := ic.GetBackend(key)
				if nb.Url == ib.Url {
					inplace++
				} else {
					incorrect++
				}
			}
			smap.Store(db, map[string]int{
				"measurements": len(measurements),
				"inplace":      inplace,
				"incorrect":    incorrect,
			})
		}(db)
	}
	wg.Wait()
	stats := make(map[string]map[string]int)
	smap.Range(func(k, v interface{}) bool {
		stats[k.(string)] = v.(map[string]int)
		return true
	})
	return map[string]interface{}{
		"name":    ib.Name,
		"url":     ib.Url,
		"active":  ib.IsActive(),
		"backlog": ib.fb.IsData(),
		"rewrite": ib.rewriteRunning,
		"stats":   stats,
	}
}

func QueryInParallel(backends []*Backend, req *http.Request, w http.ResponseWriter, decompress bool) (bodies [][]byte, inactive int, err error) {
	var wg sync.WaitGroup
	var header http.Header
	req.Header.Set("Query-Origin", "Parallel")
	ch := make(chan *QueryResult, len(backends))
	for _, be := range backends {
		if !be.Active {
			inactive++
			continue
		}
		wg.Add(1)
		go func(be *Backend) {
			defer wg.Done()
			cr := CloneQueryRequest(req)
			ch <- be.Query(cr, nil, decompress)
		}(be)
	}
	go func() {
		wg.Wait()
		close(ch)
	}()
	for qr := range ch {
		if qr.Err != nil {
			err = qr.Err
			return
		}
		header = qr.Header
		bodies = append(bodies, qr.Body)
	}
	if w != nil {
		CopyHeader(w.Header(), header)
	}
	return
}
