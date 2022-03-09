package service

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/pprof"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	gzip "github.com/klauspost/pgzip"
	"github.com/RedTimeDB/RedTimeProxy/backend"
	"github.com/RedTimeDB/RedTimeProxy/transfer"
	"github.com/RedTimeDB/RedTimeProxy/util"
)

var (
	ErrInvalidTick    = errors.New("invalid tick, require non-negative integer")
	ErrInvalidWorker  = errors.New("invalid worker, require positive integer")
	ErrInvalidBatch   = errors.New("invalid batch, require positive integer")
	ErrInvalidLimit   = errors.New("invalid limit, require positive integer")
	ErrInvalidHaAddrs = errors.New("invalid ha_addrs, require at least two addresses as <host:port>, comma-separated")
)

type HttpService struct { // nolint:golint
	ip           *backend.Proxy
	tx           *transfer.Transfer
	Username     string
	Password     string
	AuthSecure   bool
	WriteTracing bool
	QueryTracing bool
	count        uint64
}

// NewHTTPService is create http server object
func NewHTTPService(cfg *backend.ProxyConfig) (hs *HttpService) { // nolint:golint
	ip := backend.NewProxy(cfg) //create influx proxy object by config
	hs = &HttpService{
		ip:           ip,
		tx:           transfer.NewTransfer(cfg, ip.Circles),
		Username:     cfg.Username,
		Password:     cfg.Password,
		AuthSecure:   cfg.AuthSecure,
		WriteTracing: cfg.WriteTracing,
		QueryTracing: cfg.QueryTracing,
	}
	//go hs.Count()
	return
}

// Register is create routes of  http services
func (hs *HttpService) Register(mux *http.ServeMux) {
	mux.HandleFunc("/ping", hs.handlerPing)
	mux.HandleFunc("/query", hs.handlerQuery)
	mux.HandleFunc("/write", hs.handlerWrite)
	mux.HandleFunc("/health", hs.handlerHealth)
	mux.HandleFunc("/replica", hs.handlerReplica)
	mux.HandleFunc("/encrypt", hs.handlerEncrypt)
	mux.HandleFunc("/decrypt", hs.handlerDencrypt)
	mux.HandleFunc("/rebalance", hs.handlerRebalance)
	mux.HandleFunc("/recovery", hs.handlerRecovery)
	mux.HandleFunc("/resync", hs.handlerResync)
	mux.HandleFunc("/cleanup", hs.handlerCleanup)
	mux.HandleFunc("/transfer/state", hs.handlerTransferState)
	mux.HandleFunc("/transfer/stats", hs.handlerTransferStats)
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
}

func (hs *HttpService) handlerPing(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	hs.WriteHeader(w, 204)
}

func (hs *HttpService) handlerQuery(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	if !hs.checkMethodAndAuth(w, req, "GET", "POST") {
		return
	}

	db := req.FormValue("db")
	q := req.FormValue("q")
	body, err := hs.ip.Query(w, req)
	if err != nil {
		log.Printf("query error: %s, query: %s %s %s, client: %s", err, req.Method, db, q, req.RemoteAddr)
		hs.writeError(w, req, 400, err.Error())
		return
	}
	hs.writeBody(w, body)
	if hs.QueryTracing {
		log.Printf("query: %s %s %s, client: %s", req.Method, db, q, req.RemoteAddr)
	}
}

func (hs *HttpService) handlerWrite(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	atomic.AddUint64(&hs.count, 1)
	if !hs.checkMethodAndAuth(w, req, "POST") {
		return
	}

	precision := req.URL.Query().Get("precision")
	if precision == "" {
		precision = "ns"
	}
	db := req.URL.Query().Get("db")
	if db == "" {
		hs.writeError(w, req, 400, "database not found")
		return
	}
	if len(hs.ip.DBSet) > 0 && !hs.ip.DBSet[db] {
		hs.writeError(w, req, 400, fmt.Sprintf("database forbidden: %s", db))
		return
	}

	body := req.Body
	if req.Header.Get("Content-Encoding") == "gzip" {
		b, err := gzip.NewReader(body)
		if err != nil {
			hs.writeError(w, req, 400, "unable to decode gzip body")
			return
		}
		defer b.Close()
		body = b
	}
	p, err := ioutil.ReadAll(body)
	if err != nil {
		hs.writeError(w, req, 400, err.Error())
		return
	}

	err = hs.ip.Write(p, db, precision)
	if err == nil {
		hs.WriteHeader(w, 204)
	}
	if hs.WriteTracing {
		log.Printf("write: %s %s %s, client: %s", db, precision, p, req.RemoteAddr)
	}
}

func (hs *HttpService) handlerHealth(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	if !hs.checkMethodAndAuth(w, req, "GET") {
		return
	}
	hs.Write(w, req, 200, hs.ip.GetHealth())
}

func (hs *HttpService) handlerReplica(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	if !hs.checkMethodAndAuth(w, req, "GET") {
		return
	}

	db := req.FormValue("db")
	meas := req.FormValue("meas")
	if db != "" && meas != "" {
		key := backend.GetKey(db, meas)
		backends := hs.ip.GetBackends(key)
		data := make([]map[string]interface{}, len(backends))
		for i, b := range backends {
			c := hs.ip.Circles[i]
			data[i] = map[string]interface{}{
				"backend": map[string]string{"name": b.Name, "url": b.Url},
				"circle":  map[string]interface{}{"id": c.CircleId, "name": c.Name},
			}
		}
		hs.Write(w, req, 200, data)
	} else {
		hs.writeError(w, req, 400, "invalid db or meas")
	}
}

func (hs *HttpService) handlerEncrypt(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	if !hs.checkMethod(w, req, "GET") {
		return
	}
	text := req.URL.Query().Get("text")
	encrypt := util.AesEncrypt(text)
	hs.writeText(w, 200, encrypt)
}

func (hs *HttpService) handlerDencrypt(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	if !hs.checkMethod(w, req, "GET") {
		return
	}
	key := req.URL.Query().Get("key")
	text := req.URL.Query().Get("text")
	if !util.CheckCipherKey(key) {
		hs.writeError(w, req, 400, "invalid key")
		return
	}
	decrypt := util.AesDecrypt(text)
	hs.writeText(w, 200, decrypt)
}

func (hs *HttpService) handlerRebalance(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	if !hs.checkMethodAndAuth(w, req, "POST") {
		return
	}

	circleId, err := hs.formCircleId(req, "circle_id") // nolint:golint
	if err != nil {
		hs.writeError(w, req, 400, err.Error())
		return
	}
	operation := req.FormValue("operation")
	if operation != "add" && operation != "rm" {
		hs.writeError(w, req, 400, "invalid operation")
		return
	}

	var backends []*backend.Backend
	if operation == "rm" {
		var body struct {
			Backends []*backend.Config `json:"backends"`
		}
		decoder := json.NewDecoder(req.Body)
		err := decoder.Decode(&body)
		if err != nil {
			hs.writeError(w, req, 400, "invalid backends from body")
			return
		}
		for _, bkcfg := range body.Backends {
			backends = append(backends, backend.NewSimpleBackend(bkcfg))
			hs.tx.CircleStates[circleId].Stats[bkcfg.Url] = &transfer.Stats{}
		}
	}
	backends = append(backends, hs.ip.Circles[circleId].Backends...)

	if hs.tx.CircleStates[circleId].Transferring {
		hs.writeText(w, 400, fmt.Sprintf("circle %d is transferring", circleId))
		return
	}
	if hs.tx.Resyncing {
		hs.writeText(w, 400, "proxy is resyncing")
		return
	}

	err = hs.setParam(req)
	if err != nil {
		hs.writeError(w, req, 400, err.Error())
		return
	}

	dbs := hs.formValues(req, "dbs")
	go hs.tx.Rebalance(circleId, backends, dbs)
	hs.writeText(w, 202, "accepted")
}

func (hs *HttpService) handlerRecovery(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	if !hs.checkMethodAndAuth(w, req, "POST") {
		return
	}

	fromCircleId, err := hs.formCircleId(req, "from_circle_id") // nolint:golint
	if err != nil {
		hs.writeError(w, req, 400, err.Error())
		return
	}
	toCircleId, err := hs.formCircleId(req, "to_circle_id") // nolint:golint
	if err != nil {
		hs.writeError(w, req, 400, err.Error())
		return
	}
	if fromCircleId == toCircleId {
		hs.writeError(w, req, 400, "from_circle_id and to_circle_id cannot be same")
		return
	}

	if hs.tx.CircleStates[fromCircleId].Transferring || hs.tx.CircleStates[toCircleId].Transferring {
		hs.writeText(w, 400, fmt.Sprintf("circle %d or %d is transferring", fromCircleId, toCircleId))
		return
	}
	if hs.tx.Resyncing {
		hs.writeText(w, 400, "proxy is resyncing")
		return
	}

	err = hs.setParam(req)
	if err != nil {
		hs.writeError(w, req, 400, err.Error())
		return
	}

	backendUrls := hs.formValues(req, "backend_urls")
	dbs := hs.formValues(req, "dbs")
	go hs.tx.Recovery(fromCircleId, toCircleId, backendUrls, dbs)
	hs.writeText(w, 202, "accepted")
}

func (hs *HttpService) handlerResync(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	if !hs.checkMethodAndAuth(w, req, "POST") {
		return
	}

	tick, err := hs.formTick(req)
	if err != nil {
		hs.writeError(w, req, 400, err.Error())
		return
	}

	for _, cs := range hs.tx.CircleStates {
		if cs.Transferring {
			hs.writeText(w, 400, fmt.Sprintf("circle %d is transferring", cs.CircleId))
			return
		}
	}
	if hs.tx.Resyncing {
		hs.writeText(w, 400, "proxy is resyncing")
		return
	}

	err = hs.setParam(req)
	if err != nil {
		hs.writeError(w, req, 400, err.Error())
		return
	}

	dbs := hs.formValues(req, "dbs")
	go hs.tx.Resync(dbs, tick)
	hs.writeText(w, 202, "accepted")
}

func (hs *HttpService) handlerCleanup(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	if !hs.checkMethodAndAuth(w, req, "POST") {
		return
	}

	circleId, err := hs.formCircleId(req, "circle_id") // nolint:golint
	if err != nil {
		hs.writeError(w, req, 400, err.Error())
		return
	}

	if hs.tx.CircleStates[circleId].Transferring {
		hs.writeText(w, 400, fmt.Sprintf("circle %d is transferring", circleId))
		return
	}
	if hs.tx.Resyncing {
		hs.writeText(w, 400, "proxy is resyncing")
		return
	}

	err = hs.setParam(req)
	if err != nil {
		hs.writeError(w, req, 400, err.Error())
		return
	}

	go hs.tx.Cleanup(circleId)
	hs.writeText(w, 202, "accepted")
}

func (hs *HttpService) handlerTransferState(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	if !hs.checkMethodAndAuth(w, req, "GET", "POST") {
		return
	}

	if req.Method == "GET" {
		data := make([]map[string]interface{}, len(hs.tx.CircleStates))
		for k, cs := range hs.tx.CircleStates {
			data[k] = map[string]interface{}{
				"id":           cs.CircleId,
				"name":         cs.Name,
				"transferring": cs.Transferring,
			}
		}
		state := map[string]interface{}{"resyncing": hs.tx.Resyncing, "circles": data}
		hs.Write(w, req, 200, state)
		return
	} else if req.Method == "POST" {
		state := make(map[string]interface{})
		if req.FormValue("resyncing") != "" {
			resyncing, err := hs.formBool(req, "resyncing")
			if err != nil {
				hs.writeError(w, req, 400, "illegal resyncing")
				return
			}
			hs.tx.Resyncing = resyncing
			state["resyncing"] = resyncing
		}
		if req.FormValue("circle_id") != "" || req.FormValue("transferring") != "" {
			circleId, err := hs.formCircleId(req, "circle_id") // nolint:golint
			if err != nil {
				hs.writeError(w, req, 400, err.Error())
				return
			}
			transferring, err := hs.formBool(req, "transferring")
			if err != nil {
				hs.writeError(w, req, 400, "illegal transferring")
				return
			}
			cs := hs.tx.CircleStates[circleId]
			cs.Transferring = transferring
			cs.WriteOnly = transferring
			state["circle"] = map[string]interface{}{
				"id":           cs.CircleId,
				"name":         cs.Name,
				"transferring": cs.Transferring,
			}
		}
		if len(state) == 0 {
			hs.writeError(w, req, 400, "missing query parameter")
			return
		}
		hs.Write(w, req, 200, state)
		return
	}
}

func (hs *HttpService) handlerTransferStats(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()
	if !hs.checkMethodAndAuth(w, req, "GET") {
		return
	}

	circleId, err := hs.formCircleId(req, "circle_id") // nolint:golint
	if err != nil {
		hs.writeError(w, req, 400, err.Error())
		return
	}

	statsType := req.FormValue("type")
	if statsType == "rebalance" || statsType == "recovery" || statsType == "resync" || statsType == "cleanup" {
		hs.Write(w, req, 200, hs.tx.CircleStates[circleId].Stats)
	} else {
		hs.writeError(w, req, 400, "invalid stats type")
	}
}

func (hs *HttpService) Write(w http.ResponseWriter, req *http.Request, status int, data interface{}) {
	if status >= 400 {
		hs.writeError(w, req, status, data.(string))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	hs.WriteHeader(w, status)
	pretty := req.URL.Query().Get("pretty") == "true"
	w.Write(util.MarshalJSON(data, pretty))
}

func (hs *HttpService) writeError(w http.ResponseWriter, req *http.Request, status int, err string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Influxdb-Error", err)
	hs.WriteHeader(w, status)
	rsp := backend.ResponseFromError(err)
	pretty := req.URL.Query().Get("pretty") == "true"
	w.Write(util.MarshalJSON(rsp, pretty))
}

func (hs *HttpService) writeBody(w http.ResponseWriter, body []byte) {
	hs.WriteHeader(w, 200)
	w.Write(body)
}

func (hs *HttpService) writeText(w http.ResponseWriter, status int, text string) {
	hs.WriteHeader(w, status)
	w.Write([]byte(text + "\n"))
}

func (hs *HttpService) WriteHeader(w http.ResponseWriter, status int) {
	w.Header().Set("X-Influxdb-Version", backend.Version)
	w.WriteHeader(status)
}

func (hs *HttpService) checkMethodAndAuth(w http.ResponseWriter, req *http.Request, methods ...string) bool {
	return hs.checkMethod(w, req, methods...) && hs.checkAuth(w, req)
}

func (hs *HttpService) checkMethod(w http.ResponseWriter, req *http.Request, methods ...string) bool {
	for _, method := range methods {
		if req.Method == method {
			return true
		}
	}
	hs.writeError(w, req, 405, "method not allow")
	return false
}

func (hs *HttpService) checkAuth(w http.ResponseWriter, req *http.Request) bool {
	if hs.Username == "" && hs.Password == "" {
		return true
	}
	u, p := req.URL.Query().Get("u"), req.URL.Query().Get("p")
	if hs.transAuth(u) == hs.Username && hs.transAuth(p) == hs.Password {
		return true
	}
	u, p, ok := req.BasicAuth()
	if ok && hs.transAuth(u) == hs.Username && hs.transAuth(p) == hs.Password {
		return true
	}
	hs.writeError(w, req, 401, "authentication failed")
	return false
}

func (hs *HttpService) transAuth(text string) string {
	if hs.AuthSecure {
		return util.AesEncrypt(text)
	}
	return text
}

func (hs *HttpService) formValues(req *http.Request, key string) []string {
	var values []string
	str := strings.Trim(req.FormValue(key), ", ")
	if str != "" {
		values = strings.Split(str, ",")
	}
	return values
}

func (hs *HttpService) formBool(req *http.Request, key string) (bool, error) {
	return strconv.ParseBool(req.FormValue(key))
}

func (hs *HttpService) formTick(req *http.Request) (int64, error) {
	str := strings.TrimSpace(req.FormValue("tick"))
	if str == "" {
		return 0, nil
	}
	tick, err := strconv.ParseInt(str, 10, 64)
	if err != nil || tick < 0 {
		return 0, ErrInvalidTick
	}
	return tick, nil
}

func (hs *HttpService) formCircleId(req *http.Request, key string) (int, error) { // nolint:golint
	circleId, err := strconv.Atoi(req.FormValue(key)) // nolint:golint
	if err != nil || circleId < 0 || circleId >= len(hs.ip.Circles) {
		return circleId, fmt.Errorf("invalid %s", key)
	}
	return circleId, nil
}

func (hs *HttpService) setParam(req *http.Request) error {
	var err error
	err = hs.setWorker(req)
	if err != nil {
		return err
	}
	err = hs.setBatch(req)
	if err != nil {
		return err
	}
	err = hs.setLimit(req)
	if err != nil {
		return err
	}
	err = hs.setHaAddrs(req)
	if err != nil {
		return err
	}
	return nil
}

func (hs *HttpService) setWorker(req *http.Request) error {
	str := strings.TrimSpace(req.FormValue("worker"))
	if str != "" {
		worker, err := strconv.Atoi(str)
		if err != nil || worker <= 0 {
			return ErrInvalidWorker
		}
		hs.tx.Worker = worker
	} else {
		hs.tx.Worker = transfer.DefaultWorker
	}
	return nil
}

func (hs *HttpService) setBatch(req *http.Request) error {
	str := strings.TrimSpace(req.FormValue("batch"))
	if str != "" {
		batch, err := strconv.Atoi(str)
		if err != nil || batch <= 0 {
			return ErrInvalidBatch
		}
		hs.tx.Batch = batch
	} else {
		hs.tx.Batch = transfer.DefaultBatch
	}
	return nil
}

func (hs *HttpService) setLimit(req *http.Request) error {
	str := strings.TrimSpace(req.FormValue("limit"))
	if str != "" {
		limit, err := strconv.Atoi(str)
		if err != nil || limit <= 0 {
			return ErrInvalidLimit
		}
		hs.tx.Limit = limit
	} else {
		hs.tx.Limit = transfer.DefaultLimit
	}
	return nil
}

func (hs *HttpService) setHaAddrs(req *http.Request) error {
	haAddrs := hs.formValues(req, "ha_addrs")
	if len(haAddrs) > 1 {
		r, _ := regexp.Compile(`^[\w-.]+:\d{1,5}$`)
		for _, addr := range haAddrs {
			if !r.MatchString(addr) {
				return ErrInvalidHaAddrs
			}
		}
		hs.tx.HaAddrs = haAddrs
	} else if len(haAddrs) == 1 {
		return ErrInvalidHaAddrs
	}
	return nil
}

func (hs *HttpService) Count() {
	for {
		log.Println(atomic.LoadUint64(&hs.count))
		time.Sleep(time.Second)
	}
}
