package main

import (
	"encoding/json"
	"flag"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"sync"
	"time"
)

// Config структура для конфигурации балансировщика
type Config struct {
	Port     int      `json:"port"`
	Backends []string `json:"backends"`
}

type BackendStatus struct {
	URL    *url.URL
	Alive  bool
	mutex  sync.RWMutex
	Weight int // Можно использовать для взвешенного распределения
}

type LoadBalancer struct {
	backends []*BackendStatus
	current  uint64
	mutex    sync.RWMutex
}

var (
	config     Config
	lb         *LoadBalancer
	configFile string
)

func init() {
	flag.StringVar(&configFile, "config", "config.json", "Path to config file")
	flag.Parse()
}

func main() {
	// Загрузка конфигурации
	loadConfig()

	// Инициализация балансировщика
	lb = NewLoadBalancer(config.Backends)

	// Запуск проверки состояния бэкендов
	go healthCheck()
	println(string(rune(config.Port)))
	// Настройка сервера
	server := http.Server{
		Addr:    ":" + strconv.Itoa(config.Port),
		Handler: http.HandlerFunc(lb.balance),
	}

	log.Printf("Load balancer started on :%d\n", config.Port)
	if err := server.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}

func NewLoadBalancer(backendUrls []string) *LoadBalancer {
	lb := &LoadBalancer{}

	for _, bu := range backendUrls {
		backendUrl, err := url.Parse(bu)
		if err != nil {
			log.Fatal(err)
		}

		proxy := httputil.NewSingleHostReverseProxy(backendUrl)
		proxy.ErrorHandler = func(writer http.ResponseWriter, request *http.Request, e error) {
			log.Printf("[%s] %s\n", backendUrl.Host, e.Error())
			lb.markBackendStatus(backendUrl, false)

			// Попробовать еще раз
			lb.balance(writer, request)
		}

		lb.backends = append(lb.backends, &BackendStatus{
			URL:    backendUrl,
			Alive:  true,
			Weight: 1,
		})
	}

	return lb
}

func (lb *LoadBalancer) balance(w http.ResponseWriter, r *http.Request) {
	attempts := 0
	for {
		backend := lb.getNextBackend()
		if backend == nil {
			http.Error(w, "Service not available", http.StatusServiceUnavailable)
			return
		}

		lb.mutex.Lock()
		backend.mutex.RLock()
		alive := backend.Alive
		backendUrl := backend.URL
		backend.mutex.RUnlock()
		lb.mutex.Unlock()

		if alive {
			log.Printf("%s -> %s\n", r.RemoteAddr, backendUrl.Host)
			proxy := httputil.NewSingleHostReverseProxy(backendUrl)
			proxy.ServeHTTP(w, r)
			return
		}

		attempts++
		if attempts >= len(lb.backends) {
			break
		}
	}

	http.Error(w, "No available backends", http.StatusServiceUnavailable)
}

func (lb *LoadBalancer) getNextBackend() *BackendStatus {
	lb.mutex.Lock()
	defer lb.mutex.Unlock()

	next := int((lb.current + 1) % uint64(len(lb.backends)))
	liveness := 0

	for i := next; liveness < len(lb.backends); i++ {
		idx := i % len(lb.backends)
		backend := lb.backends[idx]

		backend.mutex.RLock()
		alive := backend.Alive
		backend.mutex.RUnlock()

		if alive {
			lb.current = uint64(idx)
			return backend
		}
		liveness++
	}

	return nil
}

func (lb *LoadBalancer) markBackendStatus(backendUrl *url.URL, alive bool) {
	lb.mutex.Lock()
	defer lb.mutex.Unlock()

	for _, b := range lb.backends {
		if b.URL.String() == backendUrl.String() {
			b.mutex.Lock()
			b.Alive = alive
			b.mutex.Unlock()
			return
		}
	}
}

func isBackendAlive(u *url.URL) bool {
	timeout := 2 * time.Second
	conn, err := net.DialTimeout("tcp", u.Host, timeout)
	if err != nil {
		log.Printf("Backend %s is down: %s\n", u.Host, err.Error())
		return false
	}
	defer conn.Close()
	return true
}

func healthCheck() {
	t := time.NewTicker(10 * time.Second)
	for {
		select {
		case <-t.C:
			lb.mutex.Lock()
			for _, b := range lb.backends {
				alive := isBackendAlive(b.URL)
				b.mutex.Lock()
				b.Alive = alive
				status := "up"
				if !alive {
					status = "down"
				}
				log.Printf("Backend %s is %s\n", b.URL.Host, status)
				b.mutex.Unlock()
			}
			lb.mutex.Unlock()
		}
	}
}

func loadConfig() {
	file, err := os.ReadFile(configFile)
	if err != nil {
		log.Fatal(err)
	}

	if err := json.Unmarshal(file, &config); err != nil {
		log.Fatal(err)
	}

	if len(config.Backends) == 0 {
		log.Fatal("No backends configured")
	}
}
