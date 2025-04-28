package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// Config структура для конфигурации балансировщика
type Config struct {
	Port      int      `json:"port"`
	Backends  []string `json:"backends"`
	Algorithm string   `json:"algorithm"` // "round-robin", "least-connections", "random"
}

// BackendStatus хранит информацию о состоянии бэкенда
type BackendStatus struct {
	URL          *url.URL
	Alive        bool
	mutex        sync.RWMutex
	ActiveConns  int
	Weight       int
	ResponseTime time.Duration
}

// LoadBalancer основной объект балансировщика
type LoadBalancer struct {
	backends     []*BackendStatus
	algorithm    string
	mutex        sync.RWMutex
	rrIndex      uint64
	shuttingDown bool
}

var (
	config     Config
	lb         *LoadBalancer
	configFile string
)

func init() {
	flag.StringVar(&configFile, "config", "config.json", "Path to config file")
	flag.Parse()
	rand.Seed(time.Now().UnixNano())
}

func main() {
	loadConfig()
	lb = NewLoadBalancer(config.Backends, config.Algorithm)
	// Запуск проверки состояния бэкендов
	healthCtx, healthCancel := context.WithCancel(context.Background())
	go healthCheck(healthCtx)

	server := http.Server{
		Addr:    fmt.Sprintf(":%d", config.Port),
		Handler: http.HandlerFunc(lb.ServeHTTP),
	}
	// Канал для graceful shutdown
	done := make(chan bool)
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	// Запуск сервера в отдельной горутине
	go func() {
		log.Printf("Load balancer started on :%d with %s algorithm\n", config.Port, config.Algorithm)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Could not start server: %v\n", err)
		}
	}()

	// Ожидание сигнала завершения
	<-quit
	log.Println("Server is shutting down...")

	// Помечаем, что начинается shutdown
	lb.mutex.Lock()
	lb.shuttingDown = true
	lb.mutex.Unlock()

	// Создаем контекст с таймаутом для graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// Останавливаем health checks
	healthCancel()

	// Пытаемся корректно завершить работу сервера
	if err := server.Shutdown(ctx); err != nil {
		log.Fatalf("Could not gracefully shutdown the server: %v\n", err)
	}

	// Ждем завершения всех активных соединений
	waitForActiveConnections()

	close(done)
	log.Println("Server stopped")
}

func NewLoadBalancer(backendUrls []string, algorithm string) *LoadBalancer {
	lb := &LoadBalancer{
		algorithm: algorithm,
	}

	for _, bu := range backendUrls {
		backendUrl, err := url.Parse(bu)
		if err != nil {
			log.Fatal(err)
		}

		proxy := httputil.NewSingleHostReverseProxy(backendUrl)
		proxy.ErrorHandler = func(writer http.ResponseWriter, request *http.Request, e error) {
			log.Printf("[%s] %s\n", backendUrl.Host, e.Error())
			lb.markBackendStatus(backendUrl, false)
			lb.ServeHTTP(writer, request)
		}

		lb.backends = append(lb.backends, &BackendStatus{
			URL:   backendUrl,
			Alive: true,
		})
	}

	return lb
}

func (lb *LoadBalancer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Проверяем, не начался ли shutdown
	lb.mutex.RLock()
	if lb.shuttingDown {
		w.Header().Set("Connection", "close")
		http.Error(w, "Server is shutting down", http.StatusServiceUnavailable)
		lb.mutex.RUnlock()
		return
	}
	lb.mutex.RUnlock()

	for {
		backend := lb.selectBackend()
		if backend == nil {
			http.Error(w, "Service not available", http.StatusServiceUnavailable)
			return
		}

		// Увеличиваем счетчик активных соединений
		lb.incrementConnections(backend, true)

		// Создаем прокси и обрабатываем запрос
		proxy := httputil.NewSingleHostReverseProxy(backend.URL)
		start := time.Now()
		proxy.ServeHTTP(w, r)
		duration := time.Since(start)

		// Обновляем время ответа и уменьшаем счетчик соединений
		lb.updateBackendStats(backend, duration)
		lb.incrementConnections(backend, false)

		return
	}
}

func waitForActiveConnections() {
	log.Println("Waiting for active connections to complete...")
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		allZero := true
		lb.mutex.RLock()
		for _, b := range lb.backends {
			b.mutex.RLock()
			if b.ActiveConns > 0 {
				allZero = false
				log.Printf("Waiting for %d connections on %s", b.ActiveConns, b.URL.Host)
			}
			b.mutex.RUnlock()
		}
		lb.mutex.RUnlock()

		if allZero {
			break
		}
		<-ticker.C
	}
}

/*
selectBackend выбирает бэкенд согласно заданному алгоритму
*/
func (lb *LoadBalancer) selectBackend() *BackendStatus {
	lb.mutex.Lock()
	defer lb.mutex.Unlock()

	var availableBackends []*BackendStatus
	for _, b := range lb.backends {
		b.mutex.RLock()
		if b.Alive {
			availableBackends = append(availableBackends, b)
		}
		b.mutex.RUnlock()
	}

	if len(availableBackends) == 0 {
		return nil
	}

	switch lb.algorithm {
	case "random":
		return availableBackends[rand.Intn(len(availableBackends))]
	case "least-connections":
		return selectLeastConnections(availableBackends)
	default: // round-robin
		return selectRoundRobin(availableBackends, &lb.rrIndex)
	}
}

/*
selectRoundRobin реализует алгоритм round-robin
*/
func selectRoundRobin(backends []*BackendStatus, index *uint64) *BackendStatus {
	*index = (*index + 1) % uint64(len(backends))
	return backends[*index]
}

/*
selectLeastConnections реализует алгоритм least-connections
*/
func selectLeastConnections(backends []*BackendStatus) *BackendStatus {
	var selected *BackendStatus
	minConns := -1

	for _, b := range backends {
		b.mutex.RLock()
		conns := b.ActiveConns
		b.mutex.RUnlock()

		if selected == nil || conns < minConns {
			selected = b
			minConns = conns
		}
	}

	return selected
}

/*
incrementConnections обновляет счетчик активных соединений
*/
func (lb *LoadBalancer) incrementConnections(backend *BackendStatus, increment bool) {
	for _, b := range lb.backends {
		if b.URL.String() == backend.URL.String() {
			b.mutex.Lock()
			if increment {
				b.ActiveConns++
			} else {
				b.ActiveConns--
			}
			b.mutex.Unlock()
			return
		}
	}
}

func (lb *LoadBalancer) updateBackendStats(backend *BackendStatus, duration time.Duration) {
	for _, b := range lb.backends {
		if b.URL.String() == backend.URL.String() {
			b.mutex.Lock()
			b.ResponseTime = duration
			b.mutex.Unlock()
			return
		}
	}
}

func (lb *LoadBalancer) markBackendStatus(backendUrl *url.URL, alive bool) {
	lb.mutex.Lock()
	defer lb.mutex.Unlock()

	for _, b := range lb.backends {
		if b.URL.String() == backendUrl.String() {
			b.mutex.Lock()
			b.Alive = alive
			if !alive {
				b.ActiveConns = 0 // Сбрасываем счетчик соединений для недоступного бэкенда
			}
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
	_ = conn.Close()
	return true
}

/*
healthCheck регулярно проверяет состояние бэкендов
*/
func healthCheck(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Stopping health checks...")
			return
		case <-ticker.C:
			lb.mutex.Lock()
			for _, b := range lb.backends {
				alive := isBackendAlive(b.URL)
				b.mutex.Lock()
				b.Alive = alive
				status := "up"
				if !alive {
					status = "down"
					b.ActiveConns = 0
				}
				log.Printf("Backend %s is %s (conns: %d, resp time: %v)",
					b.URL.Host, status, b.ActiveConns, b.ResponseTime)
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

	// Проверяем есть ли контрактные алгоритмы в jsone
	switch config.Algorithm {
	case "round-robin", "least-connections", "random":
	default:
		log.Printf("Unknown algorithm '%s', defaulting to 'round-robin'", config.Algorithm)
		config.Algorithm = "round-robin"
	}
}
