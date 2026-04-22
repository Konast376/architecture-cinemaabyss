package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	monolithURL      string
	moviesURL        string
	migrationPercent int
	gradualMigration bool
	rng              *rand.Rand
)

func init() {
	rng = rand.New(rand.NewSource(time.Now().UnixNano()))
}

func main() {
	monolithURL = getEnv("MONOLITH_URL", "http://localhost:8080")
	moviesURL = getEnv("MOVIES_SERVICE_URL", "http://localhost:8081")
	percentStr := getEnv("MOVIES_MIGRATION_PERCENT", "0")

	var err error
	migrationPercent, err = strconv.Atoi(percentStr)
	if err != nil || migrationPercent < 0 || migrationPercent > 100 {
		log.Fatalf("Invalid MOVIES_MIGRATION_PERCENT: must be 0..100, got %s", percentStr)
	}

	gradualMigration = getEnv("GRADUAL_MIGRATION", "true") == "true"

	port := getEnv("PROXY_PORT", "8000")

	http.HandleFunc("/health", handleHealth)
	http.HandleFunc("/api/movies/health", handleHealth)
	http.HandleFunc("/", proxyHandler)

	server := &http.Server{
		Addr:    ":" + port,
		Handler: nil,
	}

	log.Printf("Proxy started on port %s, movies migration = %d%%, gradual migration = %v", port, migrationPercent, gradualMigration)
	log.Fatal(server.ListenAndServe())
}

func getEnv(key, defaultValue string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultValue
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func proxyHandler(w http.ResponseWriter, r *http.Request) {
	// Пропускаем health-запросы
	if r.URL.Path == "/health" || r.URL.Path == "/api/movies/health" {
		handleHealth(w, r)
		return
	}

	var targetURL string

	// Для /api/movies используем логику постепенного переключения
	if strings.HasPrefix(r.URL.Path, "/api/movies") && shouldUseMoviesService() {
		targetURL = moviesURL
		log.Printf("-> movies: %s %s", r.Method, r.URL.Path)
	} else {
		targetURL = monolithURL
		log.Printf("-> monolith: %s %s", r.Method, r.URL.Path)
	}

	target, err := url.Parse(targetURL)
	if err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		log.Printf("URL parse error: %v", err)
		return
	}

	proxy := httputil.NewSingleHostReverseProxy(target)
	r.URL.Host = target.Host
	r.URL.Scheme = target.Scheme
	r.Host = target.Host

	proxy.ServeHTTP(w, r)
}

func shouldUseMoviesService() bool {
	if !gradualMigration {
		return false
	}
	if migrationPercent == 0 {
		return false
	}
	if migrationPercent == 100 {
		return true
	}
	return rng.Intn(100) < migrationPercent
}
