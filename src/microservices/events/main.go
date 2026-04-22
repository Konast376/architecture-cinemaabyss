package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
)

type MovieEvent struct {
	MovieID     int      `json:"movie_id"`
	Title       string   `json:"title"`
	Action      string   `json:"action"`
	UserID      *int     `json:"user_id,omitempty"`
	Rating      *float64 `json:"rating,omitempty"`
	Genres      []string `json:"genres,omitempty"`
	Description *string  `json:"description,omitempty"`
}

type UserEvent struct {
	UserID    int       `json:"user_id"`
	Username  *string   `json:"username,omitempty"`
	Email     *string   `json:"email,omitempty"`
	Action    string    `json:"action"`
	Timestamp time.Time `json:"timestamp"`
}

type PaymentEvent struct {
	PaymentID  int       `json:"payment_id"`
	UserID     int       `json:"user_id"`
	Amount     float64   `json:"amount"`
	Status     string    `json:"status"`
	Timestamp  time.Time `json:"timestamp"`
	MethodType *string   `json:"method_type,omitempty"`
}

type Event struct {
	ID        string      `json:"id"`
	Type      string      `json:"type"` // "movie", "user", "payment"
	Timestamp time.Time   `json:"timestamp"`
	Payload   interface{} `json:"payload"`
}

// Ответ API
type EventResponse struct {
	Status    string `json:"status"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Event     Event  `json:"event"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}

var (
	producer sarama.SyncProducer
	topic    string
)

func main() {
	brokers := getEnv("KAFKA_BROKERS", "localhost:9092")
	topic = getEnv("KAFKA_TOPIC", "cinema-events")
	consumerGroup := getEnv("KAFKA_CONSUMER_GROUP", "events-service")

	// Инициализация producer
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 3

	var err error
	producer, err = sarama.NewSyncProducer([]string{brokers}, config)
	if err != nil {
		log.Fatalf("Failed to create Kafka producer: %v", err)
	}
	defer producer.Close()
	log.Println("Kafka producer initialized")

	// Инициализация consumer (запуск в фоне)
	consumerConfig := sarama.NewConfig()
	consumerConfig.Consumer.Return.Errors = true
	consumerConfig.Consumer.Offsets.Initial = sarama.OffsetOldest

	client, err := sarama.NewConsumerGroup([]string{brokers}, consumerGroup, consumerConfig)
	if err != nil {
		log.Fatalf("Failed to create consumer group: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go consumeMessages(ctx, client)

	// HTTP сервер
	http.HandleFunc("/api/events/health", handleHealth)
	http.HandleFunc("/api/events/movie", handleMovieEvent)
	http.HandleFunc("/api/events/user", handleUserEvent)
	http.HandleFunc("/api/events/payment", handlePaymentEvent)

	port := getEnv("EVENTS_PORT", "8082")
	srv := &http.Server{
		Addr:         ":" + port,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		log.Printf("Events service listening on port %s", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	<-stop
	log.Println("Shutting down...")
	cancel()
	ctxShutdown, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShutdown()
	if err := srv.Shutdown(ctxShutdown); err != nil {
		log.Printf("HTTP server shutdown error: %v", err)
	}
}

func getEnv(key, defaultValue string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultValue
}

type eventConsumerGroupHandler struct{}

func (eventConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (eventConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }
func (h eventConsumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		log.Printf("Kafka consumer received: topic=%s, partition=%d, offset=%d, key=%s, value=%s",
			msg.Topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
		sess.MarkMessage(msg, "")
	}
	return nil
}

func consumeMessages(ctx context.Context, client sarama.ConsumerGroup) {
	handler := eventConsumerGroupHandler{}
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if err := client.Consume(ctx, []string{topic}, handler); err != nil {
				log.Printf("Consumer error: %v", err)
				time.Sleep(1 * time.Second)
			}
		}
	}
}

// ----- HTTP handlers -----
func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"status": true})
}

func handleMovieEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var payload MovieEvent
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		sendError(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	if payload.MovieID == 0 || payload.Title == "" || payload.Action == "" {
		sendError(w, "Missing required fields: movie_id, title, action", http.StatusBadRequest)
		return
	}
	event := Event{
		ID:        uuid.New().String(),
		Type:      "movie",
		Timestamp: time.Now(),
		Payload:   payload,
	}
	sendToKafkaAndRespond(w, event)
}

func handleUserEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var payload UserEvent
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		sendError(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	if payload.UserID == 0 || payload.Action == "" || payload.Timestamp.IsZero() {
		sendError(w, "Missing required fields: user_id, action, timestamp", http.StatusBadRequest)
		return
	}
	event := Event{
		ID:        uuid.New().String(),
		Type:      "user",
		Timestamp: time.Now(),
		Payload:   payload,
	}
	sendToKafkaAndRespond(w, event)
}

func handlePaymentEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var payload PaymentEvent
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		sendError(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	if payload.PaymentID == 0 || payload.UserID == 0 || payload.Amount == 0 || payload.Status == "" || payload.Timestamp.IsZero() {
		sendError(w, "Missing required fields: payment_id, user_id, amount, status, timestamp", http.StatusBadRequest)
		return
	}
	event := Event{
		ID:        uuid.New().String(),
		Type:      "payment",
		Timestamp: time.Now(),
		Payload:   payload,
	}
	sendToKafkaAndRespond(w, event)
}

// Отправка события в Kafka и формирование ответа
func sendToKafkaAndRespond(w http.ResponseWriter, event Event) {
	eventJSON, err := json.Marshal(event)
	if err != nil {
		sendError(w, "Failed to serialize event", http.StatusInternalServerError)
		return
	}

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(event.Type),
		Value: sarama.ByteEncoder(eventJSON),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		log.Printf("Failed to send message to Kafka: %v", err)
		sendError(w, "Failed to publish event", http.StatusInternalServerError)
		return
	}

	log.Printf("Event published: type=%s, id=%s, partition=%d, offset=%d", event.Type, event.ID, partition, offset)

	resp := EventResponse{
		Status:    "success",
		Partition: partition,
		Offset:    offset,
		Event:     event,
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(resp)
}

func sendError(w http.ResponseWriter, message string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(ErrorResponse{Error: message})
}

