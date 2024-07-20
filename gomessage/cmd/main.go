package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/IBM/sarama"
	_ "github.com/lib/pq"
	"github.com/sirupsen/logrus"
)

type Message struct {
	// ID   int    `json:"id"`
	Text string `json:"text"`
	// SentAt    string `json:"sent_at"`
	// Processed bool   `json:"processed"`
}

func main() {

	// Initialize Kafka producer
	// kafkaProducer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, nil)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// defer kafkaProducer.Close()

	http.HandleFunc("/message", handleMessage)

	fmt.Println("Server started on port 8080")
	http.ListenAndServe(":8080", nil)
}

func handleMessage(w http.ResponseWriter, r *http.Request) {
	var msg Message

	err := json.NewDecoder(r.Body).Decode(&msg)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	fmt.Printf("Received message: %s\n", msg.Text)
	// Initialize database connection
	connStr := "user=admin password=root dbname=gomessage sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Insert the message into the database
	_, err = db.Exec("INSERT INTO messages (text) VALUES ($1)", msg.Text)
	if err != nil {
		logrus.Fatal(err)
	}

	// Создаем producer для Kafka
	brokers := []string{"localhost:9092"}
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Fatal(err)
	}

	// Отправляем сообщение в Kafka
	msgBytes, err := json.Marshal(msg.Text)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_, _, err = producer.SendMessage(&sarama.ProducerMessage{
		Topic: "my_topic",
		Value: sarama.ByteEncoder(msgBytes),
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	fmt.Printf("Kafka")

	// Create a new Kafka consumer
	consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, nil)
	if err != nil {
		fmt.Println(err)
		return
	}

	// Subscribe to the topic
	topic := "my_topic"
	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		fmt.Println(err)
		return
	}
	w.WriteHeader(http.StatusAccepted)
	// Consume messages from the topic
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			fmt.Printf("Received message: %s\n", msg.Value)
			// Process the message and mark it as processed
			// Mark the message as processed and write it to the database
			// _, err = db.Exec("INSERT INTO messages (message, processed) VALUES ($1, true)", string(message))
			// if err != nil {
			// 	fmt.Println(err)
			// 	return
			// }

			fmt.Println("Message marked as processed and written to database")
		}
	}

}
