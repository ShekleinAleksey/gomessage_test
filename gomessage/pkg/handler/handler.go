package handler

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/sirupsen/logrus"
	"github.com/zhashkevych/todo-app/pkg/service"
)

type Handler struct {
	services *service.Service
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

	w.WriteHeader(http.StatusAccepted)
}
