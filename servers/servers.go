package main

import (
	"fmt"
	"net/http"
	"os"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run server.go <port>")
		return
	}

	port := os.Args[1]
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Println("Начало работы")

		// Задержка на 2 секунды
		time.Sleep(2 * time.Second)

		fmt.Println("Прошло 2 секунды")
		fmt.Fprintf(w, "Response from server on port %s\n", port)
	})

	fmt.Printf("Server started on :%s\n", port)
	http.ListenAndServe(":"+port, nil)
}
