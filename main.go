package main

import (
	"fmt"
	"net/http"
)

func main() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "Card Game Server is running!")
	})

	fmt.Println("Server started on :8080")
	http.ListenAndServe(":8080", nil)
}