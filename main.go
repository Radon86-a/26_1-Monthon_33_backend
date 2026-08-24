package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	// 開発用：すべての接続元を許可
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	// HTTP接続をWebSocketにアップグレード
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Upgrade error:", err)
		return
	}
	defer conn.Close()

	fmt.Println("Client connected!")

	for {
		// クライアントからのメッセージを受信
		messageType, p, err := conn.ReadMessage()
		if err != nil {
			log.Println("Read error / Disconnected:", err)
			break
		}

		fmt.Printf("Received from client: %s\n", string(p))

		// 受け取ったメッセージをそのまま返信（エコー）
		response := fmt.Sprintf("Server echo: %s", string(p))
		if err := conn.WriteMessage(messageType, []byte(response)); err != nil {
			log.Println("Write error:", err)
			break
		}
	}
}

func main() {
	http.HandleFunc("/ws", handleWebSocket)

	fmt.Println("WebSocket server listening on ws://localhost:8080/ws")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal("ListenAndServe error:", err)
	}
}