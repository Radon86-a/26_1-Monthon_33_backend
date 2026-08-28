package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// 通信メッセージの基本構造
type Message struct {
	Type    string          `json:"type"`              // "match_found", "game_start", etc.
	Payload json.RawMessage `json:"payload,omitempty"` // 詳細データ
}

// クライアントを表す構造体
type Client struct {
	ID   string
	Conn *websocket.Conn
	Room *Room
	mu   sync.Mutex
}

func (c *Client) SendJSON(v interface{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.Conn.WriteJSON(v)
}

// 対戦部屋を表す構造体
type Room struct {
	ID      string
	Player1 *Client
	Player2 *Client
}

// マネージャー（待機キューとアクティブルームを管理）
type Hub struct {
	waitingClient *Client
	rooms         map[string]*Room
	mu            sync.Mutex
}

var hub = Hub{
	rooms: make(map[string]*Room),
}

// マッチング処理
func (h *Hub) Register(client *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.waitingClient == nil {
		// 1人目：待機キューに入れる
		h.waitingClient = client
		fmt.Printf("[Hub] Player %s is waiting for opponent...\n", client.ID)

		// 待機中であることを通知
		client.SendJSON(map[string]string{
			"type":    "waiting",
			"message": "Waiting for an opponent...",
		})
	} else {
		// 2人目：マッチング成立！ルームを作成
		p1 := h.waitingClient
		p2 := client
		h.waitingClient = nil

		roomID := uuid.New().String()
		room := &Room{
			ID:      roomID,
			Player1: p1,
			Player2: p2,
		}
		p1.Room = room
		p2.Room = room
		h.rooms[roomID] = room

		fmt.Printf("[Hub] Matched! Room %s created for %s and %s\n", roomID, p1.ID, p2.ID)

		// Player 1 への通知 (先行)
		p1.SendJSON(map[string]interface{}{
			"type":        "match_found",
			"room_id":     roomID,
			"player_id":   p1.ID,
			"is_first":    true,
			"opponent_id": p2.ID,
		})

		// Player 2 への通知 (後攻)
		p2.SendJSON(map[string]interface{}{
			"type":        "match_found",
			"room_id":     roomID,
			"player_id":   p2.ID,
			"is_first":    false,
			"opponent_id": p1.ID,
		})
	}
}

// 切断時の処理
func (h *Hub) Unregister(client *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.waitingClient == client {
		h.waitingClient = nil
		fmt.Printf("[Hub] Waiting player %s disconnected\n", client.ID)
	}

	if client.Room != nil {
		room := client.Room
		delete(h.rooms, room.ID)

		// 対戦相手に切断を通知
		var opponent *Client
		if room.Player1 == client {
			opponent = room.Player2
		} else {
			opponent = room.Player1
		}

		if opponent != nil {
			opponent.Room = nil
			opponent.SendJSON(map[string]string{
				"type":    "opponent_disconnected",
				"message": "Opponent left the match.",
			})
		}
	}
}

func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Upgrade error:", err)
		return
	}

	client := &Client{
		ID:   uuid.New().String()[:8],
		Conn: conn,
	}

	// 接続成功をクライアントへ通知
	client.SendJSON(map[string]interface{}{
		"type":      "connected",
		"player_id": client.ID,
		"message":   "Connected to server successfully.",
	})

	defer func() {
		hub.Unregister(client)
		client.Conn.Close()
	}()

	for {
		_, p, err := client.Conn.ReadMessage()
		if err != nil {
			log.Printf("[Client %s] Disconnected\n", client.ID)
			break
		}

		// 受信したJSONメッセージのタイプを判定
		var msg Message
		if err := json.Unmarshal(p, &msg); err != nil {
			continue
		}

		switch msg.Type {
		case "join_match":
			// クライアントが「対戦を探す」を押したときにマッチングキューへ登録
			hub.Register(client)
		}
	}
}

func main() {
	http.HandleFunc("/ws", handleWebSocket)
	fmt.Println("Card Game Server running on ws://localhost:8080/ws")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal(err)
	}
}