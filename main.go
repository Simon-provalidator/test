package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

// WebSocket endpoint path
// `/websocket` 경로를 유지
const wsPath = "/websocket"

// 🔴 RPC Endpoint Change: Use a highly stable public RPC endpoint (Polkachu)
// 이전: const rpcEndpoint = "wss://rpc-cosmoshub.whispernode.com"
const rpcEndpoint = "wss://cosmos-rpc.polkachu.com"

// Event query to subscribe to
const subscribeQuery = "tm.event='Tx'"

// JSON-RPC Request struct
type JSONRPCRequest struct {
	JSONRPC string                 `json:"jsonrpc"`
	Method  string                 `json:"method"`
	ID      string                 `json:"id"`
	Params  map[string]interface{} `json:"params"`
}

// --- JSONRPCNotification internal struct definitions ---

// NotificationValue: TxResult 안에 Height, Log, Tx가 있음
type NotificationValue struct {
	TxResult struct {
		Height string `json:"height"`
		Log    string `json:"log"`
		Tx     string `json:"tx"`
	} `json:"TxResult"`
}

// NotificationData: type과 Value를 포함
type NotificationData struct {
	Type  string            `json:"type"`
	Value NotificationValue `json:"value"`
}

// SubscriptionResult: RPC 노드에 따라 이벤트 메시지가 Result 필드 내부에 `data` 객체를 포함할 때 사용
type SubscriptionResult struct {
	Query string           `json:"query"`
	Data  NotificationData `json:"data"` // 실제 이벤트 데이터
}

// GenericNotification: 모든 수신 메시지 구조를 포괄
type GenericNotification struct {
	JSONRPC string              `json:"jsonrpc"`
	ID      string              `json:"id,omitempty"`
	Result  *SubscriptionResult `json:"result,omitempty"` // Result 필드를 파싱할 구조체로 변경
	Method  string              `json:"method,omitempty"`
	Params  json.RawMessage     `json:"params,omitempty"`
}

// --------------------------------------------------------------------------

// TxEventPayload: Single transaction data to cache/save
type TxEventPayload struct {
	SubscriptionResult        // NotificationParams 대신 SubscriptionResult 사용
	CaptureTime        string `json:"capture_time"`
}

// BlockTxSummary: Transaction summary per block (for file saving)
type BlockTxSummary struct {
	BlockHeight  string           `json:"block_height"`
	TotalTxs     int              `json:"total_txs"`
	Transactions []TxEventPayload `json:"transactions"`
}

var blockTxCache = make(map[string][]TxEventPayload)
var currentBlockHeight string
var cacheMutex sync.Mutex

// ProcessAndSaveBlock: Core function to save the structure to a JSON file
func ProcessAndSaveBlock(identifier string, txs []TxEventPayload) error {
	if len(txs) == 0 {
		return nil
	}

	log.Printf("⏳ [Processing Start]: Starting to save %d transactions for identifier '%s'...", identifier, len(txs))

	saveDir := "./save"
	if _, err := os.Stat(saveDir); os.IsNotExist(err) {
		log.Printf("📁 [Folder Creation]: Creating save folder (%s) as it does not exist.", saveDir)
		if err := os.Mkdir(saveDir, 0755); err != nil {
			return fmt.Errorf("failed to create folder (%s): %w", saveDir, err)
		}
	}

	summary := BlockTxSummary{
		BlockHeight:  identifier,
		TotalTxs:     len(txs),
		Transactions: txs,
	}

	dataBytes, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal struct to JSON: %w", err)
	}

	fileName := fmt.Sprintf("%s/%s.json", saveDir, identifier)

	if err := os.WriteFile(fileName, dataBytes, 0644); err != nil {
		return fmt.Errorf("failed to write JSON file (%s): %w", fileName, err)
	}

	log.Printf("✅ [Save Complete]: Summary struct for identifier '%s' saved to JSON file. (Path: %s, Total Txs: %d)", identifier, fileName, summary.TotalTxs)
	return nil
}

func main() {
	log.Println("🚀 [Program Start] Cosmos Hub WebSocket Client")

	ctx, cancel := context.WithCancel(context.Background())
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGINT)

	url := rpcEndpoint + wsPath
	log.Printf("🌐 [Connection Attempt]: Attempting to connect to WebSocket endpoint: %s", url)

	// 💡 변경: 대부분의 공개 노드에서 Origin 헤더를 보내지 않거나 빈 맵을 사용하면 연결이 성공합니다.
	requestHeaders := map[string][]string{}

	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}

	// Pass requestHeaders (now empty) as the third argument in dialer.Dial call
	conn, _, err := dialer.Dial(url, requestHeaders)
	if err != nil {
		log.Fatalf("❌ [Connection Failed]: WebSocket connection failed: %v", err)
	}

	log.Println("✅ [Connection Success]: WebSocket connection successfully established.")

	// Go routine for SIGINT handling (final save on exit)
	go func() {
		<-sigChan
		log.Println("\n🛑 [Shutdown Signal Detected] Received Ctrl+C signal.")

		if err := conn.Close(); err != nil {
			log.Printf("🚨 Connection close error: %v", err)
		}
		log.Println("🌐 [Connection Close]: WebSocket connection forced close triggered.")
		time.Sleep(100 * time.Millisecond)

		log.Println("🧹 [Final Cleanup]: Starting cache data save...")

		cacheMutex.Lock()
		defer cacheMutex.Unlock()

		if len(blockTxCache) > 0 {
			log.Printf("🧹 [Final Cleanup]: Attempting to save all cached blocks (%d) before exit...", len(blockTxCache))

			for height, txs := range blockTxCache {
				if err := ProcessAndSaveBlock(height, txs); err != nil {
					log.Printf("🚨 Final save error (Block %s): %v", height, err)
				}
				delete(blockTxCache, height)
			}
			log.Printf("✅ [Final Cleanup]: All cached block data saved.")
		} else {
			log.Println("🧹 [Final Cleanup]: No cache data to save.")
		}

		cancel()
	}()

	// Send Subscribe Request
	const requestID = "1"
	subscribeRequest := JSONRPCRequest{
		JSONRPC: "2.0",
		Method:  "subscribe",
		ID:      requestID,
		Params: map[string]interface{}{
			"query": subscribeQuery,
		},
	}

	requestBytes, err := json.Marshal(subscribeRequest)
	if err != nil {
		log.Fatalf("❌ [Request Error]: Failed to marshal request JSON: %v", err)
	}

	log.Printf("📤 [Subscribe Request]: Sending event subscription request. Query: %s, ID: %s", subscribeQuery, requestID)
	if err := conn.WriteMessage(websocket.TextMessage, requestBytes); err != nil {
		log.Fatalf("❌ [Send Failed]: Failed to send subscription request: %v", err)
	}

	log.Println("👂 [Awaiting]: Waiting for real-time transaction events... (Ctrl+C to stop)")

	// Message reception loop
	done := make(chan error, 1)

	go func() {
		subscriptionResponseReceived := false

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			_, message, err := conn.ReadMessage()
			if err != nil {
				select {
				case <-ctx.Done():
					done <- nil
					return
				default:
				}

				errStr := err.Error()
				if strings.Contains(errStr, "use of closed network connection") ||
					websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) ||
					websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway) {
					done <- nil
					return
				}

				done <- err
				return
			}

			// 1st level generic parsing
			var generic GenericNotification
			if err := json.Unmarshal(message, &generic); err != nil {
				log.Printf("⚠️ [Parsing Error-1st]: Failed to parse generic message. Full message: %s", string(message))
				continue
			}

			// ------------------------------------------------------------------
			// 🔍 [NEW DEBUG]: Log all received messages before filtering
			// ------------------------------------------------------------------
			// Params Exists: generic.Params != nil && len(generic.Params) > 0 // 기존 필드
			// Result Exists: generic.Result != nil && generic.Result.Data.Type != "" // 새로 추가된 필드 확인
			isParamsExist := generic.Params != nil && len(generic.Params) > 0
			isResultExist := generic.Result != nil && generic.Result.Data.Type != ""
			log.Printf("🔍 [RX Message Debug]: ID: '%s', Method: '%s', Params Exists: %t, Result Exists: %t",
				generic.ID, generic.Method, isParamsExist, isResultExist)
			// ------------------------------------------------------------------

			// ------------------------------------------------------------------
			// ⭐️ 핵심 수정: ID='1'이더라도 Result 내에 데이터가 있으면 이벤트 메시지로 간주
			// ------------------------------------------------------------------

			// Check if this is a standard RPC Success response with no event data (initial confirmation)
			if generic.ID == requestID && !isResultExist {
				log.Printf("➡️ [Subscription Success Ack]: Received initial subscription acknowledgement for ID '%s'.", generic.ID)

				if !subscriptionResponseReceived {
					subscriptionResponseReceived = true
					log.Println("⏳ [Awaiting]: Waiting 2 seconds for transaction collection before starting save upon subscription response...")
					time.Sleep(2 * time.Second)

					// Check and save cache upon subscription response (async)
					cacheMutex.Lock()
					if len(blockTxCache) > 0 {
						for height, txs := range blockTxCache {
							txsCopy := make([]TxEventPayload, len(txs))
							copy(txsCopy, txs)

							log.Printf("✨ [Subscription Response Save]: Attempting async save of cached block %s data (%d items).", height, len(txsCopy))
							go func(h string, t []TxEventPayload) {
								if err := ProcessAndSaveBlock(h, t); err != nil {
									log.Printf("🚨 Async save error (Block %s): %v", h, err)
								}
							}(height, txsCopy)
						}
						log.Printf("💾 [Subscription Response Save]: Completed async saving of cached data.")
					} else {
						log.Println("⚠️ [Cache Warning]: No transaction data in cache to save at the time of subscription response.")
					}
					cacheMutex.Unlock()
				}
				continue
			}

			// EVENT MESSAGE HANDLING (Covers both Method='event' and ID='1' with Result data)
			// Event messages often have ID='1' and contain data in the Result field.
			if !isResultExist {
				// Result가 없는, event Method도 아닌 메시지는 무시 (e.g., ping/pong, heartbeat)
				continue
			}

			// 이제 generic.Result는 SubscriptionResult 타입이며, nil이 아닙니다.
			// Result 내부에 있는 NotificationValue에서 TxResult를 가져옵니다.
			txResult := generic.Result.Data.Value.TxResult
			newHeight := txResult.Height

			if newHeight == "" {
				log.Printf("⚠️ [Event Skip]: Is an event (Result Exists), but transaction height (Height) information is missing.")
				continue
			}

			// ------------------------------------------------------------------
			// ⭐️ Logic to immediately save previous block data upon block transition (Async)
			// ------------------------------------------------------------------
			cacheMutex.Lock()

			if currentBlockHeight != "" && newHeight != currentBlockHeight {
				oldHeight := currentBlockHeight

				if txsToSave, exists := blockTxCache[oldHeight]; exists {
					txsCopy := make([]TxEventPayload, len(txsToSave))
					copy(txsCopy, txsToSave)

					delete(blockTxCache, oldHeight)

					log.Printf("📈 [Block Transition Detected]: Transitioning from Block %s to Block %s. Starting async save for previous block (%s, Txs: %d).", oldHeight, newHeight, oldHeight, len(txsCopy))

					go func(h string, txs []TxEventPayload) {
						if err := ProcessAndSaveBlock(h, txs); err != nil {
							log.Printf("🚨 Async save error (Block %s): %v", h, err)
						}
					}(oldHeight, txsCopy)
				}
			}

			// ------------------------------------------------------------------

			// 1. Update current block height
			currentBlockHeight = newHeight

			// 2. Add current transaction data to the cache in struct format
			dataToCache := TxEventPayload{
				SubscriptionResult: *generic.Result,
				CaptureTime:        time.Now().Format(time.RFC3339),
			}

			// Debug log output
			dataToCacheJSON, _ := json.MarshalIndent(dataToCache, "   ", "  ")
			log.Printf("--------------------------------------------------")
			log.Printf("🔔 [Tx Data]: TxEventPayload struct content (Block %s) - Pre-cache:", newHeight)
			log.Println(string(dataToCacheJSON))
			log.Printf("--------------------------------------------------")

			blockTxCache[newHeight] = append(blockTxCache[newHeight], dataToCache)
			log.Printf("✅ [Tx Cache]: Tx saved to Block %s. (Current cached Tx count: %d)",
				newHeight, len(blockTxCache[newHeight]))

			var totalCachedTxs int
			for _, txs := range blockTxCache {
				totalCachedTxs += len(txs)
			}
			log.Printf("📊 [Cache Status]: Total %d transactions cached across %d blocks. (Current Height: %s)", totalCachedTxs, len(blockTxCache), currentBlockHeight)

			cacheMutex.Unlock()
		}
	}()

	// Wait for shutdown
	select {
	case err := <-done:
		if err != nil && err != context.Canceled {
			errStr := err.Error()
			if strings.Contains(errStr, "use of closed network connection") ||
				websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) ||
				websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway) {
				log.Println("🛑 [Normal Shutdown]: Expected connection close error occurred. Shutting down program.")
			} else {
				log.Printf("❌ [Error Shutdown]: Program error occurred: %v", err)
			}
		} else {
			log.Println("🛑 [Normal Shutdown]: Program shutdown signal received. Shutdown complete.")
		}
	case <-ctx.Done():
		log.Println("🛑 [Normal Shutdown]: Program shutdown signal received due to Context cancellation.")
	}

	cancel()
	log.Println("👋 [Program Exit]")
}
