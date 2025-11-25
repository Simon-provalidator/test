package main

/*
import (
	"context"
	"encoding/base64"
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
const wsPath = "/websocket"
const rpcEndpoint = "wss://cosmos-rpc.polkachu.com"
const subscribeQuery = "tm.event='Tx'"

// --- JSON-RPC Request/Notification Structs ---
type JSONRPCRequest struct {
	JSONRPC string                 `json:"jsonrpc"`
	Method  string                 `json:"method"`
	ID      string                 `json:"id"`
	Params  map[string]interface{} `json:"params"`
}

type NotificationValue struct {
	TxResult struct {
		Height string `json:"height"`
		Log    string `json:"log"` // Transaction log containing human-readable events (JSON string)
		Tx     string `json:"tx"`  // Base64-encoded transaction bytes
	} `json:"TxResult"`
}

type NotificationData struct {
	Type  string            `json:"type"`
	Value NotificationValue `json:"value"`
}

type SubscriptionResult struct {
	Query string           `json:"query"`
	Data  NotificationData `json:"data"`
}

type GenericNotification struct {
	JSONRPC string              `json:"jsonrpc"`
	ID      string              `json:"id,omitempty"`
	Result  *SubscriptionResult `json:"result,omitempty"`
	Method  string              `json:"method,omitempty"`
	Params  json.RawMessage     `json:"params,omitempty"`
}

// --- END JSON-RPC Structs ---

// TxEventPayload: Single transaction data to cache/save (Raw Data structure)
type TxEventPayload struct {
	Result      SubscriptionResult `json:"result"`
	CaptureTime string             `json:"capture_time"`
}

// BlockTxSummary: Raw transaction summary per block (for [BlockHeight]_raw.json)
type BlockTxSummary struct {
	BlockHeight  string           `json:"block_height"`
	TotalTxs     int              `json:"total_txs"`
	Transactions []TxEventPayload `json:"transactions"`
}

// --- NEW STRUCTS FOR DECODED OUTPUT (Log Parsing) ---

// Attribute: Log 이벤트 속성
type Attribute struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Event: 트랜잭션에서 발생한 이벤트
type Event struct {
	Type       string      `json:"type"`
	Attributes []Attribute `json:"attributes"`
}

// MsgLog: TxResult.Log 필드 내의 JSON 배열 요소
type MsgLog struct {
	MsgIndex int     `json:"msg_index"`
	Log      string  `json:"log"` // (Optional) Detailed log message for the message
	Events   []Event `json:"events"`
}

// DecodedTxData: Tx field를 Base64 디코딩하고 로그를 파싱한 결과를 포함
type DecodedTxData struct {
	Height string `json:"height"`
	// Technical fields
	RawBase64Tx  string `json:"raw_base64_tx"`  // 원본 Base64 문자열
	DecodedTxHex string `json:"decoded_tx_hex"` // Base64 디코딩 후 16진수 문자열로 변환한 트랜잭션 바이트

	// Human-readable fields (Parsed Events)
	RawLogString string   `json:"raw_log_string"` // 원본 Log JSON 문자열
	ParsedEvents []MsgLog `json:"parsed_events"`  // 파싱된 구조화된 이벤트
}

// DecodedBlockSummary: 디코딩된 트랜잭션 요약 (for [BlockHeight]_decoded_tx.json)
type DecodedBlockSummary struct {
	BlockHeight string          `json:"block_height"`
	TotalTxs    int             `json:"total_txs"`
	DecodedTxs  []DecodedTxData `json:"decoded_txs"`
}

// --- END NEW STRUCTS ---

var blockTxCache = make(map[string][]TxEventPayload)
var currentBlockHeight string
var cacheMutex sync.Mutex

// SaveRawBlockData: 원본 이벤트 데이터를 [BlockHeight]_raw.json으로 저장
func SaveRawBlockData(identifier string, txs []TxEventPayload) error {
	if len(txs) == 0 {
		return nil
	}

	log.Printf("⏳ [Raw Save Start]: Starting to save %d raw events for identifier '%s'...", len(txs), identifier)

	summary := BlockTxSummary{
		BlockHeight:  identifier,
		TotalTxs:     len(txs),
		Transactions: txs,
	}

	dataBytes, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal raw struct to JSON: %w", err)
	}

	fileName := fmt.Sprintf("./save/%s_raw.json", identifier)

	if err := os.WriteFile(fileName, dataBytes, 0644); err != nil {
		return fmt.Errorf("failed to write raw JSON file (%s): %w", fileName, err)
	}

	log.Printf("✅ [Raw Save Complete]: Raw event data for '%s' saved. (Path: %s)", identifier, fileName)
	return nil
}

// SaveDecodedTxData: Base64 Tx 필드를 디코딩하고 로그를 파싱하여 [BlockHeight]_decoded_tx.json으로 저장
func SaveDecodedTxData(identifier string, txs []TxEventPayload) error {
	if len(txs) == 0 {
		return nil
	}

	log.Printf("⏳ [Decode Save Start]: Starting to decode and parse %d transactions for identifier '%s'...", len(txs), identifier)

	decodedTxs := make([]DecodedTxData, 0, len(txs))

	for _, txPayload := range txs {
		// 트랜잭션 데이터 및 로그 가져오기
		base64Tx := txPayload.Result.Data.Value.TxResult.Tx
		rawLog := txPayload.Result.Data.Value.TxResult.Log // rawLog는 JSON 문자열입니다.

		// 1. Base64 Decode (for technical hex output)
		decodedBytes, err := base64.StdEncoding.DecodeString(base64Tx)
		if err != nil {
			log.Printf("⚠️ [Decode Error]: Failed to base64 decode Tx in Block %s. Error: %v", identifier, err)
			continue
		}
		decodedHex := fmt.Sprintf("%x", decodedBytes)

		// 2. Parse Log string (for human-readable output)
		var parsedLogs []MsgLog

		// rawLog가 비어있지 않은 경우에만 파싱 시도
		if len(rawLog) > 0 {
			// Tendermint/Cosmos SDK 로그는 JSON 배열 문자열입니다.
			err = json.Unmarshal([]byte(rawLog), &parsedLogs)
			if err != nil {
				// 파싱 오류 발생 시 (예: 로그가 비표준 형식일 경우)
				log.Printf("⚠️ [Log Parse Error]: Failed to parse log string for Block %s. Error: %v. Raw Log (First 100 chars): %s...", identifier, err, rawLog[:min(100, len(rawLog))])
				parsedLogs = []MsgLog{}
			}
		} else {
			// 로그 문자열 자체가 비어있는 경우 디버깅 메시지 할당
			rawLog = "Log not found or empty from TxResult"
			parsedLogs = []MsgLog{}
		}

		// 3. DecodedTxData에 모든 정보 추가
		decodedTxs = append(decodedTxs, DecodedTxData{
			Height:       txPayload.Result.Data.Value.TxResult.Height,
			RawBase64Tx:  base64Tx,
			DecodedTxHex: decodedHex,
			RawLogString: rawLog,     // 원본 로그 문자열 추가
			ParsedEvents: parsedLogs, // 파싱된 구조화된 이벤트 정보 추가
		})
	}

	if len(decodedTxs) == 0 {
		return nil
	}

	summary := DecodedBlockSummary{
		BlockHeight: identifier,
		TotalTxs:    len(decodedTxs),
		DecodedTxs:  decodedTxs,
	}

	dataBytes, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal decoded struct to JSON: %w", err)
	}

	fileName := fmt.Sprintf("./save/%s_decoded_tx.json", identifier)

	if err := os.WriteFile(fileName, dataBytes, 0644); err != nil {
		return fmt.Errorf("failed to write decoded JSON file (%s): %w", fileName, err)
	}

	log.Printf("✅ [Decode Save Complete]: Decoded Tx data for '%s' saved. (Path: %s)", identifier, fileName)
	return nil
}

// ProcessAndSaveBlock: 원본 및 디코딩된 파일 저장을 통합 관리
func ProcessAndSaveBlock(identifier string, txs []TxEventPayload) error {
	if len(txs) == 0 {
		return nil
	}

	saveDir := "./save"
	if _, err := os.Stat(saveDir); os.IsNotExist(err) {
		log.Printf("📁 [Folder Creation]: Creating save folder (%s) as it does not exist.", saveDir)
		if err := os.Mkdir(saveDir, 0755); err != nil {
			return fmt.Errorf("failed to create folder (%s): %w", saveDir, err)
		}
	}

	// 1. 원본 이벤트 데이터 저장
	if err := SaveRawBlockData(identifier, txs); err != nil {
		return fmt.Errorf("error saving raw block data: %w", err)
	}

	// 2. 디코딩된 트랜잭션 데이터 저장 (로그 파싱 포함)
	if err := SaveDecodedTxData(identifier, txs); err != nil {
		return fmt.Errorf("error saving decoded tx data: %w", err)
	}

	log.Printf("⭐ [Block Save Complete]: Block %s successfully saved to two files.", identifier)
	return nil
}

// min 함수: Go 1.18 미만 버전과의 호환성을 위해 추가
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func main() {
	log.Println("🚀 [프로그램 시작] Cosmos Hub WebSocket 클라이언트")

	ctx, cancel := context.WithCancel(context.Background())
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGINT)

	// rpcEndpoint를 wss://cosmos-rpc.polkachu.com로 다시 설정
	const rpcEndpoint = "wss://cosmos-rpc.polkachu.com"
	url := rpcEndpoint + wsPath
	log.Printf("🌐 [연결 시도]: WebSocket 엔드포인트에 연결 시도: %s", url)

	requestHeaders := map[string][]string{}

	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}

	conn, _, err := dialer.Dial(url, requestHeaders)
	if err != nil {
		log.Fatalf("❌ [연결 실패]: WebSocket 연결 실패: %v", err)
	}

	log.Println("✅ [연결 성공]: WebSocket 연결이 성공적으로 수립되었습니다.")

	// Go routine for SIGINT handling (final save on exit)
	go func() {
		<-sigChan
		log.Println("\n🛑 [종료 신호 감지] Ctrl+C 신호를 수신했습니다.")

		if err := conn.Close(); err != nil {
			log.Printf("🚨 연결 종료 오류: %v", err)
		}
		log.Println("🌐 [연결 종료]: WebSocket 연결 강제 종료가 트리거되었습니다.")
		time.Sleep(100 * time.Millisecond)

		log.Println("🧹 [최종 정리]: 캐시 데이터 저장을 시작합니다...")

		cacheMutex.Lock()
		defer cacheMutex.Unlock()

		if len(blockTxCache) > 0 {
			log.Printf("🧹 [최종 정리]: 종료 전 캐시된 모든 블록 (%d개) 데이터 저장을 시도합니다...", len(blockTxCache))

			for height, txs := range blockTxCache {
				if err := ProcessAndSaveBlock(height, txs); err != nil {
					log.Printf("🚨 최종 저장 오류 (블록 %s): %v", height, err)
				}
				delete(blockTxCache, height)
			}
			log.Printf("✅ [최종 정리]: 모든 캐시된 블록 데이터가 저장되었습니다.")
		} else {
			log.Println("🧹 [최종 정리]: 저장할 캐시 데이터가 없습니다.")
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
		log.Fatalf("❌ [요청 오류]: 요청 JSON 마샬링 실패: %v", err)
	}

	log.Printf("📤 [구독 요청]: 이벤트 구독 요청 전송. Query: %s, ID: %s", subscribeQuery, requestID)
	if err := conn.WriteMessage(websocket.TextMessage, requestBytes); err != nil {
		log.Fatalf("❌ [전송 실패]: 구독 요청 전송 실패: %v", err)
	}

	log.Println("👂 [대기 중]: 실시간 트랜잭션 이벤트를 기다리는 중... (Ctrl+C로 종료)")

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
				log.Printf("⚠️ [파싱 오류-1단계]: 일반 메시지 파싱 실패. 전체 메시지: %s", string(message))
				continue
			}

			// Debug log output
			isParamsExist := generic.Params != nil && len(generic.Params) > 0
			isResultExist := generic.Result != nil && generic.Result.Data.Type != ""
			log.Printf("🔍 [RX 메시지 디버그]: ID: '%s', Method: '%s', Params 존재: %t, Result 존재: %t",
				generic.ID, generic.Method, isParamsExist, isResultExist)

			// Check if this is a standard RPC Success response with no event data (initial confirmation)
			if generic.ID == requestID && !isResultExist {
				log.Printf("➡️ [구독 성공 확인]: ID '%s'에 대한 초기 구독 확인 응답을 수신했습니다.", generic.ID)

				if !subscriptionResponseReceived {
					subscriptionResponseReceived = true
					log.Println("⏳ [대기 중]: 구독 응답 수신 후 저장 시작 전 2초간 트랜잭션 수집을 기다리는 중...")
					time.Sleep(2 * time.Second)

					// Check and save cache upon subscription response (async)
					cacheMutex.Lock()
					if len(blockTxCache) > 0 {
						for height, txs := range blockTxCache {
							txsCopy := make([]TxEventPayload, len(txs))
							copy(txsCopy, txs)

							log.Printf("✨ [구독 응답 저장]: 캐시된 블록 %s 데이터 (%d개 항목) 비동기 저장을 시도합니다.", height, len(txsCopy))
							go func(h string, t []TxEventPayload) {
								if err := ProcessAndSaveBlock(h, t); err != nil {
									log.Printf("🚨 비동기 저장 오류 (블록 %s): %v", h, err)
								}
							}(height, txsCopy)
						}
						log.Printf("💾 [구독 응답 저장]: 캐시된 데이터의 비동기 저장이 완료되었습니다.")
					} else {
						log.Println("⚠️ [캐시 경고]: 구독 응답 시점에 저장할 트랜잭션 데이터가 캐시에 없습니다.")
					}
					cacheMutex.Unlock()
				}
				continue
			}

			// EVENT MESSAGE HANDLING
			if !isResultExist {
				continue
			}

			txResult := generic.Result.Data.Value.TxResult
			newHeight := txResult.Height

			if newHeight == "" {
				log.Printf("⚠️ [이벤트 건너뛰기]: 이벤트(결과 존재)이지만, 트랜잭션 높이(Height) 정보가 누락되었습니다.")
				continue
			}

			// ------------------------------------------------------------------
			// ⭐️ 블록 전환 시 이전 블록 데이터 즉시 저장 로직 (비동기)
			// ------------------------------------------------------------------
			cacheMutex.Lock()

			if currentBlockHeight != "" && newHeight != currentBlockHeight {
				oldHeight := currentBlockHeight

				if txsToSave, exists := blockTxCache[oldHeight]; exists {
					txsCopy := make([]TxEventPayload, len(txsToSave))
					copy(txsCopy, txsToSave)

					delete(blockTxCache, oldHeight)

					log.Printf("📈 [블록 전환 감지]: 블록 %s에서 블록 %s로 전환 중. 이전 블록 (%s, 트랜잭션 수: %d)의 비동기 저장을 시작합니다.", oldHeight, newHeight, oldHeight, len(txsCopy))

					// FIX: 'txs' -> 'txsCopy'로 변수명 수정
					go func(h string, txs []TxEventPayload) {
						if err := ProcessAndSaveBlock(h, txs); err != nil {
							log.Printf("🚨 비동기 저장 오류 (블록 %s): %v", h, err)
						}
					}(oldHeight, txsCopy) // 수정된 부분
				}
			}

			// ------------------------------------------------------------------

			// 1. Update current block height
			currentBlockHeight = newHeight

			// 2. Add current transaction data to the cache in struct format
			dataToCache := TxEventPayload{
				Result:      *generic.Result,
				CaptureTime: time.Now().Format(time.RFC3339),
			}

			// Debug log output
			log.Printf("--------------------------------------------------")
			log.Printf("🔔 [트랜잭션 이벤트]: 블록 %s에 대한 트랜잭션 수신 (Base64 트랜잭션 길이: %d, 로그 길이: %d)",
				newHeight, len(txResult.Tx), len(txResult.Log))
			log.Printf("--------------------------------------------------")

			blockTxCache[newHeight] = append(blockTxCache[newHeight], dataToCache)
			log.Printf("✅ [트랜잭션 캐시]: 블록 %s에 트랜잭션 저장. (현재 캐시된 트랜잭션 수: %d)",
				newHeight, len(blockTxCache[newHeight]))

			var totalCachedTxs int
			for _, txs := range blockTxCache {
				totalCachedTxs += len(txs)
			}
			log.Printf("📊 [캐시 상태]: 총 %d개의 트랜잭션이 %d개 블록에 캐시되었습니다. (현재 높이: %s)", totalCachedTxs, len(blockTxCache), currentBlockHeight)

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
				log.Println("🛑 [정상 종료]: 예상된 연결 종료 오류가 발생했습니다. 프로그램을 종료합니다.")
			} else {
				log.Printf("❌ [오류 종료]: 프로그램 오류가 발생했습니다: %v", err)
			}
		} else {
			log.Println("🛑 [정상 종료]: 프로그램 종료 신호를 수신했습니다. 종료 완료.")
		}
	case <-ctx.Done():
		log.Println("🛑 [정상 종료]: Context 취소로 인해 프로그램 종료 신호를 수신했습니다.")
	}

	cancel()
	log.Println("👋 [프로그램 종료]")
}
*/
