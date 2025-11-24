package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// wsPath, rpcEndpoint, JSONRPCRequest 구조체는 main.go에 이미 정의되어 있으므로 여기서는 재정의하지 않고 사용합니다.

// JSON-RPC 이벤트 알림 구조체 (수신된 Tx 이벤트 파싱용)
type JSONRPCNotification struct {
	JSONRPC string `json:"jsonrpc"`
	Method  string `json:"method"`
	Params  struct {
		Query string `json:"query"`
		Data  struct {
			Type  string `json:"type"`
			Value struct {
				TxResult struct {
					Height string `json:"height"` // 블록 높이
					Log    string `json:"log"`    // 트랜잭션 로그 (JSON string)
					Tx     string `json:"tx"`     // 실제 트랜잭션 데이터 (Base64 인코딩)
				} `json:"TxResult"`
			} `json:"value"`
		} `json:"data"`
	} `json:"params"`
}

// Tendermint Event 구조체 정의 (로그 파싱용)
type Attribute struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type Event struct {
	Type       string      `json:"type"`
	Attributes []Attribute `json:"attributes"`
}

type ABCIMessageLog struct {
	MsgIndex uint32  `json:"msg_index"`
	Log      string  `json:"log"`
	Events   []Event `json:"events"`
}

// 트랜잭션 카테고리 정의
type TxCategory string

const (
	CategoryBank    TxCategory = "BankTransfer"
	CategoryGov     TxCategory = "GovernanceProposal"
	CategoryStaking TxCategory = "StakingAction"
	CategoryOtherTx TxCategory = "OtherTxType"
	CategoryUnknown TxCategory = "Unknown" // JSON 파싱 실패 등 오류
)

// TestResult: 단일 트랜잭션 분석 결과
type TestResult struct {
	Height   string `json:"block_height"`   // 블록 높이
	TxIndex  int    `json:"tx_index"`       // 전체 리스트 내의 인덱스
	TxData   string `json:"tx_data_base64"` // Base64 인코딩된 트랜잭션 데이터
	TxLog    string `json:"tx_log_json"`
	Actual   string `json:"actual_category"`
	Status   string `json:"status"` // "PASS" 또는 "FAIL"
	ErrorMsg string `json:"error_message,omitempty"`
}

// StreamTestResult: 최종 결과 구조체
type StreamTestResult struct {
	CaptureType     string       `json:"capture_type"` // 캡처 방식 정보
	CaptureTime     string       `json:"capture_time"`
	BlockCount      int          `json:"block_count"`      // 캡처된 고유 블록 수
	TotalTxCount    int          `json:"total_tx_count"`   // 총 트랜잭션 수
	AllTransactions []TestResult `json:"all_transactions"` // 모든 트랜잭션 데이터
}

// 트랜잭션 이벤트 분석 로직 (Tx Log 파싱)
func ParseTxEvents(txLogJSON string) TxCategory {
	var logs []ABCIMessageLog

	if txLogJSON == "" || txLogJSON == "null" {
		return CategoryOtherTx
	}

	err := json.Unmarshal([]byte(txLogJSON), &logs)
	if err != nil {
		return CategoryUnknown
	}

	for _, logEntry := range logs {
		for _, event := range logEntry.Events {
			if event.Type == "transfer" {
				return CategoryBank
			}
			if event.Type == "submit_proposal" {
				return CategoryGov
			}
			if event.Type == "delegate" || event.Type == "undelegate" || event.Type == "create_validator" {
				return CategoryStaking
			}
			if event.Type == "message" {
				for _, attr := range event.Attributes {
					if attr.Key == "module" {
						if attr.Value == "bank" {
							return CategoryBank
						}
						if attr.Value == "gov" {
							return CategoryGov
						}
						if attr.Value == "staking" {
							return CategoryStaking
						}
					}
				}
			}
		}
	}

	return CategoryOtherTx
}

// 실시간 트랜잭션을 구독하고 분석하는 테스트 로직 (타임아웃 시까지 무제한 캡처)
func TestRealtimeTxAnalysis(t *testing.T) {

	// Context 및 Timeout 설정 (60초 동안 스트림)
	const CaptureTimeout = 60 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), CaptureTimeout)
	defer cancel()

	// main.go에 정의된 rpcEndpoint와 wsPath 사용
	url := rpcEndpoint + wsPath
	t.Logf("WebSocket 연결 시도: %s", url)

	// WebSocket 연결
	dialer := websocket.Dialer{
		HandshakeTimeout: 5 * time.Second,
	}
	conn, _, err := dialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("🚨 WebSocket 연결 실패: %v", err)
	}
	defer func() {
		conn.Close()
		t.Log("WebSocket 연결 종료.")
	}()

	t.Log("WebSocket 연결 성공. 구독 요청 전송.")

	// 구독 쿼리: 실시간 모든 트랜잭션 이벤트 구독
	realtimeTxQuery := "tm.event='Tx'"

	// Subscribe 요청 전송
	subscribeRequest := JSONRPCRequest{
		JSONRPC: "2.0",
		Method:  "subscribe",
		ID:      "1",
		Params: map[string]interface{}{
			"query": realtimeTxQuery,
		},
	}
	requestBytes, _ := json.Marshal(subscribeRequest)
	if err := conn.WriteMessage(websocket.TextMessage, requestBytes); err != nil {
		t.Fatalf("🚨 구독 요청 전송 실패: %v", err)
	}

	// 초기 응답 수신 대기 (구독 성공 확인)
	if _, _, err := conn.ReadMessage(); err != nil {
		t.Logf("경고: 초기 구독 응답 수신 실패/타임아웃: %v", err)
	}

	t.Logf("실시간 트랜잭션 이벤트 수신 대기 시작 (최대 %s 동안 캡처)", CaptureTimeout)

	// Context 취소 시 연결을 닫아 ReadMessage를 unblock 하는 고루틴
	go func() {
		<-ctx.Done()
		conn.Close()
	}()

	// 캡처된 트랜잭션들을 담을 평면 리스트
	var allTxs []TestResult
	// 캡처된 고유 블록 높이를 추적하기 위한 맵 (통계를 위해 유지)
	capturedBlocks := make(map[string]bool)

	// 메시지 수신 및 파싱 루프 (타임아웃까지 무제한 실행)
	for {
		_, message, err := conn.ReadMessage()

		if err != nil {
			// 타임아웃 또는 정상 종료 시 루프 종료
			if ctx.Err() != nil || websocket.IsCloseError(err, websocket.CloseNormalClosure) {
				t.Logf("스트림 종료: %v", ctx.Err())
				break
			}

			// 예기치 않은 오류 로깅
			if websocket.IsUnexpectedCloseError(err) {
				t.Logf("🚨 오류: 메시지 읽기 중 예기치 않은 오류 발생: %v (루프 종료)", err)
			}
			break
		}

		// JSON-RPC 알림으로 파싱
		var notification JSONRPCNotification
		if err := json.Unmarshal(message, &notification); err != nil {
			continue // 파싱 실패 시 스킵
		}

		txResult := notification.Params.Data.Value.TxResult

		// Tx 이벤트 알림이 아니면 스킵
		if notification.Method != "event" || notification.Params.Data.Type != "tendermint/event/Tx" || txResult.Height == "" {
			continue
		}

		txLog := txResult.Log
		txData := txResult.Tx
		txHeight := txResult.Height

		// 데이터가 없는 트랜잭션은 스킵
		if txLog == "" && txData == "" {
			continue
		}

		// 고유 블록 높이 추적
		capturedBlocks[txHeight] = true

		// 트랜잭션 분석 및 결과 저장
		result := TestResult{
			Height:  txHeight,
			TxIndex: len(allTxs),
			TxData:  txData,
			TxLog:   txLog,
			Actual:  string(ParseTxEvents(txLog)),
			Status:  "PASS",
		}
		allTxs = append(allTxs, result)

		// 실시간으로 캡처 정보를 간략히 출력 (테스트 진행 상황)
		t.Logf("Captured: Height=%s, Total Txs=%d", txHeight, len(allTxs))
	}

	// 최종 분석 결과 요약
	streamResult := StreamTestResult{
		CaptureType:     "Real-Time Stream (Time-limited to 60s, Flat List)",
		CaptureTime:     time.Now().Format(time.RFC3339),
		BlockCount:      len(capturedBlocks),
		TotalTxCount:    len(allTxs),
		AllTransactions: allTxs,
	}

	// 최종 결과를 JSON으로 마샬링
	finalDataJSON, err := json.MarshalIndent(streamResult, "", "  ")
	if err != nil {
		t.Fatalf("🚨 최종 결과 JSON 마샬링 실패: %v", err)
	}

	// 최종 결과만 테스트 로그(표준 출력)에 출력
	t.Log("\n--- 최종 스트림 분석 결과 (JSON 출력) ---")
	t.Log(string(finalDataJSON))
	t.Log("-------------------------------------------\n")

	t.Logf("✅ 테스트 완료. 총 트랜잭션 수: %d, 고유 블록 수: %d",
		streamResult.TotalTxCount, streamResult.BlockCount)
}
