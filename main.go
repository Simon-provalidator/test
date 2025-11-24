package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

// WebSocket 엔드포인트 경로 (필요시 /websocket으로 변경 가능)
const wsPath = "/websocket"

//const wsPath = "/subscribe"

// RPC 엔드포인트
// const rpcEndpoint = "wss://rpc-atomone.nodeist.net"
const rpcEndpoint = "wss://cosmoshub-rpc.0base.dev"

// 구독할 이벤트 쿼리 (tm.event='NewBlock' 또는 tm.event='Tx'로 변경 가능)
// const subscribeQuery = "tm.event='NewBlock'"
const subscribeQuery = "tm.event='Tx'"

// JSON-RPC 요청 구조체
type JSONRPCRequest struct {
	JSONRPC string                 `json:"jsonrpc"`
	Method  string                 `json:"method"`
	ID      string                 `json:"id"`
	Params  map[string]interface{} `json:"params"`
}

func main() {
	// Context와 Cancel 함수 생성 (SIGINT 처리용)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// SIGINT (Ctrl+C) 신호 처리
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGINT)

	// 별도 고루틴에서 SIGINT 신호를 받으면 context를 취소
	go func() {
		<-sigChan
		log.Println("\nSIGINT 신호를 받았습니다. 연결을 종료합니다...")
		cancel()
	}()

	// WebSocket 연결
	url := rpcEndpoint + wsPath
	log.Printf("WebSocket 연결 시도: %s", url)

	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}

	conn, _, err := dialer.Dial(url, nil)
	if err != nil {
		log.Fatalf("WebSocket 연결 실패: %v", err)
	}
	defer func() {
		log.Println("WebSocket 연결 종료 중...")
		// context 취소 후에 conn.Close()를 호출하여 읽기/쓰기 고루틴이 종료되도록 합니다.
		conn.Close()
	}()

	log.Println("WebSocket 연결 성공!")

	// Subscribe 요청 전송
	subscribeRequest := JSONRPCRequest{
		JSONRPC: "2.0",
		Method:  "subscribe",
		ID:      "1",
		Params: map[string]interface{}{
			"query": subscribeQuery,
		},
	}

	requestBytes, err := json.Marshal(subscribeRequest)
	if err != nil {
		log.Fatalf("요청 JSON 마샬링 실패: %v", err)
	}

	log.Printf("구독 요청 전송: %s", string(requestBytes))
	if err := conn.WriteMessage(websocket.TextMessage, requestBytes); err != nil {
		log.Fatalf("구독 요청 전송 실패: %v", err)
	}

	log.Printf("이벤트 구독 시작: %s", subscribeQuery)
	log.Println("이벤트를 기다리는 중... (Ctrl+C로 종료)")

	// Context 취소 시 연결을 닫아 ReadMessage를 unblock 하는 고루틴
	go func() {
		<-ctx.Done()
		// conn.Close()는 defer에서 호출되지만,
		// ReadMessage를 즉시 중단시키기 위해 CloseMessage를 보낼 수 있습니다.
		// conn.Close()만으로도 충분히 ReadMessage가 에러를 반환하고 종료되므로, 여기서는 context의 종료를 대기합니다.
	}()

	// 메시지 수신 루프
	done := make(chan error, 1)

	go func() {
		for {
			// 메시지 읽기
			_, message, err := conn.ReadMessage()
			if err != nil {
				// Context 취소로 인한 종료인지 확인
				if websocket.IsCloseError(err, websocket.CloseNormalClosure) {
					done <- nil // 정상 종료
					return
				}

				select {
				case <-ctx.Done():
					// conn.Close()가 호출되어 여기서 에러가 발생한 경우
					done <- nil
					return
				default:
				}

				// 예상치 못한 종료 에러
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					done <- err
					return
				}

				// 기타 읽기 에러
				done <- err
				return
			}

			// 받은 메시지를 콘솔에 출력
			log.Println(string(message))
		}
	}()

	// 종료 대기
	select {
	case err := <-done:
		if err != nil && err != context.Canceled {
			log.Printf("오류 발생: %v", err)
		}
	case <-ctx.Done():
		log.Println("프로그램 종료 신호 수신...")
	}

	log.Println("프로그램이 종료되었습니다.")
}
