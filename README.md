# Cosmos Hub RPC WebSocket 구독 프로그램

Cosmos Hub RPC의 이벤트를 WebSocket으로 구독하여 콘솔에 출력하는 간단한 Go 프로그램입니다.

## 실행 방법

### 1. Go 모듈 초기화

```bash
go mod init example.com/cosmos-subscribe
```

### 2. 필요한 의존성 설치

```bash
go get github.com/gorilla/websocket
```

또는

```bash
go mod tidy
```

### 3. 프로그램 실행

```bash
go run main.go
```

## 설정 변경

프로그램 상단의 상수들을 수정하여 설정을 변경할 수 있습니다:

- `wsPath`: WebSocket 경로 (기본값: `/subscribe`, 필요시 `/websocket`으로 변경)
- `rpcEndpoint`: RPC 엔드포인트 (기본값: `wss://cosmoshub-rpc.0base.dev`)
- `subscribeQuery`: 구독할 이벤트 쿼리
  - `tm.event='NewBlock'`: 새 블록 이벤트
  - `tm.event='Tx'`: 트랜잭션 이벤트

## 종료 방법

프로그램을 종료하려면 `Ctrl+C`를 누르세요. 프로그램이 WebSocket 연결을 깔끔하게 종료합니다.

## 예시 출력

```
2024/01/01 12:00:00 WebSocket 연결 시도: wss://cosmoshub-rpc.0base.dev/subscribe
2024/01/01 12:00:01 WebSocket 연결 성공!
2024/01/01 12:00:01 구독 요청 전송: {"jsonrpc":"2.0","method":"subscribe","id":"1","params":{"query":"tm.event='NewBlock'"}}
2024/01/01 12:00:01 이벤트 구독 시작: tm.event='NewBlock'
2024/01/01 12:00:01 이벤트를 기다리는 중... (Ctrl+C로 종료)
2024/01/01 12:00:02 {"jsonrpc":"2.0","id":"1","result":{}}
2024/01/01 12:00:05 {"jsonrpc":"2.0","method":"tm_event","params":{"result":{"query":"tm.event='NewBlock'","data":{"type":"tendermint/event/NewBlock","value":{...}}}}}
...
```

