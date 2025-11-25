🚀 Cosmos Hub WebSocket 트랜잭션 수집 클라이언트 (Go)

이 프로젝트는 Go 언어로 작성된 간단한 WebSocket 클라이언트입니다. Cosmos Hub (Atom)의 Tendermint RPC 노드에 연결하여 실시간 트랜잭션 이벤트를 구독하고, 수신된 데이터를 블록 높이별로 분류하여 JSON 파일로 저장합니다.

✨ 주요 기능

실시간 구독: 안정적인 공용 Tendermint RPC 엔드포인트에 WebSocket으로 연결하여 tm.event='Tx' 쿼리를 구독합니다.

데이터 파싱: Cosmos/Tendermint 이벤트 메시지(Result 필드에 데이터 포함)를 분석하여 트랜잭션 데이터를 추출합니다.

블록 단위 캐싱: 동일한 블록 높이의 트랜잭션을 메모리에 임시로 캐시합니다.

블록 전환 감지 및 저장: 새로운 블록 높이가 감지되면 이전 블록의 모든 트랜잭션을 ./save 폴더에 JSON 파일로 자동 저장합니다.

프로그램 종료 시 최종 저장: Ctrl+C 또는 SIGINT 신호 수신 시 캐시에 남아있는 모든 데이터를 안전하게 저장합니다.

🛠️ 개발 환경 설정

전제 조건 (Prerequisites)

Go (Golang) (버전 1.18 이상 권장)

Git (소스 코드 관리를 위해)

프로젝트 실행

저장소 클론:

git clone [https://github.com/당신의_아이디/cosmos-ws-client.git](https://github.com/당신의_아이디/cosmos-ws-client.git)
cd cosmos-ws-client


종속성 다운로드:
Gorilla WebSocket 라이브러리가 필요합니다.

go mod init cosmos-ws-client  # 프로젝트 모듈 초기화 (이미 되어 있다면 생략)
go get [github.com/gorilla/websocket](https://github.com/gorilla/websocket)


클라이언트 실행:

go run main.go


프로그램이 성공적으로 연결되면 실시간 트랜잭션을 수신 대기합니다.

📁 결과물 및 폴더 구조

클라이언트가 트랜잭션을 수집하고 블록 전환을 감지하면, 프로젝트 루트 디렉토리에 save 폴더가 생성되고, 그 안에 JSON 파일이 저장됩니다.

cosmos-ws-client/
├── main.go               # 메인 Go 클라이언트 코드
├── go.mod                # Go 모듈 파일
├── go.sum                # Go 모듈 체크섬 파일
├── .gitignore            # Git이 무시할 파일 정의
└── save/                 # ✨ 트랜잭션 데이터 저장 폴더
    ├── [BlockHeight].json  # 예: 28572307.json
    └── [BlockHeight].json


JSON 출력 예시 ([BlockHeight].json)

저장되는 JSON 파일은 다음과 같은 구조를 가집니다.

{
  "block_height": "28572307",
  "total_txs": 3,
  "transactions": [
    {
      "query": "tm.event='Tx'",
      "data": {
        "type": "tendermint/event/Tx",
        "value": {
          "TxResult": {
            "height": "28572307",
            "log": "[{\"msg_index\":0,\"events\":[...]}",
            "tx": "CoqiAQqIogEKIy9pYmMuY29yZS5jbGllbnQudjEuTXNnV..."
          }
        }
      },
      "capture_time": "2025-11-25T20:00:00+09:00"
    }
    // ... 나머지 트랜잭션
  ]
}


⚙️ 설정 및 맞춤화

main.go 파일 상단에 정의된 상수를 수정하여 클라이언트의 동작을 변경할 수 있습니다.

RPC 엔드포인트 변경

현재 wss://cosmos-rpc.polkachu.com/websocket을 사용하고 있습니다. 다른 Tendermint RPC 노드를 사용하려면 rpcEndpoint 상수를 변경하세요.

// main.go 파일 내
const rpcEndpoint = "wss://cosmos-rpc.polkachu.com" 
const wsPath = "/websocket" // 대부분의 노드에서 이 경로는 그대로 유지됩니다.


구독 쿼리 변경

특정 이벤트만 구독하려면 subscribeQuery 상수를 수정하세요.

// 모든 트랜잭션 이벤트 구독 (기본 설정)
const subscribeQuery = "tm.event='Tx'"

// 특정 모듈의 메시지 유형만 구독 (예: Bank 모듈의 송금 메시지)
// const subscribeQuery = "message.action='/cosmos.bank.v1beta1.MsgSend'"
