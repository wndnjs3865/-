// === 계좌매수매도 워크플로우 수정 사항 ===
// workflowId: kS9R6bUBUYMCiGAT
//
// 문제 1: 해외주식 매수 주문 노드의 authorization 헤더
//   현재: "==Bearer {{$json.access_token}}"  (= 이 두 개)
//   수정: "=Bearer {{$json.access_token}}"   (= 하나로)
//
// 문제 2: HTTP Request (잔고 조회) 노드의 tr_id
//   현재: "TTTC8434R" (국내주식 잔고 조회용)
//   수정: 해외주식 잔고 조회용 tr_id로 변경 필요 (예: JTTT3012R)
//   URL: /uapi/overseas-stock/v1/trading/inquire-balance 와 일치하는 tr_id 사용
//
// 문제 3: HTTP Request (잔고 조회) 출력 연결 없음
//   현재: main: [[]] (빈 연결)
//   수정: 필요 시 잔고 확인 결과를 활용하는 노드 연결 추가
