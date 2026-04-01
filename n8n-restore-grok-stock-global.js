// === grok-stock-global 워크플로우 복원 코드 ===
// SDK 문법: validate_workflow → update_workflow (workflowId: mOyS48zO8gtBKzuw)

const manualTriggerNode = trigger({ type: "n8n-nodes-base.manualTrigger", version: 1, config: {} });

const grokModel = node({
  type: "@n8n/n8n-nodes-langchain.lmChatXAiGrok",
  version: 1,
  config: {
    parameters: {
      model: "grok-4-1-fast-reasoning",
      options: {}
    }
  }
});

const llmChain = node({
  type: "@n8n/n8n-nodes-langchain.chainLlm",
  version: 1.9,
  config: {
    parameters: {
      promptType: "define",
      text: "오늘 미국과 글로벌 주식 시장 상황을 간단히 분석해줘.\n\n주요 지수: S&P 500, Nasdaq, Dow Jones\n필요 시 VIX, 미국 10년물 국채 금리, 달러 인덱스(DXY)도 고려해서\n\n1. 단기 시장 방향성\n2. 주요 리스크 요인\n3. 보수적인 투자 관점에서의 조언\n\n답변은 객관적이고 간결하게 해줘.",
      messages: {
        messageValues: [
          {
            message: "너는 글로벌 주식 시장 전문 리스크 관리자다.  항상 안전하고 보수적인 관점에서 판단하며, 투자자에게 과도한 리스크를 피하도록 조언한다. 답변은 객관적이고 명확하며 간결하게 작성한다."
          }
        ]
      },
      batching: {}
    },
    subnodes: {
      model: grokModel
    }
  }
});

const telegramNode = node({
  type: "n8n-nodes-base.telegram",
  version: 1.2,
  config: {
    parameters: {
      resource: "message",
      operation: "sendMessage",
      chatId: "",
      text: "=📊 글로벌 주식 시장 분석 리포트\n\n{{ $json.text }}",
      additionalFields: {
        parse_mode: "Markdown",
        disable_web_page_preview: true,
        appendAttribution: false
      }
    }
  }
});

manualTriggerNode.to(llmChain).to(telegramNode);

workflow("grok-stock-global", "grok-stock-global")
  .add(manualTriggerNode);
