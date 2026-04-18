# Termux × Claude Code 설치

모바일(Android) Termux 환경에서 Claude Code 를 설치·연동하기 위한 스크립트입니다.

## 빠른 시작

```bash
pkg install -y git
git clone https://github.com/wndnjs3865/-.git claude-termux
cd claude-termux
bash setup-termux.sh
```

## 단계별 흐름 (주 경로 → 차선책)

| 단계 | 주 경로 | 차선책 |
| ---- | ------- | ------ |
| 저장소 | `pkg update/upgrade` | `termux-change-repo` 로 미러 변경 후 재시도 |
| Node  | `pkg install nodejs-lts` | `pkg install nodejs` |
| Claude 설치 | `npm i -g @anthropic-ai/claude-code` | `~/.npm-global` 사용자 prefix 재설치 → `npx` 래퍼 |
| 인증 | OAuth (브라우저) | `ANTHROPIC_API_KEY` 환경변수 |
| 세션 | 포그라운드 | `tmux` 백그라운드 세션 |

## 전제 조건

- **Termux 는 F-Droid 버전 권장** (Play Store 버전은 업데이트 중단)
- Android 7.0+
- 저장공간 2GB 이상
- Node.js 18+ (스크립트가 자동 설치)

## 인증 방법

### OAuth (기본)
```bash
claude   # 최초 실행 → 브라우저가 열림
```

### API 키 (폴백)
Termux 에서 브라우저 연동이 안 되는 경우:
```bash
export ANTHROPIC_API_KEY="sk-ant-..."
echo 'export ANTHROPIC_API_KEY="sk-ant-..."' >> ~/.bashrc
claude
```

## 모바일 편의 설정

- **Hacker's Keyboard** 설치 → ESC/Ctrl/Tab 입력
- **Termux:Style** 로 폰트·테마
- **tmux** 로 세션 유지 (앱 백그라운드 전환 대비)

## 문제 해결

| 증상 | 해결 |
| ---- | ---- |
| `claude: command not found` | `exec bash` 로 쉘 재로드 |
| `EACCES` npm 오류 | 스크립트가 `~/.npm-global` 로 자동 전환 |
| 패키지 404 | `termux-change-repo` 실행 |
| OAuth 콜백 실패 | API 키 방식으로 전환 |
| Node 버전 낮음 | `pkg upgrade nodejs-lts` |

## 관련 브랜치

- 개발 브랜치: `claude/setup-claude-termux-8IZ1C`
