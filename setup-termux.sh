#!/data/data/com.termux/files/usr/bin/env bash
# Termux + Claude Code 설치 스크립트
# 주 경로가 실패하면 차선책으로 자동 전환됩니다.

set -u

log()  { printf '\033[1;34m[INFO]\033[0m  %s\n' "$*"; }
warn() { printf '\033[1;33m[WARN]\033[0m  %s\n' "$*"; }
err()  { printf '\033[1;31m[ERR ]\033[0m  %s\n' "$*" >&2; }
ok()   { printf '\033[1;32m[ OK ]\033[0m  %s\n' "$*"; }

need_termux() {
  if [ -z "${PREFIX:-}" ] || [ ! -d "$PREFIX" ]; then
    err "Termux 환경이 아닙니다. Termux에서 실행하세요."
    exit 1
  fi
}

step_update() {
  log "pkg 저장소 업데이트"
  yes | pkg update -y && yes | pkg upgrade -y && return 0
  warn "기본 미러 실패 → termux-change-repo 로 미러 변경 후 재시도"
  if command -v termux-change-repo >/dev/null 2>&1; then
    termux-change-repo || true
  fi
  yes | pkg update -y && yes | pkg upgrade -y
}

step_deps() {
  log "필수 패키지 설치 (nodejs-lts, git, openssh, tmux, curl)"
  if ! pkg install -y nodejs-lts git openssh tmux curl; then
    warn "nodejs-lts 실패 → nodejs 로 재시도"
    pkg install -y nodejs git openssh tmux curl || {
      err "Node.js 설치 실패. 네트워크/미러 확인 필요."
      return 1
    }
  fi
  ok "Node $(node -v) / npm $(npm -v)"
}

step_npm_prefix() {
  # 전역 설치 권한 문제 회피용 사용자 prefix
  local pfx="$HOME/.npm-global"
  mkdir -p "$pfx"
  npm config set prefix "$pfx"
  case ":$PATH:" in
    *":$pfx/bin:"*) ;;
    *)
      if ! grep -q 'npm-global/bin' "$HOME/.bashrc" 2>/dev/null; then
        printf '\nexport PATH="$HOME/.npm-global/bin:$PATH"\n' >> "$HOME/.bashrc"
      fi
      export PATH="$pfx/bin:$PATH"
      ;;
  esac
}

step_claude() {
  log "Claude Code 설치 (@anthropic-ai/claude-code)"
  if npm install -g @anthropic-ai/claude-code; then
    ok "npm 전역 설치 성공"
    return 0
  fi
  warn "전역 설치 실패 → 사용자 prefix 로 재시도"
  step_npm_prefix
  if npm install -g @anthropic-ai/claude-code; then
    ok "사용자 prefix 설치 성공 ($HOME/.npm-global/bin)"
    return 0
  fi
  warn "npm 경로 실패 → npx 폴백 래퍼 생성"
  mkdir -p "$HOME/.local/bin"
  cat > "$HOME/.local/bin/claude" <<'WRAP'
#!/data/data/com.termux/files/usr/bin/env bash
exec npx -y @anthropic-ai/claude-code "$@"
WRAP
  chmod +x "$HOME/.local/bin/claude"
  case ":$PATH:" in
    *":$HOME/.local/bin:"*) ;;
    *)
      if ! grep -q '.local/bin' "$HOME/.bashrc" 2>/dev/null; then
        printf '\nexport PATH="$HOME/.local/bin:$PATH"\n' >> "$HOME/.bashrc"
      fi
      export PATH="$HOME/.local/bin:$PATH"
      ;;
  esac
  ok "npx 폴백 래퍼 생성됨"
}

step_storage() {
  log "스토리지 접근 권한 요청 (선택)"
  if command -v termux-setup-storage >/dev/null 2>&1; then
    termux-setup-storage || warn "권한 요청 스킵됨"
  fi
}

step_verify() {
  log "설치 검증"
  if command -v claude >/dev/null 2>&1; then
    claude --version 2>/dev/null || claude -v 2>/dev/null || true
    ok "claude 커맨드 사용 가능"
  else
    err "claude 커맨드를 찾을 수 없음. 새 쉘 세션에서 다시 시도하세요: exec bash"
    return 1
  fi
}

main() {
  need_termux
  step_update      || { err "업데이트 단계 실패"; exit 1; }
  step_deps        || { err "의존성 설치 실패"; exit 1; }
  step_storage
  step_claude      || { err "Claude Code 설치 실패"; exit 1; }
  step_verify      || exit 1

  cat <<'DONE'

======================================================
 설치 완료.  다음 단계:
   1) 새 쉘 로드:   exec bash
   2) 인증 실행:    claude  (최초 실행 시 OAuth/API 키)
      - 브라우저 연동 불가 시 환경변수 사용:
          export ANTHROPIC_API_KEY="sk-ant-..."
   3) 세션 유지:    tmux new -s dev
======================================================
DONE
}

main "$@"
