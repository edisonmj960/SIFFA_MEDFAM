#!/usr/bin/env bash
# Actualizar despliegue SIFFA MedFam en Ubuntu (correr desde el servidor).
# - Hace git pull de origin/main
# - Reinstala requirements
# - Reinicia siffa.service (gunicorn)
#
# Ejecutar como root:
#   cd /opt/siffa && sudo bash update_siffa.sh
#
set -euo pipefail

SIFFA_APP_DIR="${SIFFA_APP_DIR:-$(cd "$(dirname "$0")" && pwd)}"
SIFFA_UNIX_USER="${SIFFA_UNIX_USER:-siffa}"
SERVICE_NAME="${SERVICE_NAME:-siffa}"
VENV_DIR="$SIFFA_APP_DIR/.venv"

BLUE='\033[1;34m'; GREEN='\033[1;32m'; RED='\033[1;31m'; YELLOW='\033[1;33m'; NC='\033[0m'
log(){ echo -e "${BLUE}[siffa update]${NC} $*"; }
ok(){  echo -e "${GREEN}[OK]${NC}             $*"; }
warn(){ echo -e "${YELLOW}[!]${NC}            $*"; }

require_root(){
  if [[ "$(id -u)" -ne 0 ]]; then
    echo -e "${RED}ERROR: ejecuta este script como root (sudo bash update_siffa.sh).${NC}" >&2; exit 1
  fi
}
require_root

log "cd $SIFFA_APP_DIR"
cd "$SIFFA_APP_DIR"

if [[ ! -d .git ]]; then
  echo -e "${RED}ERROR: $SIFFA_APP_DIR no es un repo git (falta .git/).${NC}" >&2
  exit 2
fi

BEFORE=$(git rev-parse --short HEAD)
log "Pulling origin/main (antes: $BEFORE)…"
git fetch --depth 1 origin main
git reset --hard origin/main
AFTER=$(git rev-parse --short HEAD)
ok "actualizado: $BEFORE → $AFTER"

chown -R "$SIFFA_UNIX_USER:$SIFFA_UNIX_USER" "$SIFFA_APP_DIR"

if [[ -d "$VENV_DIR" ]]; then
  log "Actualizando dependencias en $VENV_DIR…"
  if [[ ! -x "$VENV_DIR/bin/pip" ]]; then
    warn "$VENV_DIR no tiene pip; regenerando venv…"
    rm -rf "$VENV_DIR"
    python3 -m venv "$VENV_DIR"
    chown -R "$SIFFA_UNIX_USER:$SIFFA_UNIX_USER" "$VENV_DIR"
  fi
  sudo -u "$SIFFA_UNIX_USER" "$VENV_DIR/bin/pip" install --quiet --upgrade pip setuptools wheel >/dev/null
  sudo -u "$SIFFA_UNIX_USER" "$VENV_DIR/bin/pip" install --quiet -r requirements.txt gunicorn
  ok "dependencias actualizadas"
else
  warn "no existe .venv; desplegar con deploy_siffa_ubuntu.sh primero"
fi

if systemctl list-unit-files | grep -q "^${SERVICE_NAME}.service"; then
  log "Reiniciando $SERVICE_NAME.service…"
  systemctl restart "$SERVICE_NAME"
  sleep 2
  if systemctl is-active --quiet "$SERVICE_NAME"; then
    ok "$SERVICE_NAME activo"
  else
    warn "$SERVICE_NAME NO está activo. Ejecuta: journalctl -u $SERVICE_NAME -n 100 --no-pager"
  fi
fi

log "Smoke test 127.0.0.1:8000/login…"
HTTP=$(curl -sS -m 15 -o /dev/null -w "%{http_code}" http://127.0.0.1:8000/login 2>/dev/null || echo "000")
if [[ "$HTTP" == "200" || "$HTTP" == "302" || "$HTTP" == "301" ]]; then
  ok "gunicorn responde HTTP $HTTP"
else
  warn "gunicorn devolvió HTTP $HTTP. Revisa journalctl."
fi

echo -e "${GREEN}Update terminado ($BEFORE → $AFTER).${NC}"
