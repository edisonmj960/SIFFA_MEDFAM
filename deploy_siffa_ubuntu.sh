#!/usr/bin/env bash
# Script de despliegue SIFFA MedFam — Ubuntu 18.04/20.04/22.04/24.04
# Ejecutar como root:  sudo -i ; bash deploy_siffa_ubuntu.sh
#
# Variables opcionales (se pueden exportar ANTES de ejecutar):
#   export SIFFA_DOMAIN="siffa.medfam.com.co"      # dejar vacío => solo IP local
#   export SIFFA_BIND_IP="172.20.20.119"           # por defecto detecta la IP principal
#   export SIFFA_GIT_REPO="https://github.com/edisonmj960/SIFFA_MEDFAM.git"
#   export SIFFA_APP_DIR="/opt/siffa"
#   export SIFFA_UNIX_USER="siffa"
#   export SIFFA_USE_NGINX="1"                      # 0 = no instalar nginx (solo gunicorn en :8000)
#   export SIFFA_SSL_CERTBOT="1"                    # 0 = no solicitar Let's Encrypt (solo si hay dominio público + puerto 80 abierto)
#   export HTTPS_PROXY="http://usuario:clave@proxy:puerto"    # opcional si SISPRO requiere proxy CO
#   export HTTP_PROXY="http://usuario:clave@proxy:puerto"     # opcional
#   export NO_PROXY=".sispro.gov.co,localhost,127.0.0.1"      # opcional
#   export SIIFA_SSL_VERIFY="1"                                # 0 = deshabilitar si hay inspección TLS corporativa
#   export ADMIN_EMAIL="soporte@medfam.com.co"                 # opcional para certbot
#
set -euo pipefail
trap 'echo -e "\n[!] ERROR en línea $LINENO. Revisa arriba. Saliendo."' ERR

SIFFA_GIT_REPO="${SIFFA_GIT_REPO:-https://github.com/edisonmj960/SIFFA_MEDFAM.git}"
SIFFA_APP_DIR="${SIFFA_APP_DIR:-/opt/siffa}"
SIFFA_UNIX_USER="${SIFFA_UNIX_USER:-siffa}"
SIFFA_USE_NGINX="${SIFFA_USE_NGINX:-1}"
SIFFA_SSL_CERTBOT="${SIFFA_SSL_CERTBOT:-0}"
SIFFA_DOMAIN="${SIFFA_DOMAIN:-}"
ADMIN_EMAIL="${ADMIN_EMAIL:-}"

BLUE='\033[1;34m'; GREEN='\033[1;32m'; RED='\033[1;31m'; YELLOW='\033[1;33m'; NC='\033[0m'
log(){ echo -e "${BLUE}[siffa]${NC} $*"; }
ok(){  echo -e "${GREEN}[OK]${NC}    $*"; }
warn(){ echo -e "${YELLOW}[!]${NC}   $*"; }

require_root(){
  if [[ "$(id -u)" -ne 0 ]]; then
    echo -e "${RED}ERROR: ejecuta este script como root (sudo -i).${NC}" >&2; exit 1
  fi
}
detect_ip(){
  if [[ -n "${SIFFA_BIND_IP:-}" ]]; then echo "$SIFFA_BIND_IP"; return; fi
  local ip
  ip=$(hostname -I 2>/dev/null | awk '{print $1}') || true
  if [[ -z "$ip" ]]; then
    ip=$(ip -4 route get 1 2>/dev/null | awk '{print $7; exit}') || true
  fi
  echo "${ip:-127.0.0.1}"
}

###########################
# 0) Pre-requisitos
###########################
require_root
BIND_IP=$(detect_ip)
log "SIFFA desplegando en: $BIND_IP  (dominio: ${SIFFA_DOMAIN:-ninguno})"

export DEBIAN_FRONTEND=noninteractive
log "Actualizando índices apt…"
apt-get update -y >/dev/null 2>&1 || warn "apt update falló parcialmente, continuando…"

log "Instalando paquetes base (git, python3, venv, curl, ca-certificates, nginx)…"
pkgs=(git curl ca-certificates gnupg lsb-release sudo tar unzip)
# Detectar python3 >= 3.9 preferido
if ! command -v python3 >/dev/null 2>&1 || ! python3 -c 'import sys; sys.exit(0 if sys.version_info>=(3,8) else 1)' >/dev/null 2>&1; then
  # Ubuntu 18.04: python3.6 por defecto; necesitamos 3.8+ via ppa:deadsnakes
  release=$(lsb_release -rs 2>/dev/null || true)
  if [[ "$release" == "18.04" ]]; then
    pkgs+=(software-properties-common)
  fi
fi
pkgs+=(python3 python3-venv python3-pip)
if [[ "$SIFFA_USE_NGINX" == "1" ]]; then pkgs+=(nginx); fi
apt-get install -y "${pkgs[@]}" >/dev/null

# Fallback: si python3 < 3.8, instalar python3.10/3.9 via deadsnakes
if ! python3 -c 'import sys; sys.exit(0 if sys.version_info>=(3,8) else 1)' >/dev/null 2>&1; then
  log "Python < 3.8 detectado; instalando python3.10 via ppa:deadsnakes…"
  (apt-get install -y software-properties-common >/dev/null 2>&1 || true)
  (add-apt-repository -y ppa:deadsnakes/ppa >/dev/null 2>&1 || warn "deadsnakes no disponible; se intentará python3.10 vía apt")
  apt-get update -y >/dev/null 2>&1 || true
  apt-get install -y python3.10 python3.10-venv python3.10-dev >/dev/null
  PY_BIN="/usr/bin/python3.10"
else
  PY_BIN="$(command -v python3)"
fi
log "Python a usar: $(${PY_BIN} --version)"

if [[ "$SIFFA_SSL_CERTBOT" == "1" && -n "$SIFFA_DOMAIN" ]]; then
  log "Instalando certbot para Let's Encrypt…"
  apt-get install -y certbot python3-certbot-nginx >/dev/null 2>&1 || warn "certbox no disponible en apt; continúa sin SSL automático"
fi

###########################
# 1) Usuario Unix + carpeta app + git clone
###########################
if ! id -u "$SIFFA_UNIX_USER" >/dev/null 2>&1; then
  log "Creando usuario sistema: $SIFFA_UNIX_USER"
  useradd -r -s /usr/sbin/nologin -m -d "$SIFFA_APP_DIR" "$SIFFA_UNIX_USER"
  ok "usuario creado"
fi

if [[ ! -d "$SIFFA_APP_DIR/.git" ]]; then
  log "Clonando $SIFFA_GIT_REPO → $SIFFA_APP_DIR"
  rm -rf "$SIFFA_APP_DIR" 2>/dev/null || true
  git clone --depth 1 --branch main "$SIFFA_GIT_REPO" "$SIFFA_APP_DIR"
  ok "git clone OK: $(cd "$SIFFA_APP_DIR" && git rev-parse --short HEAD)"
else
  log "Ya existe repositorio en $SIFFA_APP_DIR; haciendo git pull…"
  (cd "$SIFFA_APP_DIR" && git fetch --depth 1 origin main && git reset --hard origin/main)
  ok "git pull OK: $(cd "$SIFFA_APP_DIR" && git rev-parse --short HEAD)"
fi

chown -R "$SIFFA_UNIX_USER:$SIFFA_UNIX_USER" "$SIFFA_APP_DIR"
chmod 750 "$SIFFA_APP_DIR"

###########################
# 2) Entorno virtual + dependencias
###########################
VENV_DIR="$SIFFA_APP_DIR/.venv"
log "Creando entorno virtual en $VENV_DIR…"
sudo -u "$SIFFA_UNIX_USER" "$PY_BIN" -m venv "$VENV_DIR"
sudo -u "$SIFFA_UNIX_USER" "$VENV_DIR/bin/pip" install --quiet --upgrade pip setuptools wheel >/dev/null
log "Instalando requirements.txt + gunicorn…"
sudo -u "$SIFFA_UNIX_USER" "$VENV_DIR/bin/pip" install --quiet -r "$SIFFA_APP_DIR/requirements.txt" gunicorn
ok "pip install OK"

###########################
# 3) Variables de entorno persistentes (systemd override + /etc/environment)
###########################
ENV_FILE="/etc/siffa.env"
log "Escribiendo variables de entorno en $ENV_FILE (edítalo luego para HTTPS_PROXY etc)…"
cat >"$ENV_FILE" <<EOF
# Variables SIIFA — aplica a gunicorn/systemd
# Edita este archivo y ejecuta:  systemctl restart siffa
SIIFA_SECURITY_BASEURL=https://siifa.sispro.gov.co/siifa-seguridad
SIIFA_FACTURA_BASEURL=https://siifa.sispro.gov.co/siifa-factura
SIIFA_MAX_RETRIES=3
SIIFA_CONNECT_TIMEOUT=10
SIIFA_READ_TIMEOUT=60
SIIFA_SSL_VERIFY=${SIIFA_SSL_VERIFY:-1}
EOF
# Si el usuario exportó variables de proxy antes de correr el script => se incorporan
for k in HTTP_PROXY HTTPS_PROXY NO_PROXY; do
  v="${!k:-}"
  if [[ -n "$v" ]]; then echo "${k}=${v}" >>"$ENV_FILE"; fi
done
chmod 640 "$ENV_FILE"
chown root:"$SIFFA_UNIX_USER" "$ENV_FILE"
ok "variables escritas en $ENV_FILE"

###########################
# 4) Systemd service (gunicorn en 127.0.0.1:8000)
###########################
SVC_FILE="/etc/systemd/system/siffa.service"
log "Instalando $SVC_FILE…"
cat >"$SVC_FILE" <<EOF
[Unit]
Description=SIFFA MedFam Flask App (gunicorn)
After=network.target

[Service]
Type=notify
User=${SIFFA_UNIX_USER}
Group=${SIFFA_UNIX_USER}
RuntimeDirectory=siffa
WorkingDirectory=${SIFFA_APP_DIR}
EnvironmentFile=${ENV_FILE}
ExecStart=${VENV_DIR}/bin/gunicorn \
          --workers 3 \
          --threads 4 \
          --timeout 900 \
          --keep-alive 5 \
          --bind 127.0.0.1:8000 \
          --access-logfile /var/log/siffa/access.log \
          --error-logfile /var/log/siffa/error.log \
          --log-level info \
          web_app:app
ExecReload=/bin/kill -HUP \$MAINPID
KillMode=mixed
TimeoutStopSec=30
PrivateTmp=true
Restart=always
RestartSec=2
# Seguridad
NoNewPrivileges=true
ProtectSystem=strict
ReadWritePaths=${SIFFA_APP_DIR} /var/log/siffa /tmp
ProtectHome=tmpfs

[Install]
WantedBy=multi-user.target
EOF

mkdir -p /var/log/siffa
chown -R "$SIFFA_UNIX_USER:$SIFFA_UNIX_USER" /var/log/siffa
chmod 750 /var/log/siffa

systemctl daemon-reload
systemctl enable --now siffa >/dev/null
sleep 3
if systemctl is-active --quiet siffa; then ok "siffa.service activo (gunicorn :8000)";
else warn "siffa.service no arrancó. Ejecuta:  journalctl -u siffa -n 100 --no-pager"; fi

# Smoke test gunicorn
sleep 1
if curl -sS -m 10 -o /dev/null -w "gunicorn: HTTP %{http_code}\n" http://127.0.0.1:8000/login >/dev/null; then
  ok "respuesta gunicorn OK en 127.0.0.1:8000/login"
else
  warn "gunicorn no responde aún en 127.0.0.1:8000 — revisar journalctl -u siffa"
fi

###########################
# 5) Nginx reverse proxy (puerto 80 => :8000)
###########################
if [[ "$SIFFA_USE_NGINX" == "1" ]]; then
  log "Configurando nginx reverse proxy…"
  ngx_site="/etc/nginx/sites-available/siffa.conf"
  cat >"$ngx_site" <<EOF
server {
    listen 80 default_server;
    listen [::]:80 default_server;
    server_name ${SIFFA_DOMAIN:-_} ${BIND_IP};
    client_max_body_size 50m;

    access_log /var/log/siffa/nginx-access.log;
    error_log  /var/log/siffa/nginx-error.log;

    location / {
        proxy_set_header Host              \$host;
        proxy_set_header X-Real-IP         \$remote_addr;
        proxy_set_header X-Forwarded-For   \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
        proxy_read_timeout 900s;
        proxy_connect_timeout 15s;
        proxy_send_timeout 900s;
        proxy_buffering off;
        proxy_pass http://127.0.0.1:8000;
    }
}
EOF
  # Deshabilitar site default de nginx
  if [[ -L /etc/nginx/sites-enabled/default ]]; then rm -f /etc/nginx/sites-enabled/default; fi
  if [[ -f /etc/nginx/sites-enabled/default ]]; then rm -f /etc/nginx/sites-enabled/default; fi
  ln -sf "$ngx_site" /etc/nginx/sites-enabled/siffa.conf
  if nginx -t >/dev/null 2>&1; then
    systemctl enable --now nginx >/dev/null 2>&1 || true
    systemctl reload nginx || systemctl restart nginx
    ok "nginx activo en http://${BIND_IP}:80"
  else
    warn "nginx -t falló. Revisa /etc/nginx/sites-available/siffa.conf"
    nginx -t || true
  fi
fi

###########################
# 6) Let's Encrypt (solo si hay dominio público y email)
###########################
if [[ "$SIFFA_SSL_CERTBOT" == "1" && -n "$SIFFA_DOMAIN" ]]; then
  if command -v certbot >/dev/null 2>&1; then
    log "Solicitando Let's Encrypt para $SIFFA_DOMAIN…"
    if [[ -n "$ADMIN_EMAIL" ]]; then
      certbot --non-interactive --agree-tos --redirect --nginx \
        -m "$ADMIN_EMAIL" -d "$SIFFA_DOMAIN" || warn "certbot falló; ejecutalo manualmente luego"
    else
      certbot --non-interactive --agree-tos --redirect --nginx \
        --register-unsafely-without-email -d "$SIFFA_DOMAIN" || warn "certbot falló; ejecutalo manualmente luego"
    fi
    ok "certbot intento completado"
  fi
fi

###########################
# 7) Diagnóstico final de salida a SIIFA
###########################
echo
log "===== DIAGNÓSTICO FINAL DE CONECTIVIDAD CON SISPRO ====="
echo
if [[ -n "${HTTPS_PROXY:-}" ]]; then
  echo "Usando HTTPS_PROXY=${HTTPS_PROXY}"
fi
CURL_OPTS=(-sS -m 15 -o /dev/null -w "HTTP %{http_code} | IP \$remote_ip | total %{time_total}s | connect %{time_connect}s\\n")
echo -n "siifa-seguridad/Auth/login  => "
(curl "${CURL_OPTS[@]}" https://siifa.sispro.gov.co/siifa-seguridad/api/Auth/login 2>&1 || echo "FAIL ConnectTimeout (whitelist/GeoIP/proxy?)")
echo -n "siifa-factura/api/Factura    => "
(curl "${CURL_OPTS[@]}" https://siifa.sispro.gov.co/siifa-factura/api/Factura 2>&1 || echo "FAIL ConnectTimeout (whitelist/GeoIP/proxy?)")
echo

###########################
# 8) Mensajes al usuario
###########################
echo
echo -e "${GREEN}===== DESPLIEGUE COMPLETADO =====${NC}"
echo
echo "  ▸ Acceso local/intranet:     http://${BIND_IP}/"
if [[ -n "$SIFFA_DOMAIN" && "$SIFFA_SSL_CERTBOT" == "1" ]]; then
  echo "  ▸ Dominio SSL:               https://${SIFFA_DOMAIN}/"
fi
echo "  ▸ Gunicorn (interno):        http://127.0.0.1:8000"
echo "  ▸ App dir:                   $SIFFA_APP_DIR"
echo "  ▸ Logs:                      /var/log/siffa/   (access, error, nginx-*)"
echo "  ▸ Variables entorno:         $ENV_FILE   (edita HTTPS_PROXY aquí si hace falta)"
echo
echo "Comandos útiles:"
echo "  systemctl status siffa            # ver estado servicio"
echo "  journalctl -u siffa -f            # ver logs en vivo"
echo "  sudo -u $SIFFA_UNIX_USER $VENV_DIR/bin/python $SIFFA_APP_DIR/siifa_bulk_client.py diagnostico"
echo "  cd $SIFFA_APP_DIR && bash update_siffa.sh      # actualizar desde GitHub"
echo
if [[ ! -f "$SIFFA_APP_DIR/update_siffa.sh" ]]; then
  warn "Aún no existe $SIFFA_APP_DIR/update_siffa.sh; lo crearemos en el próximo commit del repo"
fi
