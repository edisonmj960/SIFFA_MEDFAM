import argparse
import json
import os
import sys
import time
import urllib.parse

try:
    import requests
    from requests.adapters import HTTPAdapter
    try:
        from urllib3.util import Retry
    except Exception:
        from urllib3.util.retry import Retry
except Exception as _import_err:
    print("IMPORTANTE: Instale dependencias con: pip install requests urllib3", file=sys.stderr)
    raise


class SiifaApiError(RuntimeError):
    def __init__(self, message, status=None, payload=None):
        super().__init__(message)
        self.status = status
        self.payload = payload


def _join_url(base_url: str, path: str) -> str:
    base_url = (base_url or "").strip()
    if not base_url:
        raise ValueError("base_url requerido")
    if not base_url.endswith("/"):
        base_url += "/"
    path = path.lstrip("/")
    return urllib.parse.urljoin(base_url, path)


def _build_http_session(
    total_retries: int | None = None,
    backoff_factor: float = 1.0,
    timeout_connect: int | None = None,
    timeout_read: int | None = None,
) -> tuple["requests.Session", tuple[int, int]]:
    if total_retries is None:
        try:
            total_retries = int(os.environ.get("SIIFA_MAX_RETRIES", "3"))
        except Exception:
            total_retries = 3
    total_retries = max(0, min(int(total_retries), 10))

    if timeout_connect is None:
        try:
            timeout_connect = int(os.environ.get("SIIFA_CONNECT_TIMEOUT", "10"))
        except Exception:
            timeout_connect = 10
    if timeout_read is None:
        try:
            timeout_read = int(os.environ.get("SIIFA_READ_TIMEOUT", "60"))
        except Exception:
            timeout_read = 60
    timeout_connect = max(3, min(int(timeout_connect), 120))
    timeout_read = max(10, min(int(timeout_read), 900))

    session = requests.Session()

    retry_on_status = [408, 429, 500, 502, 503, 504, 520, 521, 522, 523, 524, 525, 527]
    try:
        retry_on_status_set = set(retry_on_status)
        retry_connect_errors = total_retries
        retry_read_errors = max(1, total_retries // 2)
        retry_strategy = Retry(
            total=total_retries,
            connect=retry_connect_errors,
            read=retry_read_errors,
            status=total_retries,
            backoff_factor=backoff_factor,
            status_forcelist=retry_on_status_set,
            allowed_methods=["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"],
            raise_on_status=False,
            respect_retry_after_header=True,
            remove_headers_on_redirect=["Authorization"],
        )
    except Exception:
        retry_strategy = Retry(
            total=total_retries,
            backoff_factor=backoff_factor,
            status_forcelist=retry_on_status,
            allowed_methods=["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"],
            raise_on_status=False,
            respect_retry_after_header=True,
        )

    adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=20)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    session.headers.update(
        {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "es-CO,es;q=0.9,en;q=0.8",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36 SiifaClient/1.1",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }
    )

    http_proxy = os.environ.get("HTTP_PROXY") or os.environ.get("http_proxy")
    https_proxy = os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy")
    no_proxy = os.environ.get("NO_PROXY") or os.environ.get("no_proxy")
    proxies = {}
    if http_proxy:
        proxies["http"] = http_proxy
    if https_proxy:
        proxies["https"] = https_proxy
    if proxies:
        session.proxies.update(proxies)
    if no_proxy:
        session.trust_env = True

    ssl_verify_env = os.environ.get("SIIFA_SSL_VERIFY", "1").strip().lower()
    if ssl_verify_env in ("0", "false", "no", "off"):
        session.verify = False
        try:
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        except Exception:
            pass
    else:
        session.verify = True

    timeouts = (timeout_connect, timeout_read)
    return session, timeouts


_GLOBAL_SESSION: "requests.Session | None" = None
_GLOBAL_TIMEOUTS: tuple[int, int] = (30, 180)


def _get_session() -> tuple["requests.Session", tuple[int, int]]:
    global _GLOBAL_SESSION, _GLOBAL_TIMEOUTS
    if _GLOBAL_SESSION is None:
        _GLOBAL_SESSION, _GLOBAL_TIMEOUTS = _build_http_session()
    return _GLOBAL_SESSION, _GLOBAL_TIMEOUTS


def _request_json(
    method: str,
    url: str,
    token: str | None = None,
    body: object | None = None,
    timeout_s: float | None = None,
) -> object:
    session, default_timeouts = _get_session()

    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    if timeout_s is not None:
        timeouts = (default_timeouts[0], int(float(timeout_s)))
    else:
        timeouts = default_timeouts

    attempts = 0
    max_attempts = 2
    last_err: Exception | None = None

    while attempts < max_attempts:
        attempts += 1
        try:
            if body is not None:
                headers["Content-Type"] = "application/json"
                data = json.dumps(body, ensure_ascii=False).encode("utf-8")
                resp = session.request(
                    method=method.upper(),
                    url=url,
                    data=data,
                    headers=headers,
                    timeout=timeouts,
                )
            else:
                resp = session.request(
                    method=method.upper(),
                    url=url,
                    headers=headers,
                    timeout=timeouts,
                )

            content_type = resp.headers.get("Content-Type", "")
            raw = resp.content
            text = raw.decode("utf-8", errors="replace") if raw else ""

            payload = None
            if text.strip():
                if "application/json" in content_type.lower() or text.lstrip().startswith(("{", "[")):
                    try:
                        payload = json.loads(text)
                    except Exception:
                        payload = text
                else:
                    payload = text

            if resp.status_code >= 400:
                message = f"HTTP {resp.status_code} al llamar {url}"
                if payload and isinstance(payload, dict):
                    msg_extra = (
                        payload.get("message")
                        or payload.get("Message")
                        or payload.get("mensaje")
                        or payload.get("Mensaje")
                        or payload.get("error")
                        or payload.get("Error")
                    )
                    if msg_extra:
                        message = f"{message}: {msg_extra}"
                raise SiifaApiError(message, status=resp.status_code, payload=payload)

            return payload

        except SiifaApiError:
            raise
        except requests.exceptions.SSLError as e:
            last_err = e
            hint = (
                " (SSL/TLS falló. Si está en red corporativa, configure "
                "SIIFA_SSL_VERIFY=0 o HTTPS_PROXY con el proxy corporativo)"
            )
            if attempts >= max_attempts:
                raise SiifaApiError(f"Error SSL/TLS al llamar {url}: {e}{hint}") from e
            time.sleep(1.0)
        except requests.exceptions.ProxyError as e:
            last_err = e
            raise SiifaApiError(
                f"Error de proxy al llamar {url}: {e}. "
                "Revise HTTP_PROXY/HTTPS_PROXY o configure NO_PROXY para el dominio."
            ) from e
        except requests.exceptions.ConnectionError as e:
            last_err = e
            hint = (
                " (no se pudo conectar al servidor SIIFA. "
                "Posibles causas: 1) La IP del servidor no está en whitelist de SISPRO, "
                "2) Bloqueo de red/firewall, 3) Requiere VPN o IP colombiana, "
                "4) El servicio SIIFA está temporalmente caído. "
                "Solución: configure HTTPS_PROXY hacia un proxy con IP colombiana "
                "o despliegue en servidor con IP whitelisteada.)"
            )
            if attempts >= max_attempts:
                raise SiifaApiError(f"Error de red al llamar {url}: {e}{hint}") from e
            time.sleep(1.5)
        except requests.exceptions.Timeout as e:
            last_err = e
            if attempts >= max_attempts:
                raise SiifaApiError(
                    f"Timeout al llamar {url}: {e}. "
                    "El servicio SIIFA respondió lento o está saturado."
                ) from e
            time.sleep(2.0)
        except Exception as e:
            last_err = e
            raise SiifaApiError(f"Error al llamar {url}: {e}") from e

    if last_err is not None:
        raise SiifaApiError(f"Error al llamar {url}: {last_err}")
    raise SiifaApiError(f"Error desconocido al llamar {url}")


class SiifaClient:
    def __init__(self, seguridad_base_url: str, factura_base_url: str):
        self.seguridad_base_url = seguridad_base_url
        self.factura_base_url = factura_base_url
        self.token = None

    def login(self, user_name: str, password: str) -> str:
        url = _join_url(self.seguridad_base_url, "/api/Auth/login")
        result = _request_json("POST", url, body={"userName": user_name, "password": password})
        if not isinstance(result, dict):
            raise SiifaApiError("Respuesta inesperada en login", payload=result)
        if not result.get("success") or not result.get("token"):
            raise SiifaApiError("Login falló", payload=result)
        self.token = result["token"]
        return self.token

    def list_facturas(self, **query_params) -> dict:
        if not self.token:
            raise ValueError("Debe autenticarse primero (token vacío).")
        url = _join_url(self.factura_base_url, "/api/Factura")
        query = {k: v for k, v in query_params.items() if v is not None and v != ""}
        if query:
            url = f"{url}?{urllib.parse.urlencode(query, doseq=True)}"
        result = _request_json("GET", url, token=self.token)
        if not isinstance(result, dict):
            raise SiifaApiError("Respuesta inesperada al consultar facturas", payload=result)
        return result

    def get_factura(self, id_factura: int) -> dict:
        if not self.token:
            raise ValueError("Debe autenticarse primero (token vacío).")
        url = _join_url(self.factura_base_url, f"/api/Factura/{int(id_factura)}")
        result = _request_json("GET", url, token=self.token)
        if not isinstance(result, dict):
            raise SiifaApiError("Respuesta inesperada al consultar detalle de factura", payload=result)
        return result

    def list_rips_transaccion(self, **query_params) -> dict:
        if not self.token:
            raise ValueError("Debe autenticarse primero (token vacío).")
        url = _join_url(self.factura_base_url, "/api/RipsTransaccion")
        query = {k: v for k, v in query_params.items() if v is not None and v != ""}
        if query:
            url = f"{url}?{urllib.parse.urlencode(query, doseq=True)}"
        result = _request_json("GET", url, token=self.token)
        if not isinstance(result, dict):
            raise SiifaApiError("Respuesta inesperada al consultar RIPS Transacción", payload=result)
        return result

    def list_rips_usuarios(self, **query_params) -> dict:
        if not self.token:
            raise ValueError("Debe autenticarse primero (token vacío).")
        url = _join_url(self.factura_base_url, "/api/RipsUsuarios")
        query = {k: v for k, v in query_params.items() if v is not None and v != ""}
        if query:
            url = f"{url}?{urllib.parse.urlencode(query, doseq=True)}"
        result = _request_json("GET", url, token=self.token)
        if not isinstance(result, dict):
            raise SiifaApiError("Respuesta inesperada al consultar RIPS Usuarios", payload=result)
        return result

    def iter_facturas(self, **query_params):
        page = int(query_params.get("NumeroPagina") or 1)
        per_page = int(query_params.get("RegistrosPorPagina") or 1500)
        while True:
            query_params["NumeroPagina"] = page
            query_params["RegistrosPorPagina"] = per_page
            page_result = self.list_facturas(**query_params)
            items = page_result.get("resultado") or []
            if not items:
                break
            for item in items:
                yield item
            total_pages = page_result.get("totalPaginas")
            if total_pages is not None and page >= int(total_pages):
                break
            page += 1

    def radicar_masivo(self, lista_radicado: list[dict]) -> list[dict]:
        if not self.token:
            raise ValueError("Debe autenticarse primero (token vacío).")
        url = _join_url(self.factura_base_url, "/api/FacturaRadicado/Masivo")
        body = {"listaRadicado": lista_radicado}
        result = _request_json("POST", url, token=self.token, body=body, timeout_s=300.0)
        if not isinstance(result, list):
            raise SiifaApiError("Respuesta inesperada al radicar masivo", payload=result)
        return result

    def crear_radicado(self, id_factura: int, radicado: str, fecha_radicado: str) -> dict:
        if not self.token:
            raise ValueError("Debe autenticarse primero (token vacío).")
        url = _join_url(self.factura_base_url, "/api/FacturaRadicado")
        body = {"idFactura": int(id_factura), "radicado": radicado, "fechaRadicado": fecha_radicado}
        result = _request_json("POST", url, token=self.token, body=body, timeout_s=300.0)
        if not isinstance(result, dict):
            raise SiifaApiError("Respuesta inesperada al crear radicado", payload=result)
        return result

    def list_radicados_by_id_factura(self, id_factura: int) -> list[dict]:
        if not self.token:
            raise ValueError("Debe autenticarse primero (token vacío).")
        url = _join_url(self.factura_base_url, f"/api/FacturaRadicado/ByIdFactura/{int(id_factura)}")
        result = _request_json("GET", url, token=self.token)
        if not isinstance(result, list):
            raise SiifaApiError("Respuesta inesperada al consultar radicados por factura", payload=result)
        return result

    def list_seguimiento_factura(self, **query_params) -> dict:
        if not self.token:
            raise ValueError("Debe autenticarse primero (token vacío).")
        url = _join_url(self.factura_base_url, "/api/SeguimientoFactura/List")
        query = {k: v for k, v in query_params.items() if v is not None and v != ""}
        if query:
            url = f"{url}?{urllib.parse.urlencode(query, doseq=True)}"
        result = _request_json("GET", url, token=self.token)
        if not isinstance(result, dict):
            raise SiifaApiError("Respuesta inesperada al consultar seguimiento de facturas", payload=result)
        return result

    def list_seguimiento_pago(self, **query_params) -> dict:
        if not self.token:
            raise ValueError("Debe autenticarse primero (token vacío).")
        url = _join_url(self.factura_base_url, "/api/SeguimientoFacturaPago/ByIdFactura")
        query = {k: v for k, v in query_params.items() if v is not None and v != ""}
        if query:
            url = f"{url}?{urllib.parse.urlencode(query, doseq=True)}"
        result = _request_json("GET", url, token=self.token)
        if not isinstance(result, dict):
            raise SiifaApiError("Respuesta inesperada al consultar seguimiento de pagos", payload=result)
        return result

    def list_pagos(self, **query_params) -> dict:
        if not self.token:
            raise ValueError("Debe autenticarse primero (token vacío).")
        url = _join_url(self.factura_base_url, "/api/SeguimientoFacturaPago")
        query = {k: v for k, v in query_params.items() if v is not None and v != ""}
        if query:
            url = f"{url}?{urllib.parse.urlencode(query, doseq=True)}"
        result = _request_json("GET", url, token=self.token)
        if not isinstance(result, dict):
            raise SiifaApiError("Respuesta inesperada al consultar pagos", payload=result)
        return result

    def crear_devoluciones_masivo(self, lista_devoluciones: list[dict]) -> list[dict]:
        if not self.token:
            raise ValueError("Debe autenticarse primero (token vacío).")
        url = _join_url(self.factura_base_url, "/api/SeguimientoFacturaDevolucion/Masivo")
        body = {"listaDevoluciones": lista_devoluciones}
        result = _request_json("POST", url, token=self.token, body=body, timeout_s=300.0)
        if not isinstance(result, list):
            raise SiifaApiError("Respuesta inesperada al crear devoluciones masivas", payload=result)
        return result

    def crear_devolucion(self, payload: dict) -> dict:
        if not self.token:
            raise ValueError("Debe autenticarse primero (token vacío).")
        url = _join_url(self.factura_base_url, "/api/SeguimientoFacturaDevolucion")
        result = _request_json("POST", url, token=self.token, body=payload, timeout_s=300.0)
        if not isinstance(result, dict):
            raise SiifaApiError("Respuesta inesperada al crear devolución", payload=result)
        return result

    def list_devoluciones_by_id_factura(self, **query_params) -> dict:
        if not self.token:
            raise ValueError("Debe autenticarse primero (token vacío).")
        url = _join_url(self.factura_base_url, "/api/SeguimientoFacturaDevolucion/ByIdFactura")
        query = {k: v for k, v in query_params.items() if v is not None and v != ""}
        if query:
            url = f"{url}?{urllib.parse.urlencode(query, doseq=True)}"
        result = _request_json("GET", url, token=self.token)
        if not isinstance(result, dict):
            raise SiifaApiError("Respuesta inesperada al consultar devoluciones", payload=result)
        return result

    def resumen_devoluciones_by_id_factura(self, id_factura: int) -> dict:
        if not self.token:
            raise ValueError("Debe autenticarse primero (token vacío).")
        url = _join_url(self.factura_base_url, f"/api/SeguimientoFacturaDevolucion/Resumen/ByIdFactura/{int(id_factura)}")
        result = _request_json("GET", url, token=self.token)
        if not isinstance(result, dict):
            raise SiifaApiError("Respuesta inesperada al consultar resumen de devoluciones", payload=result)
        return result

    def crear_glosas_masivo(self, lista_glosas: list[dict]) -> list[dict]:
        if not self.token:
            raise ValueError("Debe autenticarse primero (token vacío).")
        url = _join_url(self.factura_base_url, "/api/SeguimientoFacturaGlosa/Masivo")
        body = {"listaGlosas": lista_glosas}
        result = _request_json("POST", url, token=self.token, body=body, timeout_s=300.0)
        if not isinstance(result, list):
            raise SiifaApiError("Respuesta inesperada al crear glosas masivas", payload=result)
        return result

    def crear_glosa(self, payload: dict) -> dict:
        if not self.token:
            raise ValueError("Debe autenticarse primero (token vacío).")
        url = _join_url(self.factura_base_url, "/api/SeguimientoFacturaGlosa")
        result = _request_json("POST", url, token=self.token, body=payload, timeout_s=300.0)
        if not isinstance(result, dict):
            raise SiifaApiError("Respuesta inesperada al crear glosa", payload=result)
        return result

    def responder_glosa(self, payload: dict) -> dict:
        if not self.token:
            raise ValueError("Debe autenticarse primero (token vacío).")
        url = _join_url(self.factura_base_url, "/api/SeguimientoFacturaGlosa/Respuesta")
        result = _request_json("PUT", url, token=self.token, body=payload, timeout_s=300.0)
        if not isinstance(result, dict):
            raise SiifaApiError("Respuesta inesperada al responder glosa", payload=result)
        return result

    def list_glosas_by_id_factura(self, **query_params) -> dict:
        if not self.token:
            raise ValueError("Debe autenticarse primero (token vacío).")
        url = _join_url(self.factura_base_url, "/api/SeguimientoFacturaGlosa/ByIdFactura")
        query = {k: v for k, v in query_params.items() if v is not None and v != ""}
        if query:
            url = f"{url}?{urllib.parse.urlencode(query, doseq=True)}"
        result = _request_json("GET", url, token=self.token)
        if not isinstance(result, dict):
            raise SiifaApiError("Respuesta inesperada al consultar glosas", payload=result)
        return result

    def resumen_glosas_by_id_factura(self, id_factura: int) -> dict:
        if not self.token:
            raise ValueError("Debe autenticarse primero (token vacío).")
        url = _join_url(self.factura_base_url, f"/api/SeguimientoFacturaGlosa/Resumen/ByIdFactura/{int(id_factura)}")
        result = _request_json("GET", url, token=self.token)
        if not isinstance(result, dict):
            raise SiifaApiError("Respuesta inesperada al consultar resumen de glosas", payload=result)
        return result

    def crear_pagos_masivo(self, lista_pagos: list[dict]) -> list[dict]:
        if not self.token:
            raise ValueError("Debe autenticarse primero (token vacío).")
        url = _join_url(self.factura_base_url, "/api/SeguimientoFacturaPago/Masivo")
        body = {"listaPagos": lista_pagos}
        result = _request_json("POST", url, token=self.token, body=body, timeout_s=300.0)
        if not isinstance(result, list):
            raise SiifaApiError("Respuesta inesperada al crear pagos masivos", payload=result)
        return result

    def crear_pago(self, payload: dict) -> dict:
        if not self.token:
            raise ValueError("Debe autenticarse primero (token vacío).")
        url = _join_url(self.factura_base_url, "/api/SeguimientoFacturaPago")
        result = _request_json("POST", url, token=self.token, body=payload, timeout_s=300.0)
        if not isinstance(result, dict):
            raise SiifaApiError("Respuesta inesperada al crear pago", payload=result)
        return result

    def resumen_pagos_by_id_factura(self, id_factura: int) -> dict:
        if not self.token:
            raise ValueError("Debe autenticarse primero (token vacío).")
        url = _join_url(self.factura_base_url, f"/api/SeguimientoFacturaPago/Resumen/ByIdFactura/{int(id_factura)}")
        result = _request_json("GET", url, token=self.token)
        if not isinstance(result, dict):
            raise SiifaApiError("Respuesta inesperada al consultar resumen de pagos", payload=result)
        return result

    def list_seguimiento_tipo_codigo_by_grupo(
        self, grupo: str, nivel: int | None = None, id_padre: str | None = None
    ) -> object:
        if not self.token:
            raise ValueError("Debe autenticarse primero (token vacío).")
        url = _join_url(self.factura_base_url, "/api/SeguimientoTipoCodigo/ByGrupo")
        query = {"Grupo": grupo, "Nivel": nivel, "IdSeguimientoTipoCodigoPadre": id_padre}
        query = {k: v for k, v in query.items() if v is not None and v != ""}
        if query:
            url = f"{url}?{urllib.parse.urlencode(query, doseq=True)}"
        return _request_json("GET", url, token=self.token)


def _diagnostico_red(seguridad_base_url: str | None = None, factura_base_url: str | None = None) -> dict:
    seg_url = seguridad_base_url or os.environ.get("SIIFA_SECURITY_BASEURL", "https://siifa.sispro.gov.co/siifa-seguridad")
    fac_url = factura_base_url or os.environ.get("SIIFA_FACTURA_BASEURL", "https://siifa.sispro.gov.co/siifa-factura")
    results: dict = {}
    session, timeouts = _get_session()
    for name, base in [("seguridad", seg_url), ("factura", fac_url)]:
        url = _join_url(base, "/api/Auth/login") if name == "seguridad" else _join_url(base, "/api/Factura")
        try:
            t0 = time.time()
            resp = session.head(url, timeout=(15, 20), allow_redirects=True)
            dt = round((time.time() - t0) * 1000, 1)
            results[name] = {
                "ok": True,
                "status": resp.status_code,
                "latencia_ms": dt,
                "url": url,
                "redirected": resp.url != url,
            }
        except Exception as e:
            results[name] = {"ok": False, "error": str(type(e).__name__) + ": " + str(e), "url": url}
    return results


def _env(name: str, default: str | None = None, required: bool = False) -> str | None:
    value = os.environ.get(name, default)
    if required and not value:
        raise SystemExit(f"Falta variable de entorno: {name}")
    return value


def _cmd_consultar(args: argparse.Namespace) -> int:
    client = SiifaClient(
        seguridad_base_url=_env("SIIFA_SECURITY_BASEURL", "https://siifa.sispro.gov.co/siifa-seguridad", required=True),
        factura_base_url=_env("SIIFA_FACTURA_BASEURL", "https://siifa.sispro.gov.co/siifa-factura", required=True),
    )
    client.login(_env("SIIFA_USERNAME", required=True), _env("SIIFA_PASSWORD", required=True))

    query = {
        "NumeroFactura": args.numero_factura,
        "NitEmisor": args.nit_emisor,
        "NitAdquiriente": args.nit_adquiriente,
        "FechaEmisionInicio": args.fecha_emision_inicio,
        "FechaEmisionFinal": args.fecha_emision_final,
        "TieneRadicado": args.tiene_radicado,
        "FechaCargue": args.fecha_cargue,
        "NumeroPagina": 1,
        "RegistrosPorPagina": args.registros_por_pagina,
    }

    out_fp = None
    if args.salida:
        out_fp = open(args.salida, "w", encoding="utf-8")
    try:
        dst = out_fp or sys.stdout
        for factura in client.iter_facturas(**query):
            dst.write(json.dumps(factura, ensure_ascii=False) + "\n")
        return 0
    finally:
        if out_fp:
            out_fp.close()


def _cmd_radicar_masivo(args: argparse.Namespace) -> int:
    client = SiifaClient(
        seguridad_base_url=_env("SIIFA_SECURITY_BASEURL", "https://siifa.sispro.gov.co/siifa-seguridad", required=True),
        factura_base_url=_env("SIIFA_FACTURA_BASEURL", "https://siifa.sispro.gov.co/siifa-factura", required=True),
    )
    client.login(_env("SIIFA_USERNAME", required=True), _env("SIIFA_PASSWORD", required=True))

    with open(args.entrada, "r", encoding="utf-8") as fp:
        payload = json.load(fp)
    if isinstance(payload, dict) and "listaRadicado" in payload:
        lista = payload["listaRadicado"]
    else:
        lista = payload
    if not isinstance(lista, list):
        raise SystemExit("Entrada inválida. Debe ser una lista o un objeto con listaRadicado.")

    result = client.radicar_masivo(lista)
    text = json.dumps(result, ensure_ascii=False, indent=2)
    if args.salida:
        with open(args.salida, "w", encoding="utf-8") as fp:
            fp.write(text)
    else:
        sys.stdout.write(text + "\n")
    return 0


def _cmd_diagnostico(args: argparse.Namespace) -> int:
    result = _diagnostico_red()
    sys.stdout.write(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    all_ok = all(v.get("ok") for v in result.values())
    return 0 if all_ok else 3


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="siifa_bulk_client")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_cons = sub.add_parser("consultar", help="Consulta masiva (paginada) de facturas")
    p_cons.add_argument("--nit-emisor", default="900243869")
    p_cons.add_argument("--nit-adquiriente", default=None)
    p_cons.add_argument("--numero-factura", default=None)
    p_cons.add_argument("--fecha-emision-inicio", default=None)
    p_cons.add_argument("--fecha-emision-final", default=None)
    p_cons.add_argument("--fecha-cargue", default=None)
    p_cons.add_argument("--tiene-radicado", default=None, choices=["true", "false"])
    p_cons.add_argument("--registros-por-pagina", type=int, default=1500)
    p_cons.add_argument("--salida", default=None, help="Archivo .jsonl para escribir resultados")
    p_cons.set_defaults(func=_cmd_consultar)

    p_rad = sub.add_parser("radicar-masivo", help="Radicado masivo de facturas (requiere rol ERP/Admin)")
    p_rad.add_argument("--entrada", required=True, help="Archivo .json con listaRadicado")
    p_rad.add_argument("--salida", default=None, help="Archivo .json para escribir respuesta")
    p_rad.set_defaults(func=_cmd_radicar_masivo)

    p_diag = sub.add_parser("diagnostico", help="Diagnóstico de conectividad contra SIIFA")
    p_diag.set_defaults(func=_cmd_diagnostico)

    args = parser.parse_args(argv)
    if args.cmd == "consultar" and args.tiene_radicado in ("true", "false"):
        args.tiene_radicado = args.tiene_radicado == "true"
    try:
        return int(args.func(args))
    except SiifaApiError as e:
        sys.stderr.write(str(e) + "\n")
        if e.payload is not None:
            try:
                sys.stderr.write(json.dumps(e.payload, ensure_ascii=False, indent=2) + "\n")
            except Exception:
                sys.stderr.write(f"payload: {e.payload!r}\n")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
