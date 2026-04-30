import argparse
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request


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


def _read_json_response(resp) -> object:
    raw = resp.read()
    if not raw:
        return None
    text = raw.decode("utf-8", errors="replace")
    return json.loads(text)


def _request_json(
    method: str,
    url: str,
    token: str | None = None,
    body: object | None = None,
    timeout_s: float = 60.0,
) -> object:
    headers = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    data = None
    if body is not None:
        headers["Content-Type"] = "application/json"
        data = json.dumps(body, ensure_ascii=False).encode("utf-8")

    req = urllib.request.Request(url, data=data, method=method.upper(), headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            return _read_json_response(resp)
    except urllib.error.HTTPError as e:
        try:
            payload = _read_json_response(e)
        except Exception:
            payload = None
        message = f"HTTP {e.code} al llamar {url}"
        raise SiifaApiError(message, status=e.code, payload=payload) from e
    except urllib.error.URLError as e:
        raise SiifaApiError(f"Error de red al llamar {url}: {e}") from e


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
        result = _request_json("POST", url, token=self.token, body=body, timeout_s=180.0)
        if not isinstance(result, list):
            raise SiifaApiError("Respuesta inesperada al radicar masivo", payload=result)
        return result

    def crear_radicado(self, id_factura: int, radicado: str, fecha_radicado: str) -> dict:
        if not self.token:
            raise ValueError("Debe autenticarse primero (token vacío).")
        url = _join_url(self.factura_base_url, "/api/FacturaRadicado")
        body = {"idFactura": int(id_factura), "radicado": radicado, "fechaRadicado": fecha_radicado}
        result = _request_json("POST", url, token=self.token, body=body, timeout_s=180.0)
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
        result = _request_json("POST", url, token=self.token, body=body, timeout_s=180.0)
        if not isinstance(result, list):
            raise SiifaApiError("Respuesta inesperada al crear devoluciones masivas", payload=result)
        return result

    def crear_devolucion(self, payload: dict) -> dict:
        if not self.token:
            raise ValueError("Debe autenticarse primero (token vacío).")
        url = _join_url(self.factura_base_url, "/api/SeguimientoFacturaDevolucion")
        result = _request_json("POST", url, token=self.token, body=payload, timeout_s=180.0)
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
        result = _request_json("POST", url, token=self.token, body=body, timeout_s=180.0)
        if not isinstance(result, list):
            raise SiifaApiError("Respuesta inesperada al crear glosas masivas", payload=result)
        return result

    def crear_glosa(self, payload: dict) -> dict:
        if not self.token:
            raise ValueError("Debe autenticarse primero (token vacío).")
        url = _join_url(self.factura_base_url, "/api/SeguimientoFacturaGlosa")
        result = _request_json("POST", url, token=self.token, body=payload, timeout_s=180.0)
        if not isinstance(result, dict):
            raise SiifaApiError("Respuesta inesperada al crear glosa", payload=result)
        return result

    def responder_glosa(self, payload: dict) -> dict:
        if not self.token:
            raise ValueError("Debe autenticarse primero (token vacío).")
        url = _join_url(self.factura_base_url, "/api/SeguimientoFacturaGlosa/Respuesta")
        result = _request_json("PUT", url, token=self.token, body=payload, timeout_s=180.0)
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
        result = _request_json("POST", url, token=self.token, body=body, timeout_s=180.0)
        if not isinstance(result, list):
            raise SiifaApiError("Respuesta inesperada al crear pagos masivos", payload=result)
        return result

    def crear_pago(self, payload: dict) -> dict:
        if not self.token:
            raise ValueError("Debe autenticarse primero (token vacío).")
        url = _join_url(self.factura_base_url, "/api/SeguimientoFacturaPago")
        result = _request_json("POST", url, token=self.token, body=payload, timeout_s=180.0)
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

    args = parser.parse_args(argv)
    if args.cmd == "consultar" and args.tiene_radicado in ("true", "false"):
        args.tiene_radicado = args.tiene_radicado == "true"
    try:
        return int(args.func(args))
    except SiifaApiError as e:
        sys.stderr.write(str(e) + "\n")
        if e.payload is not None:
            sys.stderr.write(json.dumps(e.payload, ensure_ascii=False, indent=2) + "\n")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

