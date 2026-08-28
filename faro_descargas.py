#!/usr/bin/env python3
"""
faro_descargas.py
-----------------
Recolecta descargas de Faro desde App Store (Apple) y Google Play, mantiene un
HISTÓRICO ACUMULADO local (faro_historico.csv) que crece con cada corrida, y a
partir de ese histórico produce:

  1) Un Google Sheet con varias pestañas (Resumen, Por mes, Por país, Datos).
     Se conserva la primera hoja con formato fecha/tienda/descargas para no
     romper el informe de Looker Studio que ya existe.
  2) Un dashboard HTML autónomo (faro_dashboard.html) con gráficas y tablas,
     sin dependencias externas (gráficas dibujadas en SVG). Se puede abrir en
     el celular o publicar en la web.
  3) La tabla por país y tienda en la terminal (como antes).

Por qué el histórico local:
    La API de Apple solo entrega ~365 días hacia atrás. Para tener el TOTAL
    "de siempre" guardamos cada corrida en faro_historico.csv y fusionamos
    (los datos nuevos reemplazan a los viejos para las mismas fechas, porque
    las tiendas revisan las cifras recientes). Así el acumulado nunca se pierde
    aunque la ventana de Apple avance.

Instalar dependencias (una sola vez):
    pip install requests PyJWT cryptography google-cloud-storage gspread google-auth pandas

Uso:
    python faro_descargas.py                 # incremental: refresca ~35 días y fusiona
    python faro_descargas.py --dias 365      # backfill máximo de Apple (córrelo UNA vez al inicio)
    python faro_descargas.py --desde 2026-01-01 --hasta 2026-06-17
    python faro_descargas.py --sin-sheet     # no toca Google Sheets (solo histórico + HTML)

Antes de correrlo: config.json debe existir (ya lo tienes).
Los secretos (.p8 de Apple y JSON del service account) se referencian por RUTA.
"""

import argparse
import datetime as dt
import gzip
import io
import json
import sys
import time
from pathlib import Path

import jwt  # PyJWT
import requests
import pandas as pd
from google.cloud import storage
from google.oauth2.service_account import Credentials
import gspread


# --------------------------------------------------------------------------
# Rutas de trabajo
# --------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "config.json"
HISTORICO_PATH = BASE_DIR / "faro_historico.csv"          # cache acumulado all-time
DASHBOARD_PATH = BASE_DIR / "faro_dashboard.html"          # dashboard autónomo (abrir en navegador)
DASHBOARD_BODY_PATH = BASE_DIR / "faro_dashboard_body.html"  # fragmento para publicar como página web

COLUMNS = ["fecha", "tienda", "país", "descargas"]


def cargar_config():
    if not CONFIG_PATH.exists():
        sys.exit(
            f"No encontré {CONFIG_PATH}.\n"
            "Copia config.example.json a config.json y rellénalo."
        )
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))


# --------------------------------------------------------------------------
# APPLE — App Store Connect: reporte SALES / SUMMARY (Sales and Trends)
# --------------------------------------------------------------------------
APPLE_API = "https://api.appstoreconnect.apple.com/v1/salesReports"


def apple_token(cfg):
    """Genera un JWT ES256 válido ~15 min para el App Store Connect API."""
    private_key = Path(cfg["apple"]["p8_path"]).read_text()
    now = int(time.time())
    payload = {
        "iss": cfg["apple"]["issuer_id"],
        "iat": now,
        "exp": now + 15 * 60,
        "aud": "appstoreconnect-v1",
    }
    headers = {"alg": "ES256", "kid": cfg["apple"]["key_id"], "typ": "JWT"}
    return jwt.encode(payload, private_key, algorithm="ES256", headers=headers)


def apple_descargas_dia(cfg, token, fecha):
    """Descargas de un día, desglosadas por país.

    Devuelve una lista de dicts {"país": <código ISO>, "descargas": <int>}.
    """
    params = {
        "filter[frequency]": "DAILY",
        "filter[reportType]": "SALES",
        "filter[reportSubType]": "SUMMARY",
        "filter[vendorNumber]": cfg["apple"]["vendor_number"],
        "filter[reportDate]": fecha.isoformat(),
        "filter[version]": "1_1",
    }
    headers = {"Authorization": f"Bearer {token}"}
    r = requests.get(APPLE_API, params=params, headers=headers, timeout=60)

    if r.status_code == 404:
        return []  # No hubo actividad ese día
    if r.status_code == 401:
        sys.exit("Apple devolvió 401: revisa Key ID, Issuer ID y el archivo .p8.")
    r.raise_for_status()

    # La respuesta es un .gz que contiene un TSV (separado por tabuladores)
    data = gzip.decompress(r.content).decode("utf-8")
    df = pd.read_csv(io.StringIO(data), sep="\t")

    if "Product Type Identifier" not in df.columns or "Units" not in df.columns:
        return []

    # Contar descargas de la app, excluyendo actualizaciones (empieza con 7)
    # y compras in-app (empieza con I).
    ptid = df["Product Type Identifier"].astype(str)
    es_descarga = ~ptid.str.startswith("7") & ~ptid.str.startswith("I")

    dd = df.loc[es_descarga]
    if dd.empty:
        return []

    col_pais = "Country Code" if "Country Code" in dd.columns else None
    if col_pais is None:
        return [{"país": "??", "descargas": int(dd["Units"].sum())}]

    g = dd.groupby(col_pais)["Units"].sum()
    return [{"país": str(k).strip().upper(), "descargas": int(v)} for k, v in g.items()]


def apple_rango(cfg, desde, hasta):
    """Itera día por día (Apple solo da DAILY de a un día por petición)."""
    token = apple_token(cfg)
    t0 = time.time()
    filas = []
    f = desde
    while f <= hasta:
        if time.time() - t0 > 12 * 60:  # refrescar el token antes de que expire
            token = apple_token(cfg)
            t0 = time.time()
        try:
            registros = apple_descargas_dia(cfg, token, f)
        except requests.HTTPError as e:
            print(f"  Apple {f}: error {e}", file=sys.stderr)
            registros = []
        for reg in registros:
            filas.append(
                {
                    "fecha": f.isoformat(),
                    "tienda": "App Store",
                    "país": reg["país"],
                    "descargas": reg["descargas"],
                }
            )
        f += dt.timedelta(days=1)
    return filas


# --------------------------------------------------------------------------
# GOOGLE PLAY — CSV de instalaciones desde el bucket de Cloud Storage
# --------------------------------------------------------------------------
def google_credenciales(cfg):
    scopes = [
        "https://www.googleapis.com/auth/devstorage.read_only",
        "https://www.googleapis.com/auth/spreadsheets",
    ]
    return Credentials.from_service_account_file(
        cfg["play"]["service_account_path"], scopes=scopes
    )


def play_descargas_rango(cfg, creds, desde, hasta):
    client = storage.Client(credentials=creds, project=creds.project_id)
    bucket = client.bucket(cfg["play"]["bucket_id"])
    paquete = cfg["play"]["package_name"]

    meses = []
    y, m = desde.year, desde.month
    while (y, m) <= (hasta.year, hasta.month):
        meses.append(f"{y:04d}{m:02d}")
        m = 1 if m == 12 else m + 1
        y = y + 1 if m == 1 else y

    filas = []
    for ym in meses:
        blob_path = f"stats/installs/installs_{paquete}_{ym}_country.csv"
        blob = bucket.blob(blob_path)
        if not blob.exists():
            continue
        texto = blob.download_as_bytes().decode("utf-16")
        df = pd.read_csv(io.StringIO(texto))

        col_inst = (
            "Daily User Installs"
            if "Daily User Installs" in df.columns
            else "Daily Device Installs"
        )
        col_pais = "Country" if "Country" in df.columns else None
        for _, row in df.iterrows():
            fecha = dt.date.fromisoformat(str(row["Date"]).strip())
            if desde <= fecha <= hasta:
                pais = str(row[col_pais]).strip().upper() if col_pais else "??"
                filas.append(
                    {
                        "fecha": fecha.isoformat(),
                        "tienda": "Google Play",
                        "país": pais,
                        "descargas": int(row[col_inst]),
                    }
                )
    return filas


# --------------------------------------------------------------------------
# Nombres de país (código ISO -> español). Fallback: el propio código.
# --------------------------------------------------------------------------
PAISES = {
    "MX": "México", "US": "Estados Unidos", "AR": "Argentina",
    "CO": "Colombia", "PE": "Perú", "CL": "Chile", "EC": "Ecuador",
    "GT": "Guatemala", "VE": "Venezuela", "BO": "Bolivia", "DO": "Rep. Dominicana",
    "CR": "Costa Rica", "PA": "Panamá", "UY": "Uruguay", "PY": "Paraguay",
    "SV": "El Salvador", "HN": "Honduras", "NI": "Nicaragua", "PR": "Puerto Rico",
    "BR": "Brasil", "CU": "Cuba", "GY": "Guyana", "SR": "Surinam", "BZ": "Belice",
    "ES": "España", "PT": "Portugal", "FR": "Francia", "IT": "Italia",
    "DE": "Alemania", "GB": "Reino Unido", "PL": "Polonia", "IE": "Irlanda",
    "NL": "Países Bajos", "BE": "Bélgica", "CH": "Suiza", "AT": "Austria",
    "SE": "Suecia", "NO": "Noruega", "DK": "Dinamarca", "FI": "Finlandia",
    "GR": "Grecia", "CZ": "Chequia", "RO": "Rumanía", "HU": "Hungría",
    "RU": "Rusia", "UA": "Ucrania", "TR": "Turquía",
    "ZA": "Sudáfrica", "KE": "Kenia", "CI": "Costa de Marfil",
    "CD": "R.D. del Congo", "NG": "Nigeria", "EG": "Egipto", "MA": "Marruecos",
    "CA": "Canadá", "AU": "Australia", "NZ": "Nueva Zelanda",
    "PH": "Filipinas", "JP": "Japón", "CN": "China", "KR": "Corea del Sur",
    "IN": "India", "ID": "Indonesia", "MY": "Malasia", "SG": "Singapur",
    "TH": "Tailandia", "VN": "Vietnam", "IL": "Israel", "LB": "Líbano",
    "AE": "Emiratos Árabes", "SA": "Arabia Saudita",
    "TT": "Trinidad y Tobago",
    "??": "(desconocido)",
}
ALIASES = {"UK": "GB", "EL": "GR", "ZZ": "??", "": "??", "NAN": "??", "NONE": "??"}


def normalizar_codigo(codigo):
    c = str(codigo).strip().upper()
    return ALIASES.get(c, c)


def nombre_pais(codigo):
    return PAISES.get(normalizar_codigo(codigo), codigo)


# --------------------------------------------------------------------------
# HISTÓRICO ACUMULADO (cache local que crece con cada corrida)
# --------------------------------------------------------------------------
def cargar_historico():
    if not HISTORICO_PATH.exists():
        return pd.DataFrame(columns=COLUMNS)
    df = pd.read_csv(HISTORICO_PATH, dtype={"país": str})
    for c in COLUMNS:
        if c not in df.columns:
            df[c] = pd.NA
    df["país"] = df["país"].apply(normalizar_codigo)
    df["descargas"] = pd.to_numeric(df["descargas"], errors="coerce").fillna(0).astype(int)
    return df[COLUMNS]


def fusionar_historico(historico, nuevo):
    """Los datos NUEVOS reemplazan a los viejos para la misma fecha+tienda+país
    (las tiendas revisan cifras recientes). El resto del histórico se conserva."""
    if nuevo.empty:
        combinado = historico.copy()
    elif historico.empty:
        combinado = nuevo.copy()
    else:
        combinado = pd.concat([historico, nuevo], ignore_index=True)
        # keep='last' -> se queda con la fila NUEVA cuando hay choque
        combinado = combinado.drop_duplicates(
            subset=["fecha", "tienda", "país"], keep="last"
        )
    combinado = combinado.sort_values(["fecha", "tienda", "país"]).reset_index(drop=True)
    return combinado


def guardar_historico(df):
    df.to_csv(HISTORICO_PATH, index=False, encoding="utf-8")


# --------------------------------------------------------------------------
# BREAKDOWNS (todo se calcula sobre el histórico completo = acumulado real)
# --------------------------------------------------------------------------
def tabla_por_mes(df):
    """mes (YYYY-MM) x tienda, con Total y Acumulado."""
    d = df.copy()
    d["mes"] = d["fecha"].str.slice(0, 7)
    piv = d.groupby(["mes", "tienda"])["descargas"].sum().unstack(fill_value=0)
    for t in ("App Store", "Google Play"):
        if t not in piv.columns:
            piv[t] = 0
    piv = piv[["App Store", "Google Play"]]
    piv["Total"] = piv.sum(axis=1)
    piv = piv.sort_index()
    piv["Acumulado"] = piv["Total"].cumsum()
    return piv.reset_index()


def tabla_por_pais(df):
    """país x tienda, con Total, ordenado desc."""
    piv = df.groupby(["país", "tienda"])["descargas"].sum().unstack(fill_value=0)
    for t in ("App Store", "Google Play"):
        if t not in piv.columns:
            piv[t] = 0
    piv = piv[["App Store", "Google Play"]]
    piv["Total"] = piv.sum(axis=1)
    piv = piv.sort_values("Total", ascending=False)
    piv = piv.reset_index()
    piv.insert(1, "país_nombre", piv["país"].apply(nombre_pais))
    return piv


def resumen_kpis(df):
    total = int(df["descargas"].sum())
    por_tienda = df.groupby("tienda")["descargas"].sum().to_dict()
    fechas = sorted(df["fecha"].unique())
    return {
        "total": total,
        "app_store": int(por_tienda.get("App Store", 0)),
        "google_play": int(por_tienda.get("Google Play", 0)),
        "paises": int(df["país"].nunique()),
        "desde": fechas[0] if fechas else "-",
        "hasta": fechas[-1] if fechas else "-",
    }


# --------------------------------------------------------------------------
# GOOGLE SHEETS — varias pestañas
# --------------------------------------------------------------------------
def _ws(sh, titulo, filas, cols):
    """Obtiene o crea una pestaña y la reescribe."""
    try:
        ws = sh.worksheet(titulo)
        ws.clear()
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=titulo, rows=max(filas, 10), cols=max(cols, 4))
    return ws


def escribir_sheets(cfg, creds, df, por_mes, por_pais, kpis):
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(cfg["sheet"]["id"])

    # 1) Primera hoja: se mantiene el formato que usa Looker (fecha, tienda, descargas)
    base = (
        df.groupby(["fecha", "tienda"], as_index=False)["descargas"]
        .sum()
        .sort_values(["fecha", "tienda"])
    )
    ws0 = sh.sheet1
    ws0.clear()
    ws0.update(
        [["fecha", "tienda", "descargas"]]
        + [[r["fecha"], r["tienda"], int(r["descargas"])] for r in base.to_dict("records")]
    )

    # 2) Resumen
    ws = _ws(sh, "Resumen", 12, 2)
    ws.update([
        ["Métrica", "Valor"],
        ["Total acumulado", kpis["total"]],
        ["App Store", kpis["app_store"]],
        ["Google Play", kpis["google_play"]],
        ["Países", kpis["paises"]],
        ["Desde", kpis["desde"]],
        ["Hasta", kpis["hasta"]],
        ["Actualizado", dt.datetime.now().strftime("%Y-%m-%d %H:%M")],
    ])

    # 3) Por mes
    pm = por_mes.copy()
    ws = _ws(sh, "Por mes", len(pm) + 2, 5)
    ws.update(
        [["mes", "App Store", "Google Play", "Total", "Acumulado"]]
        + [[r["mes"], int(r["App Store"]), int(r["Google Play"]),
            int(r["Total"]), int(r["Acumulado"])] for r in pm.to_dict("records")]
    )

    # 4) Por país
    pp = por_pais.copy()
    ws = _ws(sh, "Por país", len(pp) + 2, 5)
    ws.update(
        [["código", "país", "App Store", "Google Play", "Total"]]
        + [[r["país"], r["país_nombre"], int(r["App Store"]),
            int(r["Google Play"]), int(r["Total"])] for r in pp.to_dict("records")]
    )

    # 5) Datos (histórico completo, por si Looker quiere desglosar por país/fecha)
    ws = _ws(sh, "Datos", len(df) + 2, 4)
    ws.update(
        [["fecha", "tienda", "país", "descargas"]]
        + [[r["fecha"], r["tienda"], r["país"], int(r["descargas"])]
           for r in df.to_dict("records")]
    )


# --------------------------------------------------------------------------
# DASHBOARD HTML AUTÓNOMO (gráficas en SVG, sin dependencias)
# --------------------------------------------------------------------------
def _fmt(n):
    return f"{int(n):,}".replace(",", " ")


def _svg_barras_mes(por_mes, w=760, h=280):
    pm = por_mes.tail(18)  # últimos 18 meses para que quepa
    meses = list(pm["mes"])
    apple = list(pm["App Store"].astype(int))
    play = list(pm["Google Play"].astype(int))
    tot = [a + p for a, p in zip(apple, play)]
    maxv = max(tot) if tot and max(tot) > 0 else 1
    n = max(len(meses), 1)
    pad_l, pad_b, pad_t = 44, 42, 14
    gw = w - pad_l - 10
    gh = h - pad_b - pad_t
    bw = gw / n * 0.62
    step = gw / n
    barras = []
    etiquetas = []
    for i, m in enumerate(meses):
        x = pad_l + step * i + (step - bw) / 2
        ha = gh * apple[i] / maxv
        hp = gh * play[i] / maxv
        y_apple = pad_t + gh - ha
        y_play = y_apple - hp
        barras.append(
            f'<rect x="{x:.1f}" y="{y_apple:.1f}" width="{bw:.1f}" height="{ha:.1f}" fill="#0A84FF"><title>{m} · App Store: {apple[i]}</title></rect>'
        )
        barras.append(
            f'<rect x="{x:.1f}" y="{y_play:.1f}" width="{bw:.1f}" height="{hp:.1f}" fill="#34C759"><title>{m} · Google Play: {play[i]}</title></rect>'
        )
        etiquetas.append(
            f'<text x="{x + bw/2:.1f}" y="{h - pad_b + 16:.1f}" text-anchor="middle" class="tick">{m[2:]}</text>'
        )
    # eje Y (3 marcas)
    ejes = []
    for k in range(4):
        val = maxv * k / 3
        y = pad_t + gh - gh * k / 3
        ejes.append(f'<line x1="{pad_l}" y1="{y:.1f}" x2="{w-10}" y2="{y:.1f}" class="grid"/>')
        ejes.append(f'<text x="{pad_l-6}" y="{y+4:.1f}" text-anchor="end" class="tick">{_fmt(val)}</text>')
    return (
        f'<svg viewBox="0 0 {w} {h}" class="chart" role="img">'
        + "".join(ejes) + "".join(barras) + "".join(etiquetas) + "</svg>"
    )


def _svg_linea_acumulado(por_mes, w=760, h=240):
    pm = por_mes
    meses = list(pm["mes"])
    acum = list(pm["Acumulado"].astype(int))
    if not acum:
        return '<svg viewBox="0 0 760 240" class="chart"></svg>'
    maxv = max(acum) if max(acum) > 0 else 1
    n = max(len(meses), 1)
    pad_l, pad_b, pad_t = 52, 42, 14
    gw = w - pad_l - 10
    gh = h - pad_b - pad_t
    pts = []
    for i, v in enumerate(acum):
        x = pad_l + (gw * i / (n - 1) if n > 1 else gw / 2)
        y = pad_t + gh - gh * v / maxv
        pts.append((x, y))
    linea = " ".join(f"{x:.1f},{y:.1f}" for x, y in pts)
    area = f"M{pad_l},{pad_t+gh} " + " ".join(f"L{x:.1f},{y:.1f}" for x, y in pts) + f" L{pts[-1][0]:.1f},{pad_t+gh} Z"
    puntos = "".join(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="3" fill="#0A84FF"><title>{meses[i]}: {acum[i]}</title></circle>' for i, (x, y) in enumerate(pts))
    ejes = []
    for k in range(4):
        val = maxv * k / 3
        y = pad_t + gh - gh * k / 3
        ejes.append(f'<line x1="{pad_l}" y1="{y:.1f}" x2="{w-10}" y2="{y:.1f}" class="grid"/>')
        ejes.append(f'<text x="{pad_l-6}" y="{y+4:.1f}" text-anchor="end" class="tick">{_fmt(val)}</text>')
    # algunas etiquetas de mes en X
    xticks = []
    paso = max(1, n // 8)
    for i in range(0, n, paso):
        x = pad_l + (gw * i / (n - 1) if n > 1 else gw / 2)
        xticks.append(f'<text x="{x:.1f}" y="{h-pad_b+16:.1f}" text-anchor="middle" class="tick">{meses[i][2:]}</text>')
    return (
        f'<svg viewBox="0 0 {w} {h}" class="chart" role="img">'
        + "".join(ejes)
        + f'<path d="{area}" fill="rgba(10,132,255,.12)"/>'
        + f'<polyline points="{linea}" fill="none" stroke="#0A84FF" stroke-width="2.5"/>'
        + puntos + "".join(xticks) + "</svg>"
    )


def _svg_tiendas(kpis, w=240, h=240):
    a, p = kpis["app_store"], kpis["google_play"]
    tot = a + p if (a + p) > 0 else 1
    fa = a / tot
    import math
    cx, cy, r = 120, 120, 78
    ang0 = -math.pi / 2
    ang1 = ang0 + 2 * math.pi * fa
    def punto(ang):
        return cx + r * math.cos(ang), cy + r * math.sin(ang)
    x0, y0 = punto(ang0)
    x1, y1 = punto(ang1)
    large = 1 if fa > 0.5 else 0
    arco_apple = f'M{cx},{cy} L{x0:.1f},{y0:.1f} A{r},{r} 0 {large} 1 {x1:.1f},{y1:.1f} Z'
    large2 = 1 if (1 - fa) > 0.5 else 0
    arco_play = f'M{cx},{cy} L{x1:.1f},{y1:.1f} A{r},{r} 0 {large2} 1 {x0:.1f},{y0:.1f} Z'
    return (
        f'<svg viewBox="0 0 {w} {h}" class="chart" role="img">'
        f'<path d="{arco_apple}" fill="#0A84FF"><title>App Store: {a}</title></path>'
        f'<path d="{arco_play}" fill="#34C759"><title>Google Play: {p}</title></path>'
        f'<circle cx="{cx}" cy="{cy}" r="46" fill="var(--card)"/>'
        f'<text x="{cx}" y="{cy-4}" text-anchor="middle" class="donut-num">{_fmt(tot)}</text>'
        f'<text x="{cx}" y="{cy+16}" text-anchor="middle" class="donut-lbl">total</text>'
        f'</svg>'
    )


def generar_dashboard(df, por_mes, por_pais, kpis):
    actualizado = dt.datetime.now().strftime("%d/%m/%Y %H:%M")
    filas_pais = "\n".join(
        f'<tr><td>{r["país_nombre"]}</td><td class="num">{_fmt(r["App Store"])}</td>'
        f'<td class="num">{_fmt(r["Google Play"])}</td><td class="num b">{_fmt(r["Total"])}</td></tr>'
        for r in por_pais.to_dict("records")
    )
    pm_rev = por_mes.iloc[::-1]
    filas_mes = "\n".join(
        f'<tr><td>{r["mes"]}</td><td class="num">{_fmt(r["App Store"])}</td>'
        f'<td class="num">{_fmt(r["Google Play"])}</td><td class="num b">{_fmt(r["Total"])}</td>'
        f'<td class="num">{_fmt(r["Acumulado"])}</td></tr>'
        for r in pm_rev.to_dict("records")
    )
    # Se devuelve un FRAGMENTO (estilos + contenido dentro de .faro), sin
    # <html>/<head>/<body>. Así el mismo bloque sirve para: (a) envolverse en
    # una página autónoma (faro_dashboard.html) y (b) publicarse tal cual como
    # página web/Artifact (faro_dashboard_body.html), que aporta su propio esqueleto.
    return f"""<style>
  :root {{
    --bg:#f2f3f7; --card:#ffffff; --ink:#111418; --muted:#6b7280;
    --line:#e6e8ee; --apple:#0A84FF; --play:#34C759;
  }}
  @media (prefers-color-scheme: dark) {{
    :root {{ --bg:#0e1116; --card:#171b22; --ink:#eceef2; --muted:#9aa3af; --line:#262b34; }}
  }}
  * {{ box-sizing:border-box; }}
  .faro {{ background:var(--bg); color:var(--ink);
    font:15px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;
    -webkit-font-smoothing:antialiased; padding:16px; max-width:900px; margin:0 auto; }}
  header {{ display:flex; align-items:baseline; justify-content:space-between; flex-wrap:wrap; gap:8px; margin:6px 2px 18px; }}
  h1 {{ font-size:22px; margin:0; letter-spacing:-.02em; }}
  .upd {{ color:var(--muted); font-size:12px; }}
  .kpis {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(150px,1fr)); gap:12px; margin-bottom:18px; }}
  .kpi {{ background:var(--card); border:1px solid var(--line); border-radius:16px; padding:16px 18px; }}
  .kpi .n {{ font-size:26px; font-weight:700; letter-spacing:-.02em; }}
  .kpi .l {{ color:var(--muted); font-size:13px; margin-top:2px; }}
  .kpi.apple .n {{ color:var(--apple); }} .kpi.play .n {{ color:var(--play); }}
  .card {{ background:var(--card); border:1px solid var(--line); border-radius:16px; padding:16px 18px; margin-bottom:16px; }}
  .card h2 {{ font-size:15px; margin:0 0 10px; font-weight:650; }}
  .row {{ display:grid; grid-template-columns:1.6fr 1fr; gap:16px; }}
  @media (max-width:640px) {{ .row {{ grid-template-columns:1fr; }} }}
  .chart {{ width:100%; height:auto; display:block; }}
  .legend {{ display:flex; gap:16px; font-size:12px; color:var(--muted); margin-top:8px; }}
  .dot {{ display:inline-block; width:9px; height:9px; border-radius:50%; margin-right:5px; vertical-align:middle; }}
  .grid {{ stroke:var(--line); stroke-width:1; }}
  .tick {{ fill:var(--muted); font-size:10px; }}
  .donut-num {{ fill:var(--ink); font-size:20px; font-weight:700; }}
  .donut-lbl {{ fill:var(--muted); font-size:11px; }}
  table {{ width:100%; border-collapse:collapse; font-size:13.5px; }}
  th, td {{ text-align:left; padding:8px 6px; border-bottom:1px solid var(--line); }}
  th {{ color:var(--muted); font-weight:600; font-size:12px; text-transform:uppercase; letter-spacing:.03em; }}
  td.num {{ text-align:right; font-variant-numeric:tabular-nums; }}
  td.b {{ font-weight:700; }}
  .scroll {{ max-height:420px; overflow:auto; }}
</style>
<div class="faro">
  <header>
    <h1>Faro · Descargas</h1>
    <span class="upd">Actualizado: {actualizado} · {kpis["desde"]} → {kpis["hasta"]}</span>
  </header>

  <section class="kpis">
    <div class="kpi"><div class="n">{_fmt(kpis["total"])}</div><div class="l">Total acumulado</div></div>
    <div class="kpi apple"><div class="n">{_fmt(kpis["app_store"])}</div><div class="l">App Store</div></div>
    <div class="kpi play"><div class="n">{_fmt(kpis["google_play"])}</div><div class="l">Google Play</div></div>
    <div class="kpi"><div class="n">{kpis["paises"]}</div><div class="l">Países</div></div>
  </section>

  <div class="row">
    <div class="card">
      <h2>Descargas por mes</h2>
      {_svg_barras_mes(por_mes)}
      <div class="legend"><span><span class="dot" style="background:var(--apple)"></span>App Store</span>
      <span><span class="dot" style="background:var(--play)"></span>Google Play</span></div>
    </div>
    <div class="card">
      <h2>Reparto por tienda</h2>
      {_svg_tiendas(kpis)}
      <div class="legend"><span><span class="dot" style="background:var(--apple)"></span>App Store</span>
      <span><span class="dot" style="background:var(--play)"></span>Google Play</span></div>
    </div>
  </div>

  <div class="card">
    <h2>Acumulado en el tiempo</h2>
    {_svg_linea_acumulado(por_mes)}
  </div>

  <div class="card">
    <h2>Por mes</h2>
    <div class="scroll"><table>
      <thead><tr><th>Mes</th><th class="num">App Store</th><th class="num">Google Play</th><th class="num">Total</th><th class="num">Acumulado</th></tr></thead>
      <tbody>{filas_mes}</tbody>
    </table></div>
  </div>

  <div class="card">
    <h2>Por país</h2>
    <div class="scroll"><table>
      <thead><tr><th>País</th><th class="num">App Store</th><th class="num">Google Play</th><th class="num">Total</th></tr></thead>
      <tbody>{filas_pais}</tbody>
    </table></div>
  </div>
</div>"""


def envolver_pagina(cuerpo):
    """Envuelve el fragmento en una página HTML autónoma (para abrir en el navegador)."""
    return (
        '<!doctype html>\n<html lang="es">\n<head>\n<meta charset="utf-8">\n'
        '<meta name="viewport" content="width=device-width, initial-scale=1">\n'
        '<meta name="apple-mobile-web-app-capable" content="yes">\n'
        '<meta name="apple-mobile-web-app-title" content="Faro">\n'
        '<meta name="theme-color" content="#0A84FF">\n'
        '<title>Faro · Descargas</title>\n</head>\n<body style="margin:0">\n'
        + cuerpo +
        '\n</body>\n</html>'
    )


# --------------------------------------------------------------------------
# Tabla por tienda y país en la terminal (como antes)
# --------------------------------------------------------------------------
def imprimir_tabla_pais(por_pais, kpis):
    print("\n" + "=" * 60)
    print("DESCARGAS POR PAÍS Y TIENDA (acumulado histórico)")
    print("=" * 60)
    ancho = max([len(r["país_nombre"]) for r in por_pais.to_dict("records")] + [6])
    enc = "País".ljust(ancho) + "  " + "App Store".rjust(10) + "  " + "Google Play".rjust(12) + "  " + "TOTAL".rjust(10)
    print(enc)
    print("-" * len(enc))
    for r in por_pais.to_dict("records"):
        print(r["país_nombre"].ljust(ancho) + "  " +
              f'{int(r["App Store"]):,}'.rjust(10) + "  " +
              f'{int(r["Google Play"]):,}'.rjust(12) + "  " +
              f'{int(r["Total"]):,}'.rjust(10))
    print("-" * len(enc))
    print("TOTAL".ljust(ancho) + "  " +
          f'{kpis["app_store"]:,}'.rjust(10) + "  " +
          f'{kpis["google_play"]:,}'.rjust(12) + "  " +
          f'{kpis["total"]:,}'.rjust(10))


# --------------------------------------------------------------------------
# MAIN
# --------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="Descargas de Faro: App Store + Google Play (histórico acumulado)")
    ap.add_argument("--dias", type=int, default=35,
                    help="Refrescar los últimos N días y fusionarlos al histórico (por defecto 35)")
    ap.add_argument("--desde", help="Fecha inicio YYYY-MM-DD (para backfill puntual)")
    ap.add_argument("--hasta", help="Fecha fin YYYY-MM-DD")
    ap.add_argument("--sin-sheet", action="store_true", help="No escribir en Google Sheets")
    args = ap.parse_args()

    hoy = dt.date.today()
    if args.desde and args.hasta:
        desde = dt.date.fromisoformat(args.desde)
        hasta = dt.date.fromisoformat(args.hasta)
    else:
        hasta = hoy - dt.timedelta(days=1)  # ayer: las tiendas publican con retraso
        desde = hasta - dt.timedelta(days=args.dias - 1)

    cfg = cargar_config()
    print(f"Ventana de refresco: {desde} -> {hasta}")

    print("Descargando datos de App Store...")
    apple = apple_rango(cfg, desde, hasta)

    print("Descargando datos de Google Play...")
    creds = google_credenciales(cfg)
    try:
        play = play_descargas_rango(cfg, creds, desde, hasta)
    except Exception as e:
        print(f"  Google Play no disponible aún: {e}", file=sys.stderr)
        play = []

    nuevo = pd.DataFrame(apple + play, columns=COLUMNS) if (apple or play) else pd.DataFrame(columns=COLUMNS)
    if not nuevo.empty:
        nuevo["país"] = nuevo["país"].apply(normalizar_codigo)
        nuevo["descargas"] = nuevo["descargas"].astype(int)

    # Fusionar con el histórico acumulado
    historico = cargar_historico()
    df = fusionar_historico(historico, nuevo)
    if df.empty:
        sys.exit("No hay datos (ni nuevos ni en el histórico). Revisa credenciales.")
    guardar_historico(df)
    print(f"Histórico: {len(df)} filas guardadas en {HISTORICO_PATH.name}")

    # Breakdowns sobre el histórico completo
    por_mes = tabla_por_mes(df)
    por_pais = tabla_por_pais(df)
    kpis = resumen_kpis(df)

    # Google Sheets
    if not args.sin_sheet:
        try:
            print("Actualizando Google Sheets (Resumen, Por mes, Por país, Datos)...")
            escribir_sheets(cfg, creds, df, por_mes, por_pais, kpis)
            print("  Sheet actualizado. Refresca Looker Studio si lo usas.")
        except Exception as e:
            print(f"  No se pudo actualizar el Sheet: {e}", file=sys.stderr)

    # Dashboard HTML (dos versiones: autónoma para el navegador y fragmento para publicar)
    cuerpo = generar_dashboard(df, por_mes, por_pais, kpis)
    DASHBOARD_PATH.write_text(envolver_pagina(cuerpo), encoding="utf-8")
    DASHBOARD_BODY_PATH.write_text(cuerpo, encoding="utf-8")
    print(f"Dashboard generado: {DASHBOARD_PATH.name} (+ {DASHBOARD_BODY_PATH.name})")

    # Terminal
    imprimir_tabla_pais(por_pais, kpis)
    print(f"\nTOTAL ACUMULADO: {kpis['total']:,}  "
          f"(App Store {kpis['app_store']:,} · Google Play {kpis['google_play']:,})")


if __name__ == "__main__":
    main()
