#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Unificador de hojas Excel
· Tipos de planilla: 'lp' (por defecto) y 'simbiu'
· Para 'simbiu' se exige XML y se agrega la columna 'texto'
Autor: Esteban · Jun-2025 · MIT
"""

from __future__ import annotations

import io
import json
import os
import re
import unicodedata
import xml.etree.ElementTree as ET
from datetime import timedelta
from pathlib import Path
from typing import Dict, List

import openpyxl
import pandas as pd
from flask import (
    Flask,
    flash,
    redirect,
    render_template_string,
    request,
    send_file,
    session,
    url_for,
)
from flask_session import Session

# ────────────────────── Configuración Flask ──────────────────────────
app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET", "cambia_esta_clave_supersecreta")

app.config["SESSION_TYPE"] = "filesystem"
app.config["SESSION_FILE_DIR"] = Path("flask_session").resolve()
app.config["SESSION_PERMANENT"] = False
Session(app)

app.permanent_session_lifetime = timedelta(hours=3)

MAPPINGS_DIR = Path("mappings")
MAPPINGS_DIR.mkdir(exist_ok=True)

# ───────────────────────── Utilidades base ───────────────────────────
def canonicalize(text: str) -> str:
    """Minúsculas sin tildes ni símbolos; separa con '_'."""
    text = text.strip().lower()
    text = "".join(
        c for c in unicodedata.normalize("NFKD", text) if not unicodedata.combining(c)
    )
    text = re.sub(r"[^a-z0-9]", "_", text)
    return re.sub(r"__+", "_", text).strip("_")


def slugify(name: str) -> str:
    return canonicalize(name) or "default"


def workbook_columns(wb: openpyxl.Workbook) -> Dict[str, List[str]]:
    """Devuelve los encabezados de cada hoja."""
    return {
        ws.title: [
            c.value or f"col_{i+1}" for i, c in enumerate(next(ws.iter_rows(max_row=1)))
        ]
        for ws in wb.worksheets
    }


def list_mappings() -> List[str]:
    """Lista los tipos de planilla disponibles."""
    defaults = {"lp", "simbiu"}
    found = {p.stem for p in MAPPINGS_DIR.glob("*.json")}
    return sorted(defaults | found)


def save_mapping(name: str, mapping: Dict[str, str]) -> None:
    (MAPPINGS_DIR / f"{slugify(name)}.json").write_text(
        json.dumps(mapping, ensure_ascii=False, indent=2)
    )


def load_mapping(name: str) -> Dict[str, str]:
    p = MAPPINGS_DIR / f"{slugify(name)}.json"
    return json.loads(p.read_text()) if p.exists() else {}

# ───────────────────────── Texto (flujo simbiu) ──────────────────────
CODE_RE = re.compile(r"/(?:index/1|VerNoticia)/(\d+)")


def build_text_df(xml_bytes: bytes) -> pd.DataFrame:
    """Extrae código y texto completo del XML."""
    root = ET.parse(io.BytesIO(xml_bytes)).getroot()
    rows = []
    for n in root.findall(".//noticia"):
        url = n.findtext("Url_Noticia", "")
        m = CODE_RE.search(url)
        if m:
            rows.append(
                {
                    "codigo": m.group(1),
                    "texto": (n.findtext("FullText", "") or "").strip(),
                }
            )
    return pd.DataFrame(rows).drop_duplicates("codigo")


def add_text_column(df: pd.DataFrame, xml_bytes: bytes) -> pd.DataFrame:
    """Une el texto del XML a la planilla (impresos / digitales)."""
    if not xml_bytes:
        return df

    # localizar la columna con el enlace (case-insensitive)
    col_url = next(
        (c for c in df.columns if canonicalize(c) == "url_noticia"), None
    )
    if col_url is None:
        return df

    text_df = build_text_df(xml_bytes)
    df["codigo_link"] = df[col_url].astype(str).str.extract(CODE_RE, expand=False)
    df = df.merge(text_df, left_on="codigo_link", right_on="codigo", how="left")
    df.drop(columns=["codigo_link", "codigo"], inplace=True)
    return df

# ───────────────────────── Unificación de hojas ──────────────────────
def unify_workbook(xlsx_bytes: bytes, mapping: Dict[str, str]) -> pd.DataFrame:
    """
    Une todas las hojas del Excel normalizando encabezados.
    Devuelve DataFrame; los hipervínculos quedan como texto visible.
    """
    wb_in = openpyxl.load_workbook(io.BytesIO(xlsx_bytes), data_only=False)
    all_rows: List[List] = []
    headers_final: List[str] = []

    # ---- determinar encabezados finales --------------------------------
    for ws in wb_in.worksheets:
        for cell in next(ws.iter_rows(max_row=1)):
            orig = cell.value or ""
            dest = mapping.get(orig, mapping.get(canonicalize(orig), canonicalize(orig)))
            if dest not in headers_final:
                headers_final.append(dest)

    # ---- recorrer filas y re-mapear ------------------------------------
    for ws in wb_in.worksheets:
        header_src = [c.value or "" for c in next(ws.iter_rows(max_row=1))]
        idx2dest = {
            idx: mapping.get(orig, mapping.get(canonicalize(orig), canonicalize(orig)))
            for idx, orig in enumerate(header_src)
        }
        for row in ws.iter_rows(min_row=2):
            new_row = [""] * len(headers_final)
            for idx, cell in enumerate(row):
                dest = idx2dest.get(idx)
                if not dest:
                    continue
                new_row[headers_final.index(dest)] = cell.value
            all_rows.append(new_row)

    df = pd.DataFrame(all_rows, columns=headers_final)
    return df

# ────────────────────────────── Rutas ────────────────────────────────
@app.route("/", methods=["GET", "POST"])
def home():
    """
    Menú inicial: seleccionar tipo de planilla, subir Excel (.xlsx)
    y, si corresponde, XML.
    """
    mappings = list_mappings()

    if request.method == "POST":
        mapping_choice = request.form.get("mapping_choice", "lp").strip().lower()
        f_xlsx = request.files.get("file_xlsx")
        f_xml = request.files.get("file_xml")

        # Validaciones
        if not (f_xlsx and f_xlsx.filename.lower().endswith(".xlsx")):
            flash("Sube un archivo Excel (.xlsx) válido.", "danger")
            return redirect(url_for("home"))

        if mapping_choice == "simbiu":
            if not (f_xml and f_xml.filename.lower().endswith(".xml")):
                flash("Para planillas Simbiu debes subir también el XML.", "danger")
                return redirect(url_for("home"))
            session["file_xml"] = f_xml.read()

        # Guardar en sesión
        session["file_xlsx"] = f_xlsx.read()
        session["mapping_name"] = mapping_choice

        wb = openpyxl.load_workbook(io.BytesIO(session["file_xlsx"]), read_only=True)
        session["cols_per_sheet"] = workbook_columns(wb)

        return redirect(url_for("mapping"))

    return render_template_string(TPL_HOME, mappings=mappings)


@app.route("/mapping", methods=["GET", "POST"])
def mapping():
    cols_per_sheet = session.get("cols_per_sheet")
    mapping_name = session.get("mapping_name", "lp")

    if not cols_per_sheet:
        return redirect(url_for("home"))

    if request.method == "POST":
        # Equivalencias capturadas
        mapping = {k: v.strip() for k, v in request.form.items() if v.strip()}
        save_mapping(mapping_name, mapping)

        # Unificación
        merged = unify_workbook(session["file_xlsx"], mapping)

        # Añadir texto si es Simbiu
        if mapping_name == "simbiu":
            merged = add_text_column(merged, session.get("file_xml", b""))

        out = io.BytesIO()
        merged.to_excel(out, index=False)
        out.seek(0)

        session.clear()  # limpiamos todo
        return send_file(
            out,
            as_attachment=True,
            download_name=f"unificado_{slugify(mapping_name)}.xlsx",
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    return render_template_string(
        TPL_MAPPING,
        mapping_name=mapping_name,
        cols_per_sheet=cols_per_sheet,
        existing_mapping=load_mapping(mapping_name),
        canonicalize=canonicalize,
    )

# ────────────────────────── Plantillas HTML ──────────────────────────
TPL_HOME = """
<!doctype html>
<title>Unificar Excel · Menú</title>
<link rel="stylesheet"
 href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css">

<div class="container py-5">
  <h1 class="mb-4">Unificador de Hojas Excel</h1>

  {% with m=get_flashed_messages(with_categories=true) %}
    {% if m %}<div class="alert alert-{{ m[0][0] }}">{{ m[0][1] }}</div>{% endif %}
  {% endwith %}

  <form method="POST" enctype="multipart/form-data" class="border p-4 rounded">
    <!-- Tipo de planilla -->
    <div class="mb-3">
      <label class="form-label">Tipo de planilla</label>
      <select id="mapping_choice" name="mapping_choice"
              class="form-select" onchange="toggleXml()">
        {% for m in mappings %}<option value="{{ m|lower }}">{{ m }}</option>{% endfor %}
      </select>
    </div>

    <!-- Excel -->
    <div class="mb-3">
      <label class="form-label">Archivo Excel (.xlsx)</label>
      <input class="form-control" type="file" name="file_xlsx" accept=".xlsx" required>
    </div>

    <!-- XML (solo Simbiu) -->
    <div class="mb-3" id="xml_div" style="display:none">
      <label class="form-label">Archivo XML</label>
      <input class="form-control" type="file" name="file_xml" accept=".xml">
    </div>

    <button class="btn btn-primary">Continuar</button>
  </form>
</div>

<script>
function toggleXml(){
  const sel = document.getElementById('mapping_choice');
  document.getElementById('xml_div').style.display =
       sel.value.toLowerCase() === 'simbiu' ? 'block' : 'none';
}
toggleXml();
</script>
"""

TPL_MAPPING = """
<!doctype html>
<title>Unificar Excel · {{ mapping_name }}</title>
<link rel="stylesheet"
 href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css">

<div class="container py-4">
  <a href="{{ url_for('home') }}" class="btn btn-link mb-3">← Menú</a>
  <h2 class="mb-4">Configuración: {{ mapping_name }}</h2>

  <form method="POST">
    {% for sheet, cols in cols_per_sheet.items() %}
      <h4 class="mt-4">{{ sheet }}</h4>
      <table class="table table-sm align-middle">
        <thead><tr><th>Columna original</th><th>Nombre final</th></tr></thead>
        <tbody>
        {% for col in cols %}
          {% set canon = canonicalize(col) %}
          {% set pre = existing_mapping.get(col, existing_mapping.get(canon, canon)) %}
          <tr>
            <td>{{ col }}</td>
            <td><input class="form-control form-control-sm" name="{{ col }}" value="{{ pre }}"></td>
          </tr>
        {% endfor %}
        </tbody>
      </table>
    {% endfor %}
    <div class="d-flex gap-3">
      <button class="btn btn-success">Unificar y descargar</button>
      <a href="{{ url_for('home') }}" class="btn btn-secondary">Cancelar</a>
    </div>
  </form>
</div>
"""

# ─────────────────────────── Lanzador ────────────────────────────────
if __name__ == "__main__":
    app.run(debug=True, port=5000)
