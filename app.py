
import os
import io
import uuid
import json
from datetime import datetime
from dateutil import parser as dateparser

from flask import Flask, render_template, request, redirect, url_for, send_file, flash
import pandas as pd

APP_ROOT = os.path.dirname(os.path.abspath(__file__))
UPLOAD_DIR = os.path.join(APP_ROOT, 'uploads')
EXPORT_DIR = os.path.join(APP_ROOT, 'exports')

os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(EXPORT_DIR, exist_ok=True)

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'dev-secret')


# ----------------------
# Utils
# ----------------------

def _is_excel(filename: str):
    return filename.lower().endswith(('.xlsx', '.xls', '.xlsm'))


def _is_csv(filename: str):
    return filename.lower().endswith('.csv')


def _save_upload(file_storage):
    file_id = str(uuid.uuid4())
    ext = os.path.splitext(file_storage.filename)[1]
    safe_name = f"{file_id}{ext}"
    path = os.path.join(UPLOAD_DIR, safe_name)
    file_storage.save(path)
    return file_id, path, safe_name


def _list_sheets(path: str):
    if _is_excel(path):
        try:
            xl = pd.ExcelFile(path)
            return xl.sheet_names
        except Exception:
            return []
    return [None]  # CSV não tem abas


def _load_df(path: str, sheet: str | None):
    if _is_excel(path):
        df = pd.read_excel(path, sheet_name=sheet)
    elif _is_csv(path):
        df = pd.read_csv(path)
    else:
        raise ValueError('Formato de arquivo não suportado. Use Excel (.xlsx) ou CSV.')
    return df


def _infer_dtype(series: pd.Series) -> str:
    if pd.api.types.is_datetime64_any_dtype(series):
        return 'datetime'
    if pd.api.types.is_numeric_dtype(series):
        return 'number'
    return 'string'


def _coerce_dates(df: pd.DataFrame) -> pd.DataFrame:
    # tenta converter colunas com cara de data
    for col in df.columns:
        if df[col].dtype == object:
            try:
                parsed = pd.to_datetime(df[col], errors='raise')
                # heurística: se conversão funcionou em >70% dos valores não nulos, mantém
                non_na = df[col].dropna().shape[0]
                if non_na == 0 or parsed.dropna().shape[0] / max(non_na, 1) >= 0.7:
                    df[col] = parsed
            except Exception:
                pass
    return df


def apply_filters(df: pd.DataFrame, filters: list[dict]) -> pd.DataFrame:
    """Aplica lista de filtros sequencialmente. Cada filtro:
    {"column": str, "op": str, "value": str | float | None, "value2": str | float | None}
    Ops suportadas:
      - equals, contains (string)
      - gt, gte, lt, lte, between (number)
      - date_between, date_eq (datetime)
    """
    out = df.copy()
    for f in filters:
        col = f.get('column')
        op = f.get('op')
        v = f.get('value')
        v2 = f.get('value2')
        if col not in out.columns:
            continue
        s = out[col]
        kind = _infer_dtype(s)

        try:
            if kind == 'datetime':
                if op == 'date_between':
                    start = dateparser.parse(v) if v else None
                    end = dateparser.parse(v2) if v2 else None
                    if start is not None:
                        out = out[out[col] >= start]
                    if end is not None:
                        out = out[out[col] <= end]
                elif op == 'date_eq' and v:
                    target = dateparser.parse(v).date()
                    out = out[pd.to_datetime(out[col]).dt.date == target]

            elif kind == 'number':
                if op == 'gt' and v is not None:
                    out = out[out[col] > float(v)]
                elif op == 'gte' and v is not None:
                    out = out[out[col] >= float(v)]
                elif op == 'lt' and v is not None:
                    out = out[out[col] < float(v)]
                elif op == 'lte' and v is not None:
                    out = out[out[col] <= float(v)]
                elif op == 'between' and v is not None and v2 is not None:
                    out = out[out[col].between(float(v), float(v2), inclusive='both')]

            else:  # string
                s = s.astype(str)
                if op == 'equals' and v is not None:
                    out = out[s.str.casefold() == str(v).casefold()]
                elif op == 'contains' and v is not None:
                    out = out[s.str.contains(str(v), case=False, na=False)]
        except Exception:
            # Se algo der errado num filtro, apenas ignora aquele filtro
            continue
    return out


# ----------------------
# Rotas
# ----------------------

@app.route('/')
def home():
    return render_template('upload.html')


@app.route('/upload', methods=['POST'])
def upload():
    file = request.files.get('file')
    if not file or file.filename.strip() == '':
        flash('Selecione um arquivo Excel (.xlsx) ou CSV.')
        return redirect(url_for('home'))

    file_id, path, safe_name = _save_upload(file)
    sheets = _list_sheets(path)
    if not sheets:
        flash('Não foi possível ler as abas da planilha.')
        return redirect(url_for('home'))

    # CSV retorna [None]; para Excel, pega a primeira aba
    first_sheet = sheets[0]
    return redirect(url_for('explore', file_id=file_id, sheet=first_sheet if first_sheet else ''))


@app.route('/explore', methods=['GET', 'POST'])
def explore():
    file_id = request.args.get('file_id') or request.form.get('file_id')
    sheet = request.args.get('sheet') or request.form.get('sheet')
    if not file_id:
        flash('ID do arquivo não encontrado. Faça o upload novamente.')
        return redirect(url_for('home'))

    # encontra o arquivo salvo
    fname = None
    for n in os.listdir(UPLOAD_DIR):
        if n.startswith(file_id):
            fname = n
            break
    if not fname:
        flash('Arquivo não encontrado no servidor (talvez expirado).')
        return redirect(url_for('home'))

    path = os.path.join(UPLOAD_DIR, fname)
    df = _load_df(path, sheet if sheet else None)
    df = _coerce_dates(df)

    # Recebe filtros (POST) como JSON no campo hidden 'filters_json'
    filters_raw = request.form.get('filters_json')
    filters = []
    if filters_raw:
        try:
            filters = json.loads(filters_raw)
        except Exception:
            filters = []

    filtered = apply_filters(df, filters) if filters else df

    # Preview limitado para não travar navegador
    preview = filtered.head(500)

    # metadados das colunas p/ construir UI
    schema = []
    for c in df.columns:
        schema.append({
            'name': c,
            'dtype': _infer_dtype(df[c])
        })

    # lista de abas (se Excel)
    sheets = _list_sheets(path)

    return render_template(
        'explore.html',
        file_id=file_id,
        current_sheet=sheet or '',
        sheets=sheets,
        schema=schema,
        total_rows=len(filtered),
        table_html=preview.to_html(classes='table table-sm table-striped', index=False, border=0, justify='center')
    )


@app.route('/download', methods=['POST'])
def download():
    file_id = request.form.get('file_id')
    sheet = request.form.get('sheet')
    fmt = request.form.get('format', 'xlsx')  # xlsx ou csv
    filters_raw = request.form.get('filters_json')

    # localizar arquivo
    fname = None
    for n in os.listdir(UPLOAD_DIR):
        if n.startswith(file_id):
            fname = n
            break
    if not fname:
        flash('Arquivo não encontrado no servidor.')
        return redirect(url_for('home'))

    path = os.path.join(UPLOAD_DIR, fname)
    df = _load_df(path, sheet if sheet else None)
    df = _coerce_dates(df)

    filters = []
    if filters_raw:
        try:
            filters = json.loads(filters_raw)
        except Exception:
            pass

    filtered = apply_filters(df, filters) if filters else df

    if fmt == 'csv':
        buf = io.StringIO()
        filtered.to_csv(buf, index=False)
        buf.seek(0)
        return send_file(io.BytesIO(buf.getvalue().encode('utf-8-sig')),
                         mimetype='text/csv',
                         as_attachment=True,
                         download_name=f'export_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')

    # default: xlsx
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine='openpyxl') as writer:
        filtered.to_excel(writer, index=False, sheet_name='Filtrado')
    buf.seek(0)
    return send_file(buf,
                     mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                     as_attachment=True,
                     download_name=f'export_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx')


if __name__ == '__main__':
    app.run(debug=True)
