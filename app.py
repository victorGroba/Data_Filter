import os
import uuid
import json
import re
from datetime import datetime
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify, send_file
import pandas as pd
import io

app = Flask(__name__)
app.secret_key = 'your-secret-key-here'

# Registra filtros customizados do Jinja2
@app.template_filter('currency_br')
def currency_br_filter(value):
    return format_currency_br(value)

@app.template_filter('month_name_br')
def month_name_br_filter(value):
    return get_month_name_br(value)

# Configuração de pastas
UPLOAD_FOLDER = 'uploads'
RESULTS_FOLDER = 'results'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(RESULTS_FOLDER, exist_ok=True)

# Armazena resultados em memória (para simplificar)
processed_results = {}

@app.route('/')
def home():
    return render_template('upload.html')

@app.route('/upload', methods=['POST'])
def upload():
    try:
        files = request.files.getlist('files[]')
        if not files or all(f.filename == '' for f in files):
            flash('Selecione pelo menos um arquivo.')
            return redirect(url_for('home'))
        
        results = []
        session_id = str(uuid.uuid4())
        
        for file in files:
            if file and file.filename and file.filename.endswith(('.xlsx', '.xls', '.csv')):
                # Salva arquivo
                filename = f"{uuid.uuid4()}_{file.filename}"
                filepath = os.path.join(UPLOAD_FOLDER, filename)
                file.save(filepath)
                
                # Processa arquivo
                try:
                    result = process_file(filepath, file.filename)
                    results.append(result)
                except Exception as e:
                    results.append({
                        'filename': file.filename,
                        'error': str(e),
                        'success': False
                    })
        
        # Salva resultados
        processed_results[session_id] = results
        
        return redirect(url_for('dashboard', session_id=session_id))
        
    except Exception as e:
        flash(f'Erro durante o upload: {str(e)}')
        return redirect(url_for('home'))

def safe_float(value):
    """Converte valor para float de forma segura"""
    try:
        if value is None or value == '':
            return 0.0
        return float(value)
    except (ValueError, TypeError):
        return 0.0

def safe_int(value):
    """Converte valor para int de forma segura"""
    try:
        if value is None or value == '':
            return None
        return int(float(value))
    except (ValueError, TypeError):
        return None

def process_file(filepath, original_name):
    """Processa um único arquivo"""
    try:
        # Determina se é Excel ou CSV
        if filepath.endswith('.csv'):
            df = pd.read_csv(filepath)
            sheet_name = 'CSV'
        else:
            excel_file = pd.ExcelFile(filepath)
            # Procura por aba "Total Mês" ou usa a primeira
            target_sheet = None
            for sheet in excel_file.sheet_names:
                if 'total' in sheet.lower() and ('mês' in sheet.lower() or 'mes' in sheet.lower()):
                    target_sheet = sheet
                    break
            
            if target_sheet is None:
                target_sheet = excel_file.sheet_names[0]
            
            df = pd.read_excel(filepath, sheet_name=target_sheet)
            sheet_name = target_sheet
        
        # Extrai valor total (maior valor numérico)
        total_value = extract_total_value(df)
        
        # Extrai datas
        emission_date, due_date = extract_dates(df)
        
        # Infere mês/ano do nome do arquivo
        month, year = extract_date_from_filename(original_name)
        
        return {
            'filename': original_name,
            'sheet_name': sheet_name,
            'total_value': safe_float(total_value),
            'emission_date': emission_date,
            'due_date': due_date,
            'month': safe_int(month),
            'year': safe_int(year),
            'success': True
        }
        
    except Exception as e:
        return {
            'filename': original_name,
            'error': str(e),
            'success': False,
            'total_value': 0.0,
            'month': None,
            'year': None
        }

def extract_total_value(df):
    """Extrai o maior valor numérico do DataFrame"""
    max_value = 0.0
    
    # Primeiro, procura por linhas que contenham "total"
    for idx, row in df.iterrows():
        row_str = ' '.join([str(val).lower() for val in row.values if pd.notna(val)])
        if 'total' in row_str:
            for col in df.columns:
                try:
                    value = pd.to_numeric(row[col], errors='coerce')
                    if pd.notna(value) and abs(value) > abs(max_value):
                        max_value = float(value)
                except:
                    continue
    
    # Se não encontrou "total", pega o maior valor da planilha
    if max_value == 0.0:
        for col in df.columns:
            try:
                numeric_col = pd.to_numeric(df[col], errors='coerce')
                col_max = numeric_col.max()
                if pd.notna(col_max) and abs(col_max) > abs(max_value):
                    max_value = float(col_max)
            except:
                continue
    
    return safe_float(max_value)

def extract_dates(df):
    """Extrai datas do DataFrame"""
    emission_date = None
    due_date = None
    
    # Busca por colunas que possam conter datas
    for col in df.columns:
        col_name = str(col).lower()
        try:
            if any(term in col_name for term in ['emissão', 'emissao', 'emitido']):
                date_series = pd.to_datetime(df[col], errors='coerce')
                valid_date = date_series.dropna().iloc[0] if not date_series.dropna().empty else None
                if valid_date:
                    emission_date = valid_date.strftime('%Y-%m-%d')
            
            if any(term in col_name for term in ['vencimento', 'vence']):
                date_series = pd.to_datetime(df[col], errors='coerce')
                valid_date = date_series.dropna().iloc[0] if not date_series.dropna().empty else None
                if valid_date:
                    due_date = valid_date.strftime('%Y-%m-%d')
        except:
            continue
    
    return emission_date, due_date

def extract_date_from_filename(filename):
    """Extrai mês e ano do nome do arquivo"""
    # Mapeia meses em português
    meses = {
        'janeiro': 1, 'jan': 1, 'fevereiro': 2, 'fev': 2,
        'março': 3, 'mar': 3, 'abril': 4, 'abr': 4,
        'maio': 5, 'mai': 5, 'junho': 6, 'jun': 6,
        'julho': 7, 'jul': 7, 'agosto': 8, 'ago': 8,
        'setembro': 9, 'set': 9, 'outubro': 10, 'out': 10,
        'novembro': 11, 'nov': 11, 'dezembro': 12, 'dez': 12
    }
    
    filename_lower = filename.lower()
    
    # Busca ano
    year_match = re.search(r'20\d{2}', filename)
    year = int(year_match.group()) if year_match else datetime.now().year
    
    # Busca mês por nome
    month = None
    for mes_nome, mes_num in meses.items():
        if mes_nome in filename_lower:
            month = mes_num
            break
    
    # Se não encontrou mês por nome, busca por número
    if month is None:
        month_match = re.search(r'(?:^|[^\d])(\d{1,2})(?:[^\d]|$)', filename)
        if month_match:
            potential_month = int(month_match.group(1))
            if 1 <= potential_month <= 12:
                month = potential_month
    
    return month, year

@app.route('/dashboard/<session_id>')
def dashboard(session_id):
    if session_id not in processed_results:
        flash('Sessão não encontrada.')
        return redirect(url_for('home'))
    
    results = processed_results[session_id]
    
    # Calcula métricas
    successful_results = [r for r in results if r.get('success', False) and safe_float(r.get('total_value', 0)) > 0]
    
    total_values = [safe_float(r.get('total_value', 0)) for r in successful_results]
    total_sum = sum(total_values)
    avg_monthly = total_sum / len(total_values) if total_values else 0.0
    file_count = len(results)
    max_value = max(total_values) if total_values else 0.0
    min_value = min(total_values) if total_values else 0.0
    
    # Dados para gráfico
    chart_data = []
    for r in successful_results:
        month = safe_int(r.get('month'))
        year = safe_int(r.get('year'))
        total = safe_float(r.get('total_value', 0))
        
        if month and year and total > 0:
            chart_data.append({
                'Label': f"{month:02d}/{year}",
                'Total': total
            })
    
    metrics = {
        'total_value': total_sum,
        'average_monthly': avg_monthly,
        'file_count': file_count,
        'max_month': {'value': max_value, 'period': 'N/A'},
        'min_month': {'value': min_value, 'period': 'N/A'}
    }
    
    return render_template('dashboard.html', 
                         session_id=session_id,
                         results=results,
                         metrics=metrics,
                         chart_data=chart_data)

@app.route('/api/dashboard_data/<session_id>')
def api_dashboard_data(session_id):
    if session_id not in processed_results:
        return jsonify({'error': 'Sessão não encontrada'}), 404
    
    results = processed_results[session_id]
    successful_results = [r for r in results if r.get('success', False) and safe_float(r.get('total_value', 0)) > 0]
    
    # Aplica filtros se fornecidos
    year_filter = request.args.get('year', type=int)
    month_filter = request.args.get('month', type=int)
    
    if year_filter:
        successful_results = [r for r in successful_results if safe_int(r.get('year')) == year_filter]
    if month_filter:
        successful_results = [r for r in successful_results if safe_int(r.get('month')) == month_filter]
    
    # Recalcula métricas
    total_values = [safe_float(r.get('total_value', 0)) for r in successful_results]
    total_sum = sum(total_values)
    avg_monthly = total_sum / len(total_values) if total_values else 0.0
    max_value = max(total_values) if total_values else 0.0
    min_value = min(total_values) if total_values else 0.0
    
    chart_data = []
    for r in successful_results:
        month = safe_int(r.get('month'))
        year = safe_int(r.get('year'))
        total = safe_float(r.get('total_value', 0))
        
        if month and year and total > 0:
            chart_data.append({
                'Label': f"{month:02d}/{year}",
                'Total': total
            })
    
    metrics = {
        'total_value': total_sum,
        'average_monthly': avg_monthly,
        'file_count': len(successful_results),
        'max_month': {'value': max_value, 'period': 'N/A'},
        'min_month': {'value': min_value, 'period': 'N/A'}
    }
    
    return jsonify({
        'metrics': metrics,
        'chart_data': chart_data
    })

@app.route('/download/<session_id>')
def download(session_id):
    if session_id not in processed_results:
        flash('Sessão não encontrada.')
        return redirect(url_for('home'))
    
    results = processed_results[session_id]
    format_type = request.args.get('format', 'xlsx')  # xlsx ou csv
    
    # Cria DataFrame
    df_data = []
    for r in results:
        df_data.append({
            'Arquivo': r.get('filename', ''),
            'Aba': r.get('sheet_name', ''),
            'Total': safe_float(r.get('total_value', 0)),
            'Data_Emissao': r.get('emission_date', ''),
            'Data_Vencimento': r.get('due_date', ''),
            'Mes': safe_int(r.get('month')),
            'Ano': safe_int(r.get('year')),
            'Sucesso': r.get('success', False),
            'Erro': r.get('error', '')
        })
    
    df = pd.DataFrame(df_data)
    
    if format_type == 'csv':
        # Retorna CSV
        buffer = io.StringIO()
        df.to_csv(buffer, index=False, encoding='utf-8-sig')
        buffer.seek(0)
        
        return send_file(
            io.BytesIO(buffer.getvalue().encode('utf-8-sig')),
            mimetype='text/csv',
            as_attachment=True,
            download_name=f'relatorio_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        )
    
    # Default: Excel
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='Consolidado', index=False)
        
        # Adiciona resumo se houver dados
        successful_data = [r for r in results if r.get('success', False)]
        if successful_data:
            # Resumo por ano
            yearly_data = {}
            for r in successful_data:
                year = safe_int(r.get('year'))
                total = safe_float(r.get('total_value', 0))
                if year and total > 0:
                    if year not in yearly_data:
                        yearly_data[year] = []
                    yearly_data[year].append(total)
            
            yearly_summary = []
            for year, values in yearly_data.items():
                yearly_summary.append({
                    'Ano': year,
                    'Total': sum(values),
                    'Média': sum(values) / len(values),
                    'Quantidade': len(values)
                })
            
            if yearly_summary:
                yearly_df = pd.DataFrame(yearly_summary)
                yearly_df.to_excel(writer, sheet_name='Resumo Anual', index=False)
    
    buffer.seek(0)
    
    return send_file(
        buffer,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name=f'relatorio_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
    )

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)