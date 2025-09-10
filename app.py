import os
import uuid
import json
import re
import sqlite3
from datetime import datetime
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify, send_file
import pandas as pd
import io
import traceback

app = Flask(__name__)
app.secret_key = 'your-secret-key-here'

# Configuração de pastas
UPLOAD_FOLDER = 'uploads'
RESULTS_FOLDER = 'results'
DATABASE_PATH = 'financial_reports.db'

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(RESULTS_FOLDER, exist_ok=True)

def init_database():
    """Inicializa o banco de dados"""
    conn = sqlite3.connect(DATABASE_PATH)
    cursor = conn.cursor()
    
    # Tabela de sessões
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sessions (
            id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            file_count INTEGER DEFAULT 0,
            total_value REAL DEFAULT 0,
            status TEXT DEFAULT 'active'
        )
    ''')
    
    # Tabela de arquivos processados
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS processed_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT,
            filename TEXT,
            original_filename TEXT,
            sheet_name TEXT,
            total_value REAL,
            emission_date TEXT,
            due_date TEXT,
            month_ref INTEGER,
            year_ref INTEGER,
            success BOOLEAN,
            error_message TEXT,
            warnings TEXT,
            data_quality TEXT,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (session_id) REFERENCES sessions (id)
        )
    ''')
    
    conn.commit()
    conn.close()

# Inicializa o banco na primeira execução
init_database()

def format_currency_br(value):
    """Formata valor para padrão brasileiro: R$ 1.234.567,89"""
    try:
        if value is None or value == 0:
            return "R$ 0,00"
        return f"R$ {value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except:
        return "R$ 0,00"

def format_date_br(month, year):
    """Formata data para padrão brasileiro: mm/aaaa"""
    try:
        if not month or not year:
            return "-"
        return f"{month:02d}/{year}"
    except:
        return "-"

def get_month_name_br(month_num):
    """Retorna nome do mês em português"""
    try:
        months = {
            1: 'Janeiro', 2: 'Fevereiro', 3: 'Março', 4: 'Abril',
            5: 'Maio', 6: 'Junho', 7: 'Julho', 8: 'Agosto',
            9: 'Setembro', 10: 'Outubro', 11: 'Novembro', 12: 'Dezembro'
        }
        return months.get(month_num, f"Mês {month_num}")
    except:
        return "Mês inválido"

# Registra filtros customizados do Jinja2
@app.template_filter('currency_br')
def currency_br_filter(value):
    return format_currency_br(value)

@app.template_filter('date_br')
def date_br_filter(month, year):
    return format_date_br(month, year)

@app.template_filter('month_name_br')
def month_name_br_filter(value):
    return get_month_name_br(value)

@app.route('/')
def home():
    """Página inicial com lista de sessões salvas"""
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT s.*, COUNT(pf.id) as file_count, SUM(pf.total_value) as total_sum
            FROM sessions s
            LEFT JOIN processed_files pf ON s.id = pf.session_id AND pf.success = 1
            WHERE s.status = 'active'
            GROUP BY s.id
            ORDER BY s.updated_at DESC
            LIMIT 20
        ''')
        
        sessions = []
        for row in cursor.fetchall():
            sessions.append({
                'id': row[0],
                'title': row[1],
                'description': row[2],
                'created_at': row[3],
                'updated_at': row[4],
                'file_count': row[6] or 0,
                'total_value': row[7] or 0,
                'status': row[8],
                'formatted_total': format_currency_br(row[7] or 0)
            })
        
        conn.close()
        return render_template('home.html', sessions=sessions)
        
    except Exception as e:
        print(f"Erro ao carregar home: {e}")
        return render_template('home.html', sessions=[])

@app.route('/upload')
def upload_page():
    """Página de upload"""
    return render_template('upload.html')

@app.route('/new_session')
def new_session():
    """Cria nova sessão e redireciona para upload"""
    return redirect(url_for('upload_page'))

@app.route('/upload', methods=['POST'])
def upload():
    try:
        print("📤 Iniciando upload...")
        
        # Pega dados do formulário
        session_title = request.form.get('session_title', '').strip()
        session_description = request.form.get('session_description', '').strip()
        
        files = request.files.getlist('files[]')
        if not files or all(f.filename == '' for f in files):
            flash('Nenhum arquivo foi selecionado.', 'warning')
            return redirect(url_for('upload_page'))
        
        # Gera título automático se não fornecido
        if not session_title:
            session_title = f"Relatório {datetime.now().strftime('%d/%m/%Y %H:%M')}"
        
        print(f"📁 {len(files)} arquivo(s) recebido(s) para sessão: {session_title}")
        
        # Cria nova sessão
        session_id = str(uuid.uuid4())
        
        # Processa arquivos
        results = []
        successful_files = 0
        total_value = 0
        
        for i, file in enumerate(files):
            try:
                if file and file.filename and file.filename.endswith(('.xlsx', '.xls', '.csv')):
                    print(f"📊 Processando arquivo {i+1}/{len(files)}: {file.filename}")
                    
                    # Salva arquivo temporariamente
                    filename = f"{uuid.uuid4()}_{file.filename}"
                    filepath = os.path.join(UPLOAD_FOLDER, filename)
                    file.save(filepath)
                    
                    # Processa arquivo
                    result = process_file(filepath, file.filename)
                    results.append(result)
                    
                    if result.get('success', False):
                        successful_files += 1
                        total_value += result.get('total_value', 0)
                    
                    # Salva no banco
                    save_processed_file(session_id, result)
                    
                    # Remove arquivo temporário
                    try:
                        os.remove(filepath)
                    except:
                        pass
                        
            except Exception as e:
                print(f"❌ Erro ao processar {file.filename}: {str(e)}")
                error_result = {
                    'filename': file.filename,
                    'error': f'Erro no processamento: {str(e)}',
                    'success': False,
                    'total_value': 0.0,
                    'month': None,
                    'year': None,
                    'warnings': [f'Erro no processamento: {str(e)}'],
                    'data_quality': 'error'
                }
                results.append(error_result)
                save_processed_file(session_id, error_result)
        
        # Salva sessão no banco
        save_session(session_id, session_title, session_description, successful_files, total_value)
        
        # Feedback para o usuário
        if successful_files == len(files):
            flash(f'✅ Todos os {successful_files} arquivo(s) foram processados com sucesso!', 'success')
        elif successful_files > 0:
            flash(f'⚠️ {successful_files} de {len(files)} arquivo(s) processados com sucesso.', 'warning')
        else:
            flash(f'❌ Nenhum arquivo foi processado com sucesso.', 'error')
        
        return redirect(url_for('dashboard', session_id=session_id))
        
    except Exception as e:
        print(f"💥 Erro crítico no upload: {str(e)}")
        traceback.print_exc()
        flash(f'Erro durante o upload: {str(e)}', 'error')
        return redirect(url_for('upload_page'))

def save_session(session_id, title, description, file_count, total_value):
    """Salva sessão no banco de dados"""
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO sessions (id, title, description, file_count, total_value)
            VALUES (?, ?, ?, ?, ?)
        ''', (session_id, title, description, file_count, total_value))
        
        conn.commit()
        conn.close()
        print(f"💾 Sessão salva: {session_id} - {title}")
        
    except Exception as e:
        print(f"Erro ao salvar sessão: {e}")

def save_processed_file(session_id, result):
    """Salva arquivo processado no banco de dados"""
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        warnings_json = json.dumps(result.get('warnings', []))
        
        cursor.execute('''
            INSERT INTO processed_files (
                session_id, filename, original_filename, sheet_name, total_value,
                emission_date, due_date, month_ref, year_ref, success,
                error_message, warnings, data_quality
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            session_id,
            result.get('filename', ''),
            result.get('filename', ''),
            result.get('sheet_name', ''),
            result.get('total_value', 0),
            result.get('emission_date'),
            result.get('due_date'),
            result.get('month'),
            result.get('year'),
            result.get('success', False),
            result.get('error', ''),
            warnings_json,
            result.get('data_quality', 'unknown')
        ))
        
        conn.commit()
        conn.close()
        
    except Exception as e:
        print(f"Erro ao salvar arquivo processado: {e}")

def load_session_data(session_id):
    """Carrega dados da sessão do banco de dados"""
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        # Carrega dados da sessão
        cursor.execute('SELECT * FROM sessions WHERE id = ?', (session_id,))
        session_row = cursor.fetchone()
        
        if not session_row:
            return None, []
        
        session_data = {
            'id': session_row[0],
            'title': session_row[1],
            'description': session_row[2],
            'created_at': session_row[3],
            'updated_at': session_row[4]
        }
        
        # Carrega arquivos processados
        cursor.execute('SELECT * FROM processed_files WHERE session_id = ? ORDER BY processed_at', (session_id,))
        files_rows = cursor.fetchall()
        
        results = []
        for row in files_rows:
            warnings = json.loads(row[12]) if row[12] else []
            result = {
                'filename': row[2],
                'original_filename': row[3],
                'sheet_name': row[4],
                'total_value': row[5],
                'emission_date': row[6],
                'due_date': row[7],
                'month': row[8],
                'year': row[9],
                'success': bool(row[10]),
                'error': row[11],
                'warnings': warnings,
                'data_quality': row[13],
                'formatted_date': format_date_br(row[8], row[9]),
                'formatted_value': format_currency_br(row[5])
            }
            results.append(result)
        
        conn.close()
        return session_data, results
        
    except Exception as e:
        print(f"Erro ao carregar sessão: {e}")
        return None, []

@app.route('/dashboard/<session_id>')
def dashboard(session_id):
    try:
        # Carrega dados da sessão
        session_data, results = load_session_data(session_id)
        
        if not session_data:
            flash('Sessão não encontrada.', 'error')
            return redirect(url_for('home'))
        
        # Calcula métricas
        successful_results = [r for r in results if r.get('success', False) and r.get('total_value', 0) > 0]
        
        total_values = [r.get('total_value', 0) for r in successful_results]
        total_sum = sum(total_values)
        avg_monthly = total_sum / len(total_values) if total_values else 0.0
        file_count = len(results)
        max_value = max(total_values) if total_values else 0.0
        min_value = min(total_values) if total_values else 0.0
        
        # Encontra período do maior e menor valor
        max_period = "N/A"
        min_period = "N/A"
        
        for r in successful_results:
            if r.get('total_value', 0) == max_value:
                max_period = format_date_br(r.get('month'), r.get('year'))
                break
        
        for r in successful_results:
            if r.get('total_value', 0) == min_value:
                min_period = format_date_br(r.get('month'), r.get('year'))
                break
        
        # Dados para gráfico ordenados por data
        chart_data = []
        for r in successful_results:
            month = r.get('month')
            year = r.get('year')
            total = r.get('total_value', 0)
            
            if month and year and total > 0:
                chart_data.append({
                    'Label': format_date_br(month, year),
                    'Total': total,
                    'sort_key': f"{year:04d}{month:02d}"
                })
        
        # Ordena por data
        chart_data.sort(key=lambda x: x['sort_key'])
        
        # Calcula estatísticas de qualidade
        quality_stats = {
            'good': len([r for r in results if r.get('data_quality') == 'good']),
            'warning': len([r for r in results if r.get('data_quality') == 'warning']),
            'poor': len([r for r in results if r.get('data_quality') == 'poor']),
            'error': len([r for r in results if r.get('data_quality') == 'error'])
        }
        
        metrics = {
            'total_value': total_sum,
            'average_monthly': avg_monthly,
            'file_count': file_count,
            'max_month': {'value': max_value, 'period': max_period},
            'min_month': {'value': min_value, 'period': min_period},
            'formatted_total': format_currency_br(total_sum),
            'formatted_average': format_currency_br(avg_monthly),
            'quality_stats': quality_stats
        }
        
        return render_template('dashboard.html', 
                             session_id=session_id,
                             session_data=session_data,
                             results=results,
                             metrics=metrics,
                             chart_data=chart_data)
                             
    except Exception as e:
        print(f"Erro no dashboard: {str(e)}")
        traceback.print_exc()
        flash(f'Erro ao carregar dashboard: {str(e)}', 'error')
        return redirect(url_for('home'))

@app.route('/delete_session/<session_id>', methods=['POST'])
def delete_session(session_id):
    """Deleta uma sessão"""
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        # Marca sessão como deletada (soft delete)
        cursor.execute('UPDATE sessions SET status = ? WHERE id = ?', ('deleted', session_id))
        
        conn.commit()
        conn.close()
        
        flash('Sessão deletada com sucesso.', 'success')
        return redirect(url_for('home'))
        
    except Exception as e:
        print(f"Erro ao deletar sessão: {e}")
        flash('Erro ao deletar sessão.', 'error')
        return redirect(url_for('home'))

@app.route('/duplicate_session/<session_id>')
def duplicate_session(session_id):
    """Duplica uma sessão existente"""
    try:
        session_data, results = load_session_data(session_id)
        
        if not session_data:
            flash('Sessão não encontrada.', 'error')
            return redirect(url_for('home'))
        
        # Cria nova sessão
        new_session_id = str(uuid.uuid4())
        new_title = f"Cópia de {session_data['title']}"
        
        # Salva nova sessão
        successful_files = len([r for r in results if r.get('success', False)])
        total_value = sum([r.get('total_value', 0) for r in results if r.get('success', False)])
        
        save_session(new_session_id, new_title, session_data.get('description', ''), successful_files, total_value)
        
        # Copia arquivos
        for result in results:
            save_processed_file(new_session_id, result)
        
        flash(f'Sessão duplicada com sucesso: {new_title}', 'success')
        return redirect(url_for('dashboard', session_id=new_session_id))
        
    except Exception as e:
        print(f"Erro ao duplicar sessão: {e}")
        flash('Erro ao duplicar sessão.', 'error')
        return redirect(url_for('home'))

@app.route('/download')
def download():
    """Exporta os dados da sessão em CSV ou XLSX."""
    try:
        session_id = request.args.get('session_id')
        export_format = (request.args.get('format') or 'xlsx').lower()

        # Carrega dados da sessão e arquivos processados
        session_data, results = load_session_data(session_id)
        if not session_data:
            flash('Sessão não encontrada.', 'error')
            return redirect(url_for('home'))

        # Monte o DataFrame com os resultados válidos (ou todos, se preferir)
        rows = []
        for r in results:
            rows.append({
                'Arquivo': r.get('filename'),
                'Mês': r.get('month'),
                'Ano': r.get('year'),
                'Data Emissão': r.get('emission_date'),
                'Data Vencimento': r.get('due_date'),
                'Valor Total': r.get('total_value', 0.0),
                'Qualidade': r.get('data_quality'),
                'Avisos': '; '.join(r.get('warnings', []) if isinstance(r.get('warnings'), list) else [])
            })

        import pandas as pd
        import io
        from flask import send_file

        df = pd.DataFrame(rows)

        # Se quiser exportar apenas os bem-sucedidos, use:
        # df = df[df['Qualidade'].isin(['good', 'warning']) | df['Valor Total'].fillna(0).gt(0)]

        # Nome do arquivo
        safe_title = re.sub(r'[^a-zA-Z0-9_-]+', '_', session_data.get('title', f'sessao_{session_id}'))
        if export_format == 'csv':
            buf = io.BytesIO()
            # Use utf-8-sig para abrir direto no Excel com acentuação correta
            csv_bytes = df.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
            buf.write(csv_bytes)
            buf.seek(0)
            return send_file(
                buf,
                as_attachment=True,
                download_name=f'{safe_title}.csv',
                mimetype='text/csv'
            )

        # Padrão: XLSX
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Dados', index=False)

            # (Opcional) cria um pequeno resumo por mês/ano
            try:
                resumo = (df.assign(Valor=df['Valor Total'].fillna(0.0))
                            .groupby(['Ano', 'Mês'], dropna=False)['Valor'].sum()
                            .reset_index()
                            .sort_values(by=['Ano', 'Mês']))
                resumo.to_excel(writer, sheet_name='Resumo', index=False)
            except Exception:
                # se algo der errado no resumo, seguimos apenas com a aba Dados
                pass

        buf.seek(0)
        return send_file(
            buf,
            as_attachment=True,
            download_name=f'{safe_title}.xlsx',
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )

    except Exception as e:
        print(f'Erro no download/exportação: {e}')
        traceback.print_exc()
        flash('Falha ao gerar o arquivo de exportação.', 'error')
        return redirect(url_for('dashboard', session_id=session_id))


# Resto das funções (process_file, extract_total_value, etc.) permanecem iguais...
# [Inclua aqui as funções de processamento que já estavam funcionando]

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

def extract_date_from_filename_improved(filename):
    """Extrai mês e ano do nome do arquivo de forma mais robusta"""
    try:
        meses = {
            'janeiro': 1, 'jan': 1, 'fevereiro': 2, 'fev': 2,
            'março': 3, 'mar': 3, 'abril': 4, 'abr': 4,
            'maio': 5, 'mai': 5, 'junho': 6, 'jun': 6,
            'julho': 7, 'jul': 7, 'agosto': 8, 'ago': 8,
            'setembro': 9, 'set': 9, 'outubro': 10, 'out': 10,
            'novembro': 11, 'nov': 11, 'dezembro': 12, 'dez': 12
        }
        
        filename_lower = filename.lower()
        
        year = None
        year_patterns = [
            r'[-\s]+(20\d{2})',  # " - 2023" ou " 2023"
            r'(20\d{2})[-\s]*',  # "2023 -" ou "2023"
            r'[-\s]+(\d{2})(?:[^\d]|$)'  # " - 23" (converter para 20XX)
        ]
        
        for pattern in year_patterns:
            match = re.search(pattern, filename)
            if match:
                year_candidate = int(match.group(1))
                if year_candidate < 100:
                    year_candidate = 2000 + year_candidate if year_candidate < 50 else 1900 + year_candidate
                current_year = datetime.now().year
                if 2000 <= year_candidate <= current_year + 5:
                    year = year_candidate
                    break
        
        if year is None:
            year = datetime.now().year
        
        month = None
        for mes_nome, mes_num in meses.items():
            if mes_nome in filename_lower:
                month = mes_num
                break
        
        if month is None:
            month_match = re.search(r'(?:^|[^\d])(\d{1,2})(?:[^\d]|$)', filename)
            if month_match:
                potential_month = int(month_match.group(1))
                if 1 <= potential_month <= 12:
                    month = potential_month
        
        return month, year
        
    except Exception as e:
        print(f"Erro na extração de data do filename {filename}: {e}")
        return None, datetime.now().year

def validate_extracted_data(result):
    """Valida se os dados extraídos fazem sentido"""
    try:
        warnings = []
        
        if result['total_value'] <= 0:
            warnings.append("⚠️ Valor total é zero ou negativo")
        elif result['total_value'] > 10000000:
            warnings.append("⚠️ Valor parece muito alto (>R$ 10 milhões)")
        elif result['total_value'] < 1000:
            warnings.append("⚠️ Valor parece muito baixo (<R$ 1.000)")
        
        current_year = datetime.now().year
        if result['year']:
            if result['year'] < 2000 or result['year'] > current_year + 1:
                warnings.append(f"⚠️ Ano {result['year']} parece incorreto")
            elif result['year'] > current_year:
                warnings.append(f"⚠️ Ano {result['year']} é futuro")
        
        if result['month'] and (result['month'] < 1 or result['month'] > 12):
            warnings.append(f"⚠️ Mês {result['month']} é inválido")
        
        result['warnings'] = warnings
        result['data_quality'] = 'good' if len(warnings) == 0 else 'warning' if len(warnings) <= 2 else 'poor'
        
        return result
        
    except Exception as e:
        print(f"Erro na validação de dados: {e}")
        result['warnings'] = [f'Erro na validação: {str(e)}']
        result['data_quality'] = 'error'
        return result

def process_file(filepath, original_name):
    """Processa um único arquivo"""
    try:
        print(f"📊 Processando: {original_name}")
        
        if filepath.endswith('.csv'):
            df = pd.read_csv(filepath, encoding='utf-8')
            sheet_name = 'CSV'
        else:
            excel_file = pd.ExcelFile(filepath)
            target_sheet = None
            for sheet in excel_file.sheet_names:
                if 'total' in sheet.lower() and ('mês' in sheet.lower() or 'mes' in sheet.lower()):
                    target_sheet = sheet
                    break
            
            if target_sheet is None:
                target_sheet = excel_file.sheet_names[0]
            
            df = pd.read_excel(filepath, sheet_name=target_sheet)
            sheet_name = target_sheet
        
        total_value = extract_total_value(df)
        emission_date, due_date = extract_dates(df)
        month, year = extract_date_from_filename_improved(original_name)
        
        result = {
            'filename': original_name,
            'sheet_name': sheet_name,
            'total_value': safe_float(total_value),
            'emission_date': emission_date,
            'due_date': due_date,
            'month': safe_int(month),
            'year': safe_int(year),
            'success': True,
            'formatted_date': format_date_br(month, year),
            'formatted_value': format_currency_br(safe_float(total_value))
        }
        
        result = validate_extracted_data(result)
        return result
        
    except Exception as e:
        print(f"❌ Erro no processamento de {original_name}: {str(e)}")
        return {
            'filename': original_name,
            'error': str(e),
            'success': False,
            'total_value': 0.0,
            'month': None,
            'year': None,
            'warnings': [f'Erro no processamento: {str(e)}'],
            'data_quality': 'error',
            'formatted_date': '-',
            'formatted_value': 'R$ 0,00'
        }

def extract_total_value(df):
    """Extrai o maior valor numérico do DataFrame"""
    try:
        max_value = 0.0
        
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
        
    except Exception as e:
        print(f"Erro na extração de valor: {e}")
        return 0.0

def extract_dates(df):
    """Extrai datas do DataFrame"""
    try:
        emission_date = None
        due_date = None
        
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
        
    except Exception as e:
        print(f"Erro na extração de datas: {e}")
        return None, None

if __name__ == '__main__':
    print("🚀 Iniciando Sistema Financeiro com Persistência...")
    print("📍 Acesse: http://localhost:5000")
    print("💾 Banco de dados: financial_reports.db")
    app.run(host='0.0.0.0', port=5000, debug=True)