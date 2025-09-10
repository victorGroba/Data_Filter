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
from collections import Counter

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

def format_date_br(date_str):
    """Formata data para padrão brasileiro: dd/mm/aaaa"""
    try:
        if not date_str or date_str == '-' or pd.isna(date_str):
            return "-"
        
        # Se já está no formato brasileiro
        if isinstance(date_str, str) and '/' in date_str and len(date_str.split('/')) == 3:
            parts = date_str.split('/')
            if len(parts[0]) == 2:  # já está dd/mm/aaaa
                return date_str
        
        # Se está no formato YYYY-MM-DD
        if isinstance(date_str, str) and '-' in date_str:
            try:
                date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                return date_obj.strftime('%d/%m/%Y')
            except:
                pass
        
        # Se é um objeto datetime
        if hasattr(date_str, 'strftime'):
            return date_str.strftime('%d/%m/%Y')
        
        # Tenta converter string para datetime
        if isinstance(date_str, str):
            try:
                # Tenta vários formatos
                for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%Y/%m/%d']:
                    try:
                        date_obj = datetime.strptime(date_str, fmt)
                        return date_obj.strftime('%d/%m/%Y')
                    except:
                        continue
            except:
                pass
        
        return str(date_str)
        
    except Exception as e:
        print(f"Erro ao formatar data {date_str}: {e}")
        return "-"

def format_date_period_br(month, year):
    """Formata período para padrão brasileiro: mm/aaaa"""
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
def date_br_filter(date_str):
    return format_date_br(date_str)

@app.template_filter('date_period_br')
def date_period_br_filter(month, year):
    return format_date_period_br(month, year)

@app.template_filter('month_name_br')
def month_name_br_filter(value):
    return get_month_name_br(value)

@app.route('/')
def home():
    """Página inicial com lista de sessões salvas - CORRIGIDA"""
    try:
        print("🏠 Carregando página inicial...")
        
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        # Query corrigida para buscar sessões ativas
        cursor.execute('''
            SELECT 
                s.id,
                s.title,
                s.description,
                s.created_at,
                s.updated_at,
                s.file_count,
                s.total_value,
                s.status,
                COUNT(CASE WHEN pf.success = 1 THEN 1 END) as real_file_count,
                SUM(CASE WHEN pf.success = 1 THEN pf.total_value ELSE 0 END) as real_total_value
            FROM sessions s
            LEFT JOIN processed_files pf ON s.id = pf.session_id
            WHERE s.status = 'active'
            GROUP BY s.id, s.title, s.description, s.created_at, s.updated_at, s.file_count, s.total_value, s.status
            ORDER BY s.updated_at DESC
            LIMIT 20
        ''')
        
        rows = cursor.fetchall()
        print(f"📊 Query retornou {len(rows)} sessões ativas")
        
        sessions = []
        for row in rows:
            # Usa os valores reais calculados da query
            file_count = row[8] or 0  # real_file_count
            total_value = row[9] or 0  # real_total_value
            
            session_data = {
                'id': row[0],
                'title': row[1],
                'description': row[2] or '',
                'created_at': row[3],
                'updated_at': row[4],
                'file_count': file_count,
                'total_value': total_value,
                'status': row[7],
                'formatted_total': format_currency_br(total_value)
            }
            sessions.append(session_data)
            print(f"✅ Sessão: {session_data['title']} - {file_count} arquivos - {session_data['formatted_total']}")
        
        conn.close()
        
        print(f"🎯 Enviando {len(sessions)} sessões para o template")
        return render_template('home.html', sessions=sessions)
        
    except Exception as e:
        print(f"❌ Erro ao carregar home: {e}")
        import traceback
        traceback.print_exc()
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
    """Upload corrigido com melhor tratamento de arquivos"""
    try:
        print("📤 Iniciando upload...")
        
        # Pega dados do formulário
        session_title = request.form.get('session_title', '').strip()
        session_description = request.form.get('session_description', '').strip()
        
        # Corrige a obtenção dos arquivos
        files = []
        if 'files[]' in request.files:
            files = request.files.getlist('files[]')
        elif 'files' in request.files:
            files = [request.files['files']]
        
        # Filtra arquivos válidos
        valid_files = []
        for file in files:
            if file and file.filename and file.filename.strip():
                if file.filename.endswith(('.xlsx', '.xls', '.csv')):
                    valid_files.append(file)
                else:
                    flash(f'Arquivo {file.filename} tem formato não suportado.', 'warning')
        
        if not valid_files:
            flash('Nenhum arquivo válido foi selecionado.', 'warning')
            return redirect(url_for('upload_page'))
        
        # Gera título automático se não fornecido
        if not session_title:
            session_title = f"Relatório {datetime.now().strftime('%d/%m/%Y %H:%M')}"
        
        print(f"📁 {len(valid_files)} arquivo(s) válido(s) recebido(s) para sessão: {session_title}")
        
        # Cria nova sessão
        session_id = str(uuid.uuid4())
        
        # Processa arquivos
        results = []
        successful_files = 0
        total_value = 0
        
        for i, file in enumerate(valid_files):
            try:
                print(f"📊 Processando arquivo {i+1}/{len(valid_files)}: {file.filename}")
                
                # Gera nome único para o arquivo
                file_extension = os.path.splitext(file.filename)[1]
                unique_filename = f"{uuid.uuid4()}{file_extension}"
                filepath = os.path.join(UPLOAD_FOLDER, unique_filename)
                
                # Salva arquivo
                file.save(filepath)
                print(f"✅ Arquivo salvo: {filepath}")
                
                # Processa arquivo
                result = process_file(filepath, file.filename)
                results.append(result)
                
                if result.get('success', False):
                    successful_files += 1
                    total_value += result.get('total_value', 0)
                
                # Salva no banco
                save_processed_file(session_id, result, unique_filename)
                
                # Mantém arquivo salvo para possível reprocessamento
                print(f"🔄 Arquivo processado: {file.filename}")
                        
            except Exception as e:
                print(f"❌ Erro ao processar {file.filename}: {str(e)}")
                traceback.print_exc()
                
                error_result = {
                    'filename': file.filename,
                    'error': f'Erro no processamento: {str(e)}',
                    'success': False,
                    'total_value': 0.0,
                    'month': None,
                    'year': None,
                    'emission_date': None,
                    'due_date': None,
                    'warnings': [f'Erro no processamento: {str(e)}'],
                    'data_quality': 'error'
                }
                results.append(error_result)
                save_processed_file(session_id, error_result, '')
        
        # Salva sessão no banco
        save_session(session_id, session_title, session_description, successful_files, total_value)
        
        # Feedback para o usuário
        if successful_files == len(valid_files):
            flash(f'✅ Todos os {successful_files} arquivo(s) foram processados com sucesso!', 'success')
        elif successful_files > 0:
            flash(f'⚠️ {successful_files} de {len(valid_files)} arquivo(s) processados com sucesso.', 'warning')
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

def save_processed_file(session_id, result, stored_filename=''):
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
            stored_filename,  # Nome do arquivo salvo no disco
            result.get('filename', ''),  # Nome original
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

def cleanup_session_files(session_id):
    """Remove arquivos físicos da sessão"""
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        # Busca todos os arquivos da sessão
        cursor.execute('SELECT filename FROM processed_files WHERE session_id = ?', (session_id,))
        files = cursor.fetchall()
        
        # Remove arquivos físicos
        for (filename,) in files:
            if filename and filename.strip():
                filepath = os.path.join(UPLOAD_FOLDER, filename)
                try:
                    if os.path.exists(filepath):
                        os.remove(filepath)
                        print(f"🗑️ Arquivo removido: {filepath}")
                except Exception as e:
                    print(f"Erro ao remover arquivo {filepath}: {e}")
        
        conn.close()
        
    except Exception as e:
        print(f"Erro na limpeza de arquivos: {e}")

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
                'filename': row[3],  # original_filename
                'stored_filename': row[2],  # filename no disco
                'sheet_name': row[4],
                'total_value': row[5],
                'emission_date': format_date_br(row[6]) if row[6] else None,
                'due_date': format_date_br(row[7]) if row[7] else None,
                'month': row[8],
                'year': row[9],
                'success': bool(row[10]),
                'error': row[11],
                'warnings': warnings,
                'data_quality': row[13],
                'formatted_date': format_date_period_br(row[8], row[9]),
                'formatted_value': format_currency_br(row[5])
            }
            results.append(result)
        
        conn.close()
        return session_data, results
        
    except Exception as e:
        print(f"Erro ao carregar sessão: {e}")
        return None, []

def calculate_metrics(results, year_filter=None, month_filter=None):
    """Calcula métricas com filtros opcionais"""
    try:
        # Aplica filtros
        filtered_results = []
        for r in results:
            if not r.get('success', False) or not r.get('total_value', 0) > 0:
                continue
            
            if year_filter and r.get('year') != int(year_filter):
                continue
                
            if month_filter and r.get('month') != int(month_filter):
                continue
                
            filtered_results.append(r)
        
        if not filtered_results:
            return {
                'total_value': 0,
                'average_monthly': 0,
                'file_count': 0,
                'max_month': {'value': 0, 'period': 'N/A'},
                'min_month': {'value': 0, 'period': 'N/A'},
                'formatted_total': format_currency_br(0),
                'formatted_average': format_currency_br(0),
                'quality_stats': {'good': 0, 'warning': 0, 'poor': 0, 'error': 0}
            }
        
        total_values = [r.get('total_value', 0) for r in filtered_results]
        total_sum = sum(total_values)
        avg_monthly = total_sum / len(total_values) if total_values else 0.0
        file_count = len(filtered_results)
        max_value = max(total_values) if total_values else 0.0
        min_value = min(total_values) if total_values else 0.0
        
        # Encontra período do maior e menor valor
        max_period = "N/A"
        min_period = "N/A"
        
        for r in filtered_results:
            if r.get('total_value', 0) == max_value:
                max_period = format_date_period_br(r.get('month'), r.get('year'))
                break
        
        for r in filtered_results:
            if r.get('total_value', 0) == min_value:
                min_period = format_date_period_br(r.get('month'), r.get('year'))
                break
        
        # Calcula estatísticas de qualidade
        quality_stats = {
            'good': len([r for r in results if r.get('data_quality') == 'good']),
            'warning': len([r for r in results if r.get('data_quality') == 'warning']),
            'poor': len([r for r in results if r.get('data_quality') == 'poor']),
            'error': len([r for r in results if r.get('data_quality') == 'error'])
        }
        
        return {
            'total_value': total_sum,
            'average_monthly': avg_monthly,
            'file_count': file_count,
            'max_month': {'value': max_value, 'period': max_period},
            'min_month': {'value': min_value, 'period': min_period},
            'formatted_total': format_currency_br(total_sum),
            'formatted_average': format_currency_br(avg_monthly),
            'quality_stats': quality_stats
        }
        
    except Exception as e:
        print(f"Erro no cálculo de métricas: {e}")
        return {
            'total_value': 0,
            'average_monthly': 0,
            'file_count': 0,
            'max_month': {'value': 0, 'period': 'N/A'},
            'min_month': {'value': 0, 'period': 'N/A'},
            'formatted_total': format_currency_br(0),
            'formatted_average': format_currency_br(0),
            'quality_stats': {'good': 0, 'warning': 0, 'poor': 0, 'error': 0}
        }

def get_chart_data(results, year_filter=None, month_filter=None):
    """Gera dados do gráfico com filtros"""
    try:
        # Filtra resultados válidos
        successful_results = []
        for r in results:
            if not r.get('success', False) or not r.get('total_value', 0) > 0:
                continue
            
            if year_filter and r.get('year') != int(year_filter):
                continue
                
            if month_filter and r.get('month') != int(month_filter):
                continue
                
            successful_results.append(r)
        
        # Dados para gráfico ordenados por data
        chart_data = []
        for r in successful_results:
            month = r.get('month')
            year = r.get('year')
            total = r.get('total_value', 0)
            
            if month and year and total > 0:
                chart_data.append({
                    'Label': format_date_period_br(month, year),
                    'Total': total,
                    'sort_key': f"{year:04d}{month:02d}"
                })
        
        # Ordena por data
        chart_data.sort(key=lambda x: x['sort_key'])
        return chart_data
        
    except Exception as e:
        print(f"Erro na geração de dados do gráfico: {e}")
        return []

@app.route('/dashboard/<session_id>')
def dashboard(session_id):
    try:
        # Carrega dados da sessão
        session_data, results = load_session_data(session_id)
        
        if not session_data:
            flash('Sessão não encontrada.', 'error')
            return redirect(url_for('home'))
        
        # Calcula métricas sem filtros (dados iniciais)
        metrics = calculate_metrics(results)
        chart_data = get_chart_data(results)
        
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

@app.route('/api/dashboard_data/<session_id>')
def api_dashboard_data(session_id):
    """API para dados filtrados do dashboard"""
    try:
        year_filter = request.args.get('year')
        month_filter = request.args.get('month')
        
        print(f"🔍 Filtros recebidos - Ano: {year_filter}, Mês: {month_filter}")
        
        # Carrega dados da sessão
        session_data, results = load_session_data(session_id)
        
        if not session_data:
            return jsonify({'error': 'Sessão não encontrada'}), 404
        
        # Calcula métricas com filtros
        metrics = calculate_metrics(results, year_filter, month_filter)
        chart_data = get_chart_data(results, year_filter, month_filter)
        
        print(f"📊 Dados filtrados - Total: {metrics['formatted_total']}, Arquivos: {metrics['file_count']}")
        
        return jsonify({
            'success': True,
            'metrics': metrics,
            'chart_data': chart_data
        })
        
    except Exception as e:
        print(f"Erro na API de dados do dashboard: {e}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/api/quality_report/<session_id>')
def api_quality_report(session_id):
    """API para relatório de qualidade"""
    try:
        session_data, results = load_session_data(session_id)
        
        if not session_data:
            return jsonify({'error': 'Sessão não encontrada'}), 404
        
        # Análise de qualidade
        total_files = len(results)
        successful_files = len([r for r in results if r.get('success', False)])
        files_with_warnings = len([r for r in results if r.get('warnings') and len(r.get('warnings', [])) > 0])
        
        # Estatísticas dos valores
        successful_results = [r for r in results if r.get('success', False) and r.get('total_value', 0) > 0]
        values = [r.get('total_value', 0) for r in successful_results]
        
        value_statistics = {}
        if values:
            values_sorted = sorted(values)
            value_statistics = {
                'count': len(values),
                'total': sum(values),
                'average': sum(values) / len(values),
                'median': values_sorted[len(values_sorted) // 2] if values_sorted else 0,
                'max': max(values),
                'min': min(values)
            }
        
        # Problemas mais comuns
        all_warnings = []
        for r in results:
            if r.get('warnings'):
                all_warnings.extend(r.get('warnings', []))
        
        if r.get('error'):
            all_warnings.append(r.get('error'))
        
        common_issues = Counter(all_warnings).most_common(10)
        
        # Distribuição por anos
        years = [r.get('year') for r in results if r.get('year')]
        years_distribution = Counter(years).most_common()
        
        # Recomendações
        recommendations = []
        
        if files_with_warnings > total_files * 0.3:
            recommendations.append({
                'type': 'warning',
                'title': 'Muitos Arquivos com Alertas',
                'description': f'{files_with_warnings} de {total_files} arquivos têm alertas. Verifique a nomeação e estrutura dos arquivos.',
                'files': [r.get('filename') for r in results if r.get('warnings')][:5]
            })
        
        if successful_files < total_files * 0.8:
            recommendations.append({
                'type': 'error',
                'title': 'Taxa de Sucesso Baixa',
                'description': f'Apenas {successful_files} de {total_files} arquivos foram processados com sucesso.',
                'files': [r.get('filename') for r in results if not r.get('success', False)][:5]
            })
        
        if value_statistics and value_statistics.get('count', 0) > 0:
            avg_value = value_statistics['average']
            outliers = [r for r in successful_results if abs(r.get('total_value', 0) - avg_value) > avg_value * 2]
            if outliers:
                recommendations.append({
                    'type': 'info',
                    'title': 'Valores Atípicos Detectados',
                    'description': f'{len(outliers)} arquivo(s) com valores muito diferentes da média.',
                    'files': [r.get('filename') for r in outliers][:3]
                })
        
        return jsonify({
            'success': True,
            'summary': {
                'total_files': total_files,
                'successful_files': successful_files,
                'files_with_warnings': files_with_warnings,
                'success_rate': (successful_files / total_files * 100) if total_files > 0 else 0,
                'warning_rate': (files_with_warnings / total_files * 100) if total_files > 0 else 0
            },
            'value_statistics': value_statistics,
            'common_issues': common_issues,
            'years_distribution': years_distribution,
            'recommendations': recommendations
        })
        
    except Exception as e:
        print(f"Erro no relatório de qualidade: {e}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/edit_session/<session_id>', methods=['GET', 'POST'])
def edit_session(session_id):
    """Edita informações da sessão"""
    try:
        if request.method == 'POST':
            new_title = request.form.get('title', '').strip()
            new_description = request.form.get('description', '').strip()
            
            if not new_title:
                flash('Título é obrigatório.', 'error')
                return redirect(url_for('edit_session', session_id=session_id))
            
            conn = sqlite3.connect(DATABASE_PATH)
            cursor = conn.cursor()
            
            cursor.execute('''
                UPDATE sessions 
                SET title = ?, description = ?, updated_at = CURRENT_TIMESTAMP 
                WHERE id = ?
            ''', (new_title, new_description, session_id))
            
            conn.commit()
            conn.close()
            
            flash('Relatório atualizado com sucesso!', 'success')
            return redirect(url_for('dashboard', session_id=session_id))
        
        # GET - Carrega dados para edição
        session_data, _ = load_session_data(session_id)
        if not session_data:
            flash('Sessão não encontrada.', 'error')
            return redirect(url_for('home'))
        
        return render_template('edit_session.html', session_data=session_data)
        
    except Exception as e:
        flash('Erro ao editar sessão.', 'error')
        return redirect(url_for('home'))

@app.route('/delete_session/<session_id>', methods=['POST'])
def delete_session(session_id):
    """Deleta uma sessão E remove os arquivos físicos"""
    try:
        # Remove arquivos físicos primeiro
        cleanup_session_files(session_id)
        
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        # Remove registros dos arquivos processados
        cursor.execute('DELETE FROM processed_files WHERE session_id = ?', (session_id,))
        
        # Marca sessão como deletada (soft delete)
        cursor.execute('UPDATE sessions SET status = ? WHERE id = ?', ('deleted', session_id))
        
        conn.commit()
        conn.close()
        
        flash('Sessão e arquivos deletados com sucesso.', 'success')
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
            save_processed_file(new_session_id, result, result.get('stored_filename', ''))
        
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
                'Período': f"{r.get('month'):02d}/{r.get('year')}" if r.get('month') and r.get('year') else '-',
                'Data Emissão': r.get('emission_date') or '-',
                'Data Vencimento': r.get('due_date') or '-',
                'Valor Total': r.get('total_value', 0.0),
                'Qualidade': r.get('data_quality'),
                'Avisos': '; '.join(r.get('warnings', []) if isinstance(r.get('warnings'), list) else [])
            })

        import pandas as pd
        import io
        from flask import send_file

        df = pd.DataFrame(rows)

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
                            .groupby(['Período'], dropna=False)['Valor'].sum()
                            .reset_index()
                            .sort_values(by=['Período']))
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

# Funções de processamento melhoradas

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
        
        # Valida datas de emissão e vencimento
        if result.get('emission_date') and result.get('due_date'):
            try:
                emission = datetime.strptime(result['emission_date'], '%d/%m/%Y')
                due = datetime.strptime(result['due_date'], '%d/%m/%Y')
                if due < emission:
                    warnings.append("⚠️ Data de vencimento anterior à emissão")
            except:
                pass
        
        result['warnings'] = warnings
        result['data_quality'] = 'good' if len(warnings) == 0 else 'warning' if len(warnings) <= 2 else 'poor'
        
        return result
        
    except Exception as e:
        print(f"Erro na validação de dados: {e}")
        result['warnings'] = [f'Erro na validação: {str(e)}']
        result['data_quality'] = 'error'
        return result

def process_file(filepath, original_name):
    """Processa um único arquivo com extração melhorada de datas"""
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
        emission_date, due_date = extract_dates_improved(df)
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
            'formatted_date': format_date_period_br(month, year),
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
            'emission_date': None,
            'due_date': None,
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
    
    # Adicione esta função no seu app.py

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
        
        # Carrega arquivos processados ORDENADOS POR ANO E MÊS
        cursor.execute('''
            SELECT * FROM processed_files 
            WHERE session_id = ? 
            ORDER BY 
                CASE WHEN year_ref IS NULL THEN 1 ELSE 0 END,
                year_ref ASC,
                CASE WHEN month_ref IS NULL THEN 1 ELSE 0 END,
                month_ref ASC,
                processed_at ASC
        ''', (session_id,))
        files_rows = cursor.fetchall()
        
        results = []
        for row in files_rows:
            warnings = json.loads(row[12]) if row[12] else []
            result = {
                'filename': row[3],  # original_filename
                'stored_filename': row[2],  # filename no disco
                'sheet_name': row[4],
                'total_value': row[5],
                'emission_date': format_date_br(row[6]) if row[6] else None,
                'due_date': format_date_br(row[7]) if row[7] else None,
                'month': row[8],
                'year': row[9],
                'success': bool(row[10]),
                'error': row[11],
                'warnings': warnings,
                'data_quality': row[13],
                'formatted_date': format_date_period_br(row[8], row[9]) if row[8] and row[9] else '-',
                'formatted_value': format_currency_br(row[5])
            }
            results.append(result)
        
        conn.close()
        return session_data, results
        
    except Exception as e:
        print(f"Erro ao carregar sessão: {e}")
        return None, []

def extract_dates_improved(df):
    """Extrai datas do DataFrame com melhor formatação"""
    try:
        emission_date = None
        due_date = None
        
        print("🔍 Procurando datas na planilha...")
        
        # Busca em todas as células por datas
        for idx, row in df.iterrows():
            for col in df.columns:
                cell_value = row[col]
                if pd.isna(cell_value):
                    continue
                    
                cell_str = str(cell_value).lower()
                
                # Verifica se é uma data válida
                date_obj = None
                try:
                    # Tenta converter diretamente se for datetime
                    if hasattr(cell_value, 'strftime'):
                        date_obj = cell_value
                    else:
                        # Tenta vários formatos de data
                        date_str = str(cell_value).strip()
                        if len(date_str) >= 8:  # Pelo menos 8 caracteres para uma data
                            for fmt in ['%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y', '%m/%d/%Y']:
                                try:
                                    date_obj = datetime.strptime(date_str, fmt)
                                    break
                                except:
                                    continue
                                    
                    if date_obj:
                        formatted_date = date_obj.strftime('%d/%m/%Y')
                        
                        # Busca contexto para identificar tipo de data
                        context = []
                        # Verifica célula anterior e posterior
                        if idx > 0:
                            prev_cell = str(df.iloc[idx-1, df.columns.get_loc(col)]).lower()
                            context.append(prev_cell)
                        if idx < len(df) - 1:
                            next_cell = str(df.iloc[idx+1, df.columns.get_loc(col)]).lower()
                            context.append(next_cell)
                        # Verifica nome da coluna
                        context.append(str(col).lower())
                        
                        context_str = ' '.join(context)
                        
                        # Identifica se é data de emissão
                        if any(term in context_str for term in ['emissão', 'emissao', 'emitido', 'emission']):
                            if not emission_date:
                                emission_date = formatted_date
                                print(f"📅 Data de emissão encontrada: {emission_date}")
                        
                        # Identifica se é data de vencimento
                        elif any(term in context_str for term in ['vencimento', 'vence', 'due', 'expir']):
                            if not due_date:
                                due_date = formatted_date
                                print(f"📅 Data de vencimento encontrada: {due_date}")
                        
                        # Se não tem contexto específico, usa a primeira como emissão e segunda como vencimento
                        elif not emission_date and not due_date:
                            emission_date = formatted_date
                            print(f"📅 Primeira data encontrada (assumindo emissão): {emission_date}")
                        elif emission_date and not due_date:
                            due_date = formatted_date
                            print(f"📅 Segunda data encontrada (assumindo vencimento): {due_date}")
                            
                except Exception as date_error:
                    continue
        
        # Busca em colunas específicas se não encontrou
        if not emission_date or not due_date:
            for col in df.columns:
                col_name = str(col).lower()
                try:
                    if any(term in col_name for term in ['emissão', 'emissao', 'emitido']) and not emission_date:
                        date_series = pd.to_datetime(df[col], errors='coerce')
                        valid_date = date_series.dropna().iloc[0] if not date_series.dropna().empty else None
                        if valid_date:
                            emission_date = valid_date.strftime('%d/%m/%Y')
                            print(f"📅 Data de emissão da coluna {col}: {emission_date}")
                    
                    if any(term in col_name for term in ['vencimento', 'vence']) and not due_date:
                        date_series = pd.to_datetime(df[col], errors='coerce')
                        valid_date = date_series.dropna().iloc[0] if not date_series.dropna().empty else None
                        if valid_date:
                            due_date = valid_date.strftime('%d/%m/%Y')
                            print(f"📅 Data de vencimento da coluna {col}: {due_date}")
                except:
                    continue
        
        print(f"✅ Extração de datas concluída - Emissão: {emission_date}, Vencimento: {due_date}")
        return emission_date, due_date
        
    except Exception as e:
        print(f"Erro na extração de datas: {e}")
        return None, None

if __name__ == '__main__':
    print("🚀 Iniciando Sistema Financeiro com Datas Melhoradas...")
    print("📍 Acesse: http://localhost:5000")
    print("💾 Banco de dados: financial_reports.db")
    print("📅 Formatação de datas: dd/mm/aaaa")
    app.run(host='0.0.0.0', port=5000, debug=True)