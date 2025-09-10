from flask import Flask, render_template, redirect, url_for, flash
import sqlite3
import os

app = Flask(__name__)
app.secret_key = 'your-secret-key-here'

DATABASE_PATH = 'financial_reports.db'

def format_currency_br(value):
    """Formata valor para padrão brasileiro: R$ 1.234.567,89"""
    try:
        if value is None or value == 0:
            return "R$ 0,00"
        return f"R$ {value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except:
        return "R$ 0,00"

@app.route('/')
def home():
    """Página inicial com DEBUG para verificar sessões"""
    try:
        print("🔍 DEBUG: Verificando sessões no banco...")
        
        # Verifica se o banco existe
        if not os.path.exists(DATABASE_PATH):
            print(f"❌ Banco de dados não encontrado: {DATABASE_PATH}")
            flash('Banco de dados não encontrado. Crie um relatório primeiro.', 'warning')
            return render_template('home.html', sessions=[])
        
        # Conecta ao banco
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        # Lista todas as tabelas
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        print(f"📊 Tabelas no banco: {tables}")
        
        # Verifica quantas sessões existem
        cursor.execute('SELECT COUNT(*) FROM sessions')
        total_sessions = cursor.fetchone()[0]
        print(f"📈 Total de sessões na tabela: {total_sessions}")
        
        # Lista todas as sessões (incluindo deletadas)
        cursor.execute('SELECT id, title, status, created_at FROM sessions ORDER BY created_at DESC')
        all_sessions = cursor.fetchall()
        print(f"📋 Todas as sessões:")
        for session in all_sessions:
            print(f"  - {session[0][:8]}... | {session[1]} | Status: {session[2]}")
        
        # Carrega sessões ativas para o template
        cursor.execute('''
            SELECT s.*, COUNT(pf.id) as file_count, SUM(pf.total_value) as total_sum
            FROM sessions s
            LEFT JOIN processed_files pf ON s.id = pf.session_id AND pf.success = 1
            WHERE s.status = 'active'
            GROUP BY s.id
            ORDER BY s.updated_at DESC
            LIMIT 20
        ''')
        
        rows = cursor.fetchall()
        print(f"🎯 Sessões ativas encontradas: {len(rows)}")
        
        sessions = []
        for row in rows:
            session_data = {
                'id': row[0],
                'title': row[1],
                'description': row[2],
                'created_at': row[3],
                'updated_at': row[4],
                'file_count': row[6] or 0,
                'total_value': row[7] or 0,
                'status': row[8],
                'formatted_total': format_currency_br(row[7] or 0)
            }
            sessions.append(session_data)
            print(f"  ✅ {session_data['title']} - {session_data['file_count']} arquivos - {session_data['formatted_total']}")
        
        conn.close()
        
        if not sessions:
            print("⚠️ Nenhuma sessão ativa encontrada")
            flash('Nenhum relatório ativo encontrado.', 'info')
        else:
            print(f"✅ {len(sessions)} sessões serão exibidas no template")
        
        return render_template('home.html', sessions=sessions)
        
    except Exception as e:
        print(f"💥 ERRO na função home(): {str(e)}")
        import traceback
        traceback.print_exc()
        flash(f'Erro ao carregar relatórios: {str(e)}', 'error')
        return render_template('home.html', sessions=[])

@app.route('/debug')
def debug():
    """Página de debug para verificar o banco"""
    try:
        if not os.path.exists(DATABASE_PATH):
            return f"<h1>Banco de dados não existe: {DATABASE_PATH}</h1>"
        
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        html = "<h1>DEBUG - Banco de Dados</h1>"
        
        # Lista tabelas
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        html += f"<h2>Tabelas: {tables}</h2>"
        
        # Sessions
        cursor.execute('SELECT * FROM sessions ORDER BY created_at DESC LIMIT 10')
        sessions = cursor.fetchall()
        html += f"<h2>Sessões ({len(sessions)}):</h2><ul>"
        for s in sessions:
            html += f"<li>{s[0][:8]}... - {s[1]} - Status: {s[7]} - Criado: {s[3]}</li>"
        html += "</ul>"
        
        # Processed files
        cursor.execute('SELECT COUNT(*) FROM processed_files')
        file_count = cursor.fetchone()[0]
        html += f"<h2>Arquivos processados: {file_count}</h2>"
        
        cursor.execute('SELECT session_id, original_filename, success, total_value FROM processed_files LIMIT 20')
        files = cursor.fetchall()
        html += "<ul>"
        for f in files:
            html += f"<li>Sessão: {f[0][:8]}... - {f[1]} - Sucesso: {f[2]} - Valor: {f[3]}</li>"
        html += "</ul>"
        
        conn.close()
        
        html += '<br><a href="/">Voltar ao Home</a>'
        return html
        
    except Exception as e:
        return f"<h1>Erro no debug: {str(e)}</h1>"

@app.route('/fix_sessions')
def fix_sessions():
    """Tenta corrigir sessões com status None ou em branco"""
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        # Corrige sessões sem status
        cursor.execute("UPDATE sessions SET status = 'active' WHERE status IS NULL OR status = ''")
        updated = cursor.rowcount
        
        conn.commit()
        conn.close()
        
        flash(f'Corrigidas {updated} sessões sem status.', 'success')
        return redirect('/')
        
    except Exception as e:
        flash(f'Erro ao corrigir sessões: {str(e)}', 'error')
        return redirect('/')

@app.route('/upload')
def upload_page():
    return """
    <h1>Página de Upload (Simples)</h1>
    <p>Esta é uma versão básica da página de upload para testar.</p>
    <a href="/">Voltar ao Home</a>
    """

if __name__ == '__main__':
    print("🔍 Iniciando app de DEBUG...")
    print(f"📁 Procurando banco em: {os.path.abspath(DATABASE_PATH)}")
    print("🌐 Acesse: http://localhost:5000")
    print("🔧 Debug: http://localhost:5000/debug")
    print("🛠️ Corrigir: http://localhost:5000/fix_sessions")
    
    app.run(host='0.0.0.0', port=5000, debug=True)