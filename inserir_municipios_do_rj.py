import pandas as pd
import os
import sys

# Adicionar o diretório atual ao path para importar meuDB
sys.path.append(os.path.dirname(__file__))

from meuDB import get_db_cursor

def inserir_municipios_rj():
    """
    Insere os municípios do RJ na tabela trampo.municipios_rj
    """
    caminho_csv = "municipios_rj.csv"
    
    print("🏙️ INICIANDO INSERÇÃO DE MUNICÍPIOS DO RJ")
    print("=" * 50)
    
    try:
        # Verificar se o arquivo existe
        if not os.path.exists(caminho_csv):
            print(f"❌ Arquivo não encontrado: {caminho_csv}")
            print("📁 Arquivos no diretório atual:")
            for file in os.listdir('.'):
                if file.endswith('.csv'):
                    print(f"   📄 {file}")
            return {"status": "error", "message": f"Arquivo {caminho_csv} não encontrado"}
        
        # ⚠️ CORREÇÃO: Usar latin-1 em vez de utf-8
        print("🔤 Tentando encoding: latin-1")
        df = pd.read_csv(caminho_csv, sep=';', encoding='latin-1')
        print(f"📊 CSV lido: {len(df)} municípios encontrados")
        
        # Verificar estrutura
        print("📋 Primeiras linhas do CSV:")
        print(df.head())
        
        # Conectar ao banco e inserir
        with get_db_cursor() as cursor:
            # Limpar tabela existente (opcional)
            cursor.execute("DELETE FROM trampo.municipios_rj")
            print("🗑️  Tabela limpa")
            
            # Query de inserção
            insert_query = """
                INSERT INTO trampo.municipios_rj (ID, NOME)
                VALUES (%s, %s)
            """
            
            registros_inseridos = 0
            erros = []
            
            # Inserir cada município
            for index, row in df.iterrows():
                try:
                    cursor.execute(insert_query, (int(row['id']), row['MUNICIPIO_VENDA']))
                    registros_inseridos += 1
                    
                    # Mostrar progresso a cada 10 registros
                    if registros_inseridos % 10 == 0:
                        print(f"📝 Inseridos: {registros_inseridos}/{len(df)}")
                        
                except Exception as e:
                    erro_msg = f"❌ Erro ao inserir {row['id']} - {row['MUNICIPIO_VENDA']}: {e}"
                    erros.append(erro_msg)
                    print(erro_msg)
            
            print(f"\n✅ INSERÇÃO CONCLUÍDA")
            print(f"📈 Registros inseridos: {registros_inseridos}/{len(df)}")
            
            if erros:
                print(f"⚠️  Erros encontrados: {len(erros)}")
                for erro in erros[:5]:  # Mostrar apenas primeiros 5 erros
                    print(f"   {erro}")
            
            # Verificar inserção
            cursor.execute("SELECT COUNT(*) as total FROM trampo.municipios_rj")
            total_tabela = cursor.fetchone()['total']
            print(f"📊 Total na tabela: {total_tabela} municípios")
            
            # Listar alguns municípios inseridos
            cursor.execute("SELECT ID, NOME FROM trampo.municipios_rj ORDER BY ID LIMIT 5")
            primeiros = cursor.fetchall()
            print("🏙️  Primeiros municípios inseridos:")
            for municipio in primeiros:
                print(f"   {municipio['ID']}: {municipio['NOME']}")
            
            return {
                "status": "success",
                "registros_inseridos": registros_inseridos,
                "total_tabela": total_tabela,
                "erros": len(erros)
            }
            
    except Exception as e:
        print(f"❌ ERRO: {e}")
        import traceback
        traceback.print_exc()
        return {
            "status": "error",
            "message": str(e)
        }

# Executar
if __name__ == "__main__":
    print("🚀 INICIANDO SCRIPT DE INSERÇÃO DE MUNICÍPIOS")
    resultado = inserir_municipios_rj()
    print(f"\n🎯 RESULTADO FINAL: {resultado}")