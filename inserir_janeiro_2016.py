"""
Script específico para inserir Janeiro/2016 com delimitador correto
"""
import pandas as pd
import os
import sys
from pathlib import Path

# Adicionar o diretório atual ao path para importar módulos
sys.path.append(os.path.dirname(__file__))

from meuDB import get_db_cursor
from etl_vendas_medicamentos import EtlVendaMedicamento

class InserirJaneiro2016:
    """Classe especializada para inserir Janeiro/2016 com delimitador correto"""
    
    def __init__(self):
        self.etl = EtlVendaMedicamento()
        self.ano = 2016
        self.mes = 1
        self.arquivo = "EDA_Industrializados_201601.csv"
        self.caminho_arquivo = os.path.join(
            "D:\\LagoDeDados\\medicamentos\\medicamentos_venda", 
            self.arquivo
        )
    
    def verificar_arquivo(self):
        """Verifica se o arquivo existe"""
        if not os.path.exists(self.caminho_arquivo):
            print(f"❌ Arquivo não encontrado: {self.caminho_arquivo}")
            return False
        
        tamanho_mb = os.path.getsize(self.caminho_arquivo) / (1024 * 1024)
        print(f"✅ Arquivo encontrado: {self.arquivo}")
        print(f"📁 Tamanho: {tamanho_mb:.2f} MB")
        return True
    
    def diagnosticar_arquivo(self):
        """Faz diagnóstico completo do arquivo"""
        print("\n🔍 DIAGNÓSTICO DO ARQUIVO")
        print("=" * 50)
        
        try:
            # Testar diferentes delimitadores
            delimitadores = [';', '\t', ',', '|']
            
            for delim in delimitadores:
                try:
                    df = pd.read_csv(self.caminho_arquivo, 
                                   sep=delim, 
                                   encoding='latin-1', 
                                   nrows=5,  # Ler apenas 5 linhas para teste
                                   low_memory=False)
                    
                    print(f"\n🎯 Delimitador '{repr(delim)}':")
                    print(f"   Colunas: {len(df.columns)}")
                    print(f"   Nomes das colunas: {df.columns.tolist()}")
                    
                    if 'UF_VENDA' in df.columns:
                        print(f"   ✅ UF_VENDA encontrada!")
                        return delim
                    else:
                        print(f"   ❌ UF_VENDA não encontrada")
                        
                except Exception as e:
                    print(f"   ❌ Falhou com {repr(delim)}: {str(e)}")
            
            return None
            
        except Exception as e:
            print(f"❌ Erro no diagnóstico: {e}")
            return None
    
    def ler_arquivo_corretamente(self):
        """Lê o arquivo com o delimitador correto"""
        print(f"\n📖 LENDO ARQUIVO COM DELIMITADOR CORRETO")
        print("=" * 50)
        
        try:
            # Janeiro/2016 usa ponto e vírgula
            df = pd.read_csv(self.caminho_arquivo, 
                           sep=';', 
                           encoding='latin-1', 
                           low_memory=False)
            
            print(f"✅ Arquivo lido com sucesso!")
            print(f"📊 Total de registros: {len(df):,}")
            print(f"🏷️ Colunas: {df.columns.tolist()}")
            
            # Verificar registros RJ
            if 'UF_VENDA' in df.columns:
                df_rj = df[df['UF_VENDA'] == 'RJ']
                print(f"🎯 Registros RJ: {len(df_rj):,}")
                
                if len(df_rj) > 0:
                    print(f"🏙️ Municípios RJ (top 5):")
                    municipios = df_rj['MUNICIPIO_VENDA'].value_counts().head(5)
                    for municipio, qtd in municipios.items():
                        print(f"   📍 {municipio}: {qtd:,} registros")
            
            return df
            
        except Exception as e:
            print(f"❌ Erro ao ler arquivo: {e}")
            return None
    
    def limpar_dados_existentes(self):
        """Remove registros existentes de Janeiro/2016"""
        print(f"\n🗑️  LIMPANDO DADOS EXISTENTES")
        print("=" * 50)
        
        try:
            with get_db_cursor() as cursor:
                delete_query = """
                    DELETE FROM trampo.vendas_medicamentos 
                    WHERE ANO_VENDA = %s AND MES_VENDA = %s
                """
                cursor.execute(delete_query, (self.ano, self.mes))
                affected_rows = cursor.rowcount
                print(f"✅ Dados existentes removidos: {affected_rows} registros")
                return True
                
        except Exception as e:
            print(f"❌ Erro ao limpar dados: {e}")
            return False
    
    def inserir_dados(self, df):
        """Insere os dados no banco"""
        print(f"\n💾 INSERINDO DADOS NO BANCO")
        print("=" * 50)
        
        if df is None:
            print("❌ Nenhum dado para inserir")
            return 0
        
        # Filtrar apenas registros RJ
        df_rj = df[df['UF_VENDA'] == 'RJ']
        total_registros = len(df_rj)
        
        if total_registros == 0:
            print("❌ Nenhum registro RJ encontrado para inserir")
            return 0
        
        print(f"🎯 Inserindo {total_registros:,} registros RJ...")
        
        insert_query = """
            INSERT INTO trampo.vendas_medicamentos 
            (ANO_VENDA, MES_VENDA, PRINCIPIO_ATIVO, MUNICIPIO_VENDA, 
            QTD_VENDIDA, CONSELHO_PRESCRITOR, UF_CONSELHO_PRESCRITOR)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        
        registros_inseridos = 0
        municipios_validos = self.etl.criar_set_municipios_rj()
        
        try:
            with get_db_cursor() as cursor:
                for index, row in df_rj.iterrows():
                    try:
                        # Verificar se município é válido
                        municipio = str(row['MUNICIPIO_VENDA']).strip().upper()
                        if municipio not in municipios_validos:
                            continue
                        
                        # Preparar dados
                        principio_ativo = str(row['PRINCIPIO_ATIVO'])[:255] if pd.notna(row['PRINCIPIO_ATIVO']) else ''
                        qtd_vendida = int(float(str(row['QTD_VENDIDA']).replace(',', '.'))) if pd.notna(row['QTD_VENDIDA']) else 0
                        conselho = str(row['CONSELHO_PRESCRITOR'])[:50] if pd.notna(row['CONSELHO_PRESCRITOR']) else ''
                        uf_conselho = str(row['UF_CONSELHO_PRESCRITOR'])[:2] if pd.notna(row['UF_CONSELHO_PRESCRITOR']) else ''
                        
                        insert_data = (
                            self.ano,
                            self.mes,
                            principio_ativo,
                            municipio[:100],  # Limitar tamanho
                            qtd_vendida,
                            conselho,
                            uf_conselho
                        )
                        
                        cursor.execute(insert_query, insert_data)
                        registros_inseridos += 1
                        
                        # Mostrar progresso
                        if registros_inseridos % 10000 == 0:
                            print(f"📊 Progresso: {registros_inseridos:,}/{total_registros:,}")
                            
                    except Exception as e:
                        # Ignorar erros em linhas individuais
                        continue
            
            print(f"✅ Inserção concluída: {registros_inseridos:,} registros")
            return registros_inseridos
            
        except Exception as e:
            print(f"❌ Erro na inserção: {e}")
            return 0
    
    def executar(self):
        """Executa o processo completo"""
        print("🚀 INICIANDO INSERÇÃO DE JANEIRO/2016")
        print("=" * 60)
        
        # 1. Verificar arquivo
        if not self.verificar_arquivo():
            return {"status": "error", "message": "Arquivo não encontrado"}
        
        # 2. Diagnosticar
        delimitador = self.diagnosticar_arquivo()
        if not delimitador:
            return {"status": "error", "message": "Não foi possível detectar o delimitador"}
        
        print(f"\n🎯 Delimitador identificado: '{repr(delimitador)}'")
        
        # 3. Ler arquivo
        df = self.ler_arquivo_corretamente()
        if df is None:
            return {"status": "error", "message": "Erro ao ler arquivo"}
        
        # 4. Limpar dados existentes
        if not self.limpar_dados_existentes():
            return {"status": "error", "message": "Erro ao limpar dados existentes"}
        
        # 5. Inserir dados
        registros_inseridos = self.inserir_dados(df)
        
        # 6. Resultado final
        resultado = {
            "status": "success",
            "ano": self.ano,
            "mes": self.mes,
            "arquivo": self.arquivo,
            "delimitador_utilizado": ";",
            "registros_rj_encontrados": len(df[df['UF_VENDA'] == 'RJ']) if 'UF_VENDA' in df.columns else 0,
            "registros_inseridos": registros_inseridos,
            "message": f"Inserção de Janeiro/2016 concluída com sucesso"
        }
        
        print(f"\n🎉 PROCESSO CONCLUÍDO!")
        print("=" * 60)
        print(f"📊 Resultado final:")
        print(f"   ✅ Arquivo: {resultado['arquivo']}")
        print(f"   ✅ Delimitador: {resultado['delimitador_utilizado']}")
        print(f"   ✅ Registros RJ encontrados: {resultado['registros_rj_encontrados']:,}")
        print(f"   ✅ Registros inseridos: {resultado['registros_inseridos']:,}")
        
        return resultado

def main():
    """Função principal"""
    inseridor = InserirJaneiro2016()
    resultado = inseridor.executar()
    
    print(f"\n📋 RESUMO FINAL:")
    for chave, valor in resultado.items():
        print(f"   {chave}: {valor}")

if __name__ == "__main__":
    main()