import pandas as pd
import json
import os
import time
import decimal
from datetime import datetime
from tqdm import tqdm
from meuDB import get_db_cursor
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DecimalEncoder(json.JSONEncoder):
    """Encoder personalizado para converter Decimal para float"""
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

class AgrupadorVendasMedicamentos:
    def __init__(self):
        self.df_final = pd.DataFrame(columns=[
            'ANO_VENDA', 
            'MUNICIPIO_VENDA', 
            'PRINCIPIO_ATIVO', 
            'TOTAL_VENDIDO'
        ])
        self.tempo_espera = 1  # segundos entre consultas

    # No método obter_estatisticas(), modifique:
    def obter_estatisticas(self):
        if self.df_final.empty:
            return {"status": "vazio"}
        
        # Verificação de soma
        soma_dataframe = float(self.df_final['TOTAL_VENDIDO'].sum())
        soma_por_ano = 0
        for ano in self.df_final['ANO_VENDA'].unique():
            df_ano = self.df_final[self.df_final['ANO_VENDA'] == ano]
            soma_por_ano += float(df_ano['TOTAL_VENDIDO'].sum())
        
        print(f"🔍 DEBUG - Soma pelo DataFrame: {soma_dataframe:,.0f}")
        print(f"🔍 DEBUG - Soma por ano: {soma_por_ano:,.0f}")
        print(f"🔍 DEBUG - Diferença: {soma_por_ano - soma_dataframe:,.0f}")
        
        estatisticas = {
            "total_registros": len(self.df_final),
            "municipios_unicos": self.df_final['MUNICIPIO_VENDA'].nunique(),
            "anos_unicos": self.df_final['ANO_VENDA'].nunique(),
            "principios_unicos": self.df_final['PRINCIPIO_ATIVO'].nunique(),
            "total_vendido": soma_dataframe,  # Usar a soma direta do DataFrame
            "data_geracao": datetime.now().isoformat()
        }
        
        return estatisticas

    def analisar_dados_por_ano(self):
        """Faz análise detalhada por ano"""
        if self.df_final.empty:
            print("⚠️ DataFrame vazio")
            return
        
        print("\n🔍 ANÁLISE DETALHADA POR ANO:")
        print("=" * 50)
        
        # Anos presentes
        anos_unicos = sorted(self.df_final['ANO_VENDA'].unique())
        print(f"📅 ANOS NO DATAFRAME: {anos_unicos}")
        
        # Estatísticas por ano
        for ano in anos_unicos:
            df_ano = self.df_final[self.df_final['ANO_VENDA'] == ano]
            total_vendido_ano = df_ano['TOTAL_VENDIDO'].sum()
            municipios_ano = df_ano['MUNICIPIO_VENDA'].nunique()
            
            print(f"\n📊 {ano}:")
            print(f"   📦 Total vendido: {total_vendido_ano:,.0f} unidades")
            print(f"   🏙️  Municípios: {municipios_ano}")
            print(f"   📋 Registros: {len(df_ano):,}")
            
            # Top 3 princípios ativos desse ano
            top_principios = df_ano.groupby('PRINCIPIO_ATIVO')['TOTAL_VENDIDO'].sum().nlargest(3)
            print(f"   💊 Top 3 princípios ativos:")
            for principio, total in top_principios.items():
                print(f"      - {principio}: {total:,.0f}")

    def mostrar_amostra_2016(self, n=5):
        """Mostra amostra específica de 2016"""
        if self.df_final.empty:
            return
        
        df_2016 = self.df_final[self.df_final['ANO_VENDA'] == 2016]
        
        if df_2016.empty:
            print("\n❌ Nenhum dado encontrado para 2016")
            return
        
        print(f"\n🎯 AMOSTRA DE DADOS DE 2016 (primeiros {n} registros):")
        print("=" * 60)
        print(df_2016.head(n).to_string(index=False))
        
        print(f"\n📈 Total de registros de 2016: {len(df_2016):,}")
        print(f"📦 Volume total 2016: {df_2016['TOTAL_VENDIDO'].sum():,.0f} unidades")
        
    def obter_municipios_rj(self):
        """Obtém a lista de municípios do RJ do banco"""
        municipios = []
        
        try:
            with get_db_cursor(dictionary=True) as cursor:
                print("🔄 Consultando municípios do RJ...")
                cursor.execute("SELECT ID, NOME FROM trampo.municipios_rj ORDER BY NOME")
                resultados = cursor.fetchall()
                
                for municipio in resultados:
                    municipios.append({
                        'id': municipio['ID'],
                        'nome': municipio['NOME']
                    })
                
                print(f"✅ Encontrados {len(municipios)} municípios do RJ")
                return municipios
                
        except Exception as e:
            print(f"❌ Erro ao obter municípios: {e}")
            return []
    
    def converter_decimals_para_float(self, dados):
        """Converte valores Decimal para float em uma lista de dicionários"""
        for registro in dados:
            for chave, valor in registro.items():
                if isinstance(valor, decimal.Decimal):
                    registro[chave] = float(valor)
        return dados
    
    def agrupar_por_municipio(self, municipio_nome, tentativa=1):
        """Agrupa vendas para um município específico - COM CONTROLE DE ESPERA"""
        try:
            print(f"🔄 Consultando município: {municipio_nome} (tentativa {tentativa})...")
            
            with get_db_cursor(dictionary=True) as cursor:
                query = """
                    SELECT 
                        ANO_VENDA, 
                        MUNICIPIO_VENDA, 
                        PRINCIPIO_ATIVO,
                        SUM(QTD_VENDIDA) as TOTAL_VENDIDO
                    FROM trampo.vendas_medicamentos  
                    WHERE MUNICIPIO_VENDA = %s
                    GROUP BY ANO_VENDA, MUNICIPIO_VENDA, PRINCIPIO_ATIVO
                    ORDER BY TOTAL_VENDIDO DESC
                """
                
                # Executar consulta e AGUARDAR resultado
                inicio = time.time()
                cursor.execute(query, (municipio_nome,))
                resultados = cursor.fetchall()
                fim = time.time()
                
                # Converter Decimals para float
                resultados = self.converter_decimals_para_float(resultados)
                
                tempo_consulta = fim - inicio
                print(f"✅ Município '{municipio_nome}': {len(resultados)} registros em {tempo_consulta:.2f}s")
                
                return resultados
                
        except Exception as e:
            print(f"❌ Erro ao agrupar município {municipio_nome}: {e}")
            
            # Tentar novamente se for erro de timeout
            if "timeout" in str(e).lower() and tentativa < 3:
                print(f"🔄 Tentando novamente ({tentativa + 1}/3) após {self.tempo_espera}s...")
                time.sleep(self.tempo_espera)
                return self.agrupar_por_municipio(municipio_nome, tentativa + 1)
            else:
                return []
    
    def processar_todos_municipios(self, municipios_limit=None):
        """Processa todos os municípios do RJ - COM CONTROLE DE EXECUÇÃO"""
        municipios = self.obter_municipios_rj()
        
        if not municipios:
            print("❌ Nenhum município encontrado!")
            return False
        
        # Limitar se especificado (para testes)
        if municipios_limit:
            municipios = municipios[:municipios_limit]
            print(f"🔧 Modo teste: processando apenas {municipios_limit} municípios")
        else:
            print(f"🚀 Processando TODOS os {len(municipios)} municípios")
        
        total_registros = 0
        municipios_com_dados = 0
        municipios_sem_dados = 0
        
        print(f"\n🚀 INICIANDO PROCESSAMENTO DE {len(municipios)} MUNICÍPIOS")
        print("=" * 60)
        
        # Barra de progresso com controle de tempo
        with tqdm(total=len(municipios), desc="🏙️ Processando municípios") as pbar:
            for i, municipio in enumerate(municipios, 1):
                nome_municipio = municipio['nome']
                
                print(f"\n[{i}/{len(municipios)}] Processando: {nome_municipio}")
                
                # Agrupar dados do município (AGUARDANDO resultado)
                dados_municipio = self.agrupar_por_municipio(nome_municipio)
                
                if dados_municipio:
                    # Adicionar ao DataFrame
                    df_municipio = pd.DataFrame(dados_municipio)
                    self.df_final = pd.concat([self.df_final, df_municipio], ignore_index=True)
                    
                    total_registros += len(dados_municipio)
                    municipios_com_dados += 1
                    print(f"✅ Adicionados {len(dados_municipio)} registros")
                else:
                    municipios_sem_dados += 1
                    print(f"⚠️  Nenhum dado encontrado para {nome_municipio}")
                
                # Atualizar barra de progresso
                pbar.update(1)
                pbar.set_postfix({
                    'Registros': total_registros, 
                    'ComDados': municipios_com_dados,
                    'SemDados': municipios_sem_dados
                })
                
                # Pequena pausa entre consultas para não sobrecarregar o banco
                if i < len(municipios):  # Não esperar após o último
                    print(f"⏳ Aguardando {self.tempo_espera}s antes do próximo...")
                    time.sleep(self.tempo_espera)
        
        print(f"\n✅ PROCESSAMENTO CONCLUÍDO!")
        print("=" * 50)
        print(f"📈 Total de registros: {total_registros:,}")
        print(f"🏙️ Municípios com dados: {municipios_com_dados}")
        print(f"🏙️ Municípios sem dados: {municipios_sem_dados}")
        print(f"📊 Taxa de sucesso: {(municipios_com_dados/len(municipios))*100:.1f}%")
        
        return True
    
    def salvar_json(self, caminho_arquivo=None):
        """Salva o DataFrame em formato JSON - CORRIGIDO para Decimal"""
        if self.df_final.empty:
            print("⚠️ DataFrame vazio - nada para salvar")
            return False
        
        if not caminho_arquivo:
            os.makedirs('dados_agrupados', exist_ok=True)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            caminho_arquivo = os.path.join('dados_agrupados', f'vendas_agrupadas_{timestamp}.json')
        
        try:
            print(f"💾 Salvando JSON...")
            
            # Converter DataFrame para tipos Python nativos
            df_para_json = self.df_final.copy()
            
            # Converter colunas numéricas para tipos nativos
            df_para_json['ANO_VENDA'] = df_para_json['ANO_VENDA'].astype(int)
            df_para_json['TOTAL_VENDIDO'] = df_para_json['TOTAL_VENDIDO'].astype(float)
            
            # Converter para dict
            dados_json = df_para_json.to_dict('records')
            
            # Salvar arquivo com encoder personalizado
            with open(caminho_arquivo, 'w', encoding='utf-8') as f:
                json.dump(dados_json, f, ensure_ascii=False, indent=2, cls=DecimalEncoder)
            
            # Verificar salvamento
            tamanho_arquivo = os.path.getsize(caminho_arquivo)
            
            print(f"✅ JSON salvo em: {caminho_arquivo}")
            print(f"📏 Tamanho: {tamanho_arquivo:,} bytes")
            print(f"📊 Registros: {len(dados_json):,}")
            
            return caminho_arquivo
            
        except Exception as e:
            print(f"❌ Erro ao salvar JSON: {e}")
            return False
    
    def salvar_csv(self, caminho_arquivo=None):
        """Salva o DataFrame em formato CSV"""
        if self.df_final.empty:
            print("⚠️ DataFrame vazio - nada para salvar")
            return False
        
        if not caminho_arquivo:
            os.makedirs('dados_agrupados', exist_ok=True)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            caminho_arquivo = os.path.join('dados_agrupados', f'vendas_agrupadas_{timestamp}.csv')
        
        try:
            print(f"💾 Salvando CSV...")
            
            # Salvar CSV
            self.df_final.to_csv(caminho_arquivo, index=False, encoding='utf-8')
            
            # Verificar salvamento
            tamanho_arquivo = os.path.getsize(caminho_arquivo)
            
            print(f"✅ CSV salvo em: {caminho_arquivo}")
            print(f"📏 Tamanho: {tamanho_arquivo:,} bytes")
            print(f"📊 Registros: {len(self.df_final):,}")
            
            return caminho_arquivo
            
        except Exception as e:
            print(f"❌ Erro ao salvar CSV: {e}")
            return False
    
    def salvar_ambos_formatos(self):
        """Salva em ambos JSON e CSV e retorna os caminhos"""
        print("\n💾 INICIANDO SALVAMENTO DOS ARQUIVOS")
        print("=" * 40)
        
        json_path = self.salvar_json()
        print("")  # Linha em branco
        
        csv_path = self.salvar_csv()
        
        return json_path, csv_path
    
    def obter_estatisticas(self):
        """Retorna estatísticas do agrupamento"""
        if self.df_final.empty:
            return {"status": "vazio"}
        
        estatisticas = {
            "total_registros": len(self.df_final),
            "municipios_unicos": self.df_final['MUNICIPIO_VENDA'].nunique(),
            "anos_unicos": self.df_final['ANO_VENDA'].nunique(),
            "principios_unicos": self.df_final['PRINCIPIO_ATIVO'].nunique(),
            "total_vendido": float(self.df_final['TOTAL_VENDIDO'].sum()),
            "data_geracao": datetime.now().isoformat()
        }
        
        return estatisticas

def executar_processamento_completo():
    """Executa o processamento completo de TODOS os municípios"""
    print("🚀 INICIANDO PROCESSAMENTO COMPLETO - TODOS OS 92 MUNICÍPIOS")
    print("=" * 60)
    print("⏰ Cada consulta aguardará resultado antes de continuar")
    print("🔄 Tentativas automáticas em caso de erro")
    print("⏳ Pausas entre consultas para não sobrecarregar o banco")
    print("💾 Suporte a Decimal para JSON")
    print("=" * 60)
    
    # Inicializar agrupador
    agrupador = AgrupadorVendasMedicamentos()
    
    # Processar TODOS os municípios (sem limite)
    print("\n🏙️ INICIANDO CONSULTAS POR MUNICÍPIO")
    sucesso = agrupador.processar_todos_municipios()  # Sem limite = todos
    
    if sucesso and not agrupador.df_final.empty:
        print("\n✅ DADOS PROCESSADOS COM SUCESSO!")
        
        # 🆕 NOVO: Análise detalhada
        agrupador.analisar_dados_por_ano()
        agrupador.mostrar_amostra_2016()
        agrupador.obter_estatisticas()
        
        # Salvar resultados
        json_path, csv_path = agrupador.salvar_ambos_formatos()
        
        # Estatísticas finais
        stats = agrupador.obter_estatisticas()
        
        print("\n🎉 PROCESSAMENTO COMPLETO FINALIZADO!")
        print("=" * 50)
        print(f"📊 TOTAL DE REGISTROS: {stats['total_registros']:,}")
        print(f"🏙️ MUNICÍPIOS PROCESSADOS: {stats['municipios_unicos']}/92")
        print(f"📅 ANOS ENCONTRADOS: {stats['anos_unicos']}")
        print(f"💊 PRINCÍPIOS ATIVOS: {stats['principios_unicos']:,}")
        print(f"📦 TOTAL VENDIDO: {stats['total_vendido']:,.0f} unidades")
        print(f"📍 JSON: {json_path}")
        print(f"📍 CSV: {csv_path}")
        
        # Mostrar amostra dos dados
        print(f"\n🔍 AMOSTRA DOS DADOS (primeiras 3 linhas):")
        print(agrupador.df_final.head(3).to_string(index=False))
        
    else:
        print("❌ Nenhum dado foi processado!")



if __name__ == "__main__":
    # Executar processamento COMPLETO (todos os 92 municípios)
    executar_processamento_completo()