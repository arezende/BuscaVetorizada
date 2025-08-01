import logging
import pickle
import csv
import time
import os
import math
from collections import defaultdict

class Buscador:
    """
    Classe responsável por realizar buscas em um modelo vetorial salvo.
    Lê um arquivo de consultas, executa cada uma contra o modelo e
    escreve os resultados ranqueados em um arquivo de saída.
    """
    def __init__(self, config_path="BUSCA.CFG"):
        """
        Inicializa o Buscador.

        :param config_path: Caminho para o arquivo de configuração.
        """
        self.config_path = config_path
        self.arquivo_modelo = ""
        self.arquivo_consultas = ""
        self.arquivo_resultados = ""
        
        # Estruturas de dados
        self.modelo_tfidf = None
        self.normas_documentos = None
        self.consultas = {}
        self.resultados_finais = {}

        # Configuração do logging
        self.logger = logging.getLogger('Buscador')
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)

        self.logger.info("Módulo Buscador instanciado.")

    def _ler_config(self):
        """Lê o arquivo de configuração (BUSCA.CFG)."""
        self.logger.info(f"Lendo arquivo de configuração: {self.config_path}")
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                for linha in f:
                    linha = linha.strip()
                    if not linha:
                        continue
                    chave, valor = linha.split('=', 1)
                    if chave.upper() == 'MODELO':
                        self.arquivo_modelo = valor
                    elif chave.upper() == 'CONSULTAS':
                        self.arquivo_consultas = valor
                    elif chave.upper() == 'RESULTADOS':
                        self.arquivo_resultados = valor
            
            if not all([self.arquivo_modelo, self.arquivo_consultas, self.arquivo_resultados]):
                raise ValueError("Arquivo de configuração incompleto. Instruções MODELO, CONSULTAS e RESULTADOS são obrigatórias.")

            self.logger.info(f"Arquivo do modelo: {self.arquivo_modelo}")
            self.logger.info(f"Arquivo de consultas: {self.arquivo_consultas}")
            self.logger.info(f"Arquivo de resultados: {self.arquivo_resultados}")

        except FileNotFoundError:
            self.logger.error(f"Arquivo de configuração '{self.config_path}' não encontrado.")
            raise
        except Exception as e:
            self.logger.error(f"Erro ao ler arquivo de configuração: {e}")
            raise

    def _carregar_modelo(self):
        """Carrega o arquivo de modelo (.pkl) em memória."""
        self.logger.info(f"Carregando modelo de '{self.arquivo_modelo}'...")
        try:
            with open(self.arquivo_modelo, 'rb') as f:
                modelo_completo = pickle.load(f)
                self.modelo_tfidf = modelo_completo['modelo_tfidf']
                self.normas_documentos = modelo_completo['normas_documentos']
            self.logger.info("Modelo carregado com sucesso.")
        except FileNotFoundError:
            self.logger.error(f"Arquivo de modelo '{self.arquivo_modelo}' não encontrado.")
            raise
        except Exception as e:
            self.logger.error(f"Erro ao carregar o modelo: {e}")
            raise

    def _carregar_consultas(self):
        """Carrega o arquivo CSV de consultas em memória."""
        self.logger.info(f"Carregando consultas de '{self.arquivo_consultas}'...")
        try:
            with open(self.arquivo_consultas, 'r', encoding='utf-8') as f:
                reader = csv.reader(f, delimiter=';')
                next(reader) # Pula o cabeçalho
                for linha in reader:
                    query_num, query_text = linha
                    self.consultas[int(query_num)] = query_text
            self.logger.info(f"Total de {len(self.consultas)} consultas carregadas.")
        except FileNotFoundError:
            self.logger.error(f"Arquivo de consultas '{self.arquivo_consultas}' não encontrado.")
            raise
        except Exception as e:
            self.logger.error(f"Erro ao carregar o arquivo de consultas: {e}")
            raise

    def _realizar_buscas(self):
        """
        Executa cada consulta contra o modelo, calcula a similaridade de cosseno
        e armazena os resultados ranqueados.
        """
        self.logger.info("Iniciando a execução das buscas...")
        
        for query_num, query_text in self.consultas.items():
            # CORREÇÃO: Removemos o .lower() pois os termos no modelo e na consulta já estão em maiúsculas.
            termos_consulta = query_text.split()
            
            # Dicionário para armazenar o score de cada documento para esta consulta
            scores_docs = defaultdict(float)
            
            # Cada palavra na consulta tem peso 1
            norma_consulta = math.sqrt(len(termos_consulta))
            
            if norma_consulta == 0:
                continue # Pula consultas vazias

            # Calcula o produto escalar (numerador da similaridade de cosseno)
            for termo in termos_consulta:
                if termo in self.modelo_tfidf:
                    for doc_id, peso in self.modelo_tfidf[termo].items():
                        # O peso do termo na consulta é 1
                        scores_docs[doc_id] += peso * 1
            
            # Calcula a similaridade de cosseno final para cada documento
            for doc_id in scores_docs:
                norma_doc = self.normas_documentos.get(doc_id, 1)
                if norma_doc > 0:
                    scores_docs[doc_id] /= (norma_doc * norma_consulta)

            # Ordena os documentos pelo score em ordem decrescente
            docs_ranqueados = sorted(scores_docs.items(), key=lambda item: item[1], reverse=True)
            
            # Formata a saída conforme especificado: lista de ternos (posição, doc, score)
            resultado_formatado = []
            for i, (doc_id, score) in enumerate(docs_ranqueados):
                posicao = i + 1
                resultado_formatado.append((posicao, doc_id, score))
            
            self.resultados_finais[query_num] = resultado_formatado

        self.logger.info("Execução das buscas concluída.")

    def _escrever_resultados(self):
        """Escreve os resultados ranqueados em um arquivo CSV."""
        if not self.resultados_finais:
            self.logger.warning("Nenhum resultado para escrever.")
            return

        self.logger.info(f"Escrevendo resultados em '{self.arquivo_resultados}'...")
        try:
            # Garante que o diretório de saída exista
            diretorio_saida = os.path.dirname(self.arquivo_resultados)
            if diretorio_saida and not os.path.exists(diretorio_saida):
                self.logger.info(f"Criando diretório de saída: {diretorio_saida}")
                os.makedirs(diretorio_saida)

            with open(self.arquivo_resultados, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f, delimiter=';')
                writer.writerow(['QueryNumber', 'Results'])
                
                for query_num, ranking in sorted(self.resultados_finais.items()):
                    # Converte a lista de ternos para sua representação em string
                    writer.writerow([query_num, str(ranking)])
            
            self.logger.info("Arquivo de resultados salvo com sucesso.")
        except Exception as e:
            self.logger.error(f"Erro ao escrever o arquivo de resultados: {e}")
            raise

    def executar(self):
        """
        Orquestra a execução completa do módulo Buscador.
        """
        start_time = time.time()
        self.logger.info("Executando o pipeline do Buscador.")
        try:
            self._ler_config()
            self._carregar_modelo()
            self._carregar_consultas()
            self._realizar_buscas()
            self._escrever_resultados()
        except Exception as e:
            self.logger.critical(f"Pipeline do Buscador falhou com um erro irrecuperável: {e}")
        finally:
            end_time = time.time()
            total_time = end_time - start_time
            self.logger.info(f"Pipeline do Buscador concluído em {total_time:.2f} segundos.")


if __name__ == "__main__":
    # Ponto de entrada do script.
    buscador = Buscador(config_path="BUSCA.CFG")
    buscador.executar()
