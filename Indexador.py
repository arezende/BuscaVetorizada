import logging
import csv
import math
import pickle
import time
import os
import ast
from collections import defaultdict

class Indexador:
    """
    Classe responsável por criar o modelo vetorial a partir de uma lista invertida.
    Calcula os pesos TF-IDF para cada termo em cada documento.
    """
    def __init__(self, config_path="INDEX.CFG"):
        """
        Inicializa o Indexador.

        :param config_path: Caminho para o arquivo de configuração.
        """
        self.config_path = config_path
        self.arquivo_leitura = ""
        self.arquivo_escrita = ""
        
        # Estruturas de dados para o modelo
        self.lista_invertida = {}
        self.documentos = set()
        self.modelo = defaultdict(dict) # Dicionário aninhado para armazenar os pesos TF-IDF: {termo: {doc_id: peso}}
        self.normas_documentos = defaultdict(float) # Armazena a norma euclidiana de cada vetor de documento

        # Configuração do logging
        self.logger = logging.getLogger('Indexador')
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)

        self.logger.info("Módulo Indexador instanciado.")

    def _ler_config(self):
        """Lê o arquivo de configuração (INDEX.CFG)."""
        self.logger.info(f"Lendo arquivo de configuração: {self.config_path}")
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                for linha in f:
                    linha = linha.strip()
                    if not linha:
                        continue
                    chave, valor = linha.split('=', 1)
                    if chave.upper() == 'LEIA':
                        self.arquivo_leitura = valor
                    elif chave.upper() == 'ESCREVA':
                        self.arquivo_escrita = valor
            
            if not self.arquivo_leitura or not self.arquivo_escrita:
                raise ValueError("Arquivo de configuração incompleto. Instruções LEIA e ESCREVA são obrigatórias.")

            self.logger.info(f"Arquivo de lista invertida a ser lido: {self.arquivo_leitura}")
            self.logger.info(f"Arquivo de modelo a ser salvo: {self.arquivo_escrita}")

        except FileNotFoundError:
            self.logger.error(f"Arquivo de configuração '{self.config_path}' não encontrado.")
            raise
        except Exception as e:
            self.logger.error(f"Erro ao ler arquivo de configuração: {e}")
            raise

    def _carregar_lista_invertida(self):
        """Carrega o arquivo CSV da lista invertida em memória."""
        self.logger.info(f"Carregando lista invertida de '{self.arquivo_leitura}'...")
        try:
            with open(self.arquivo_leitura, 'r', encoding='utf-8') as f:
                reader = csv.reader(f, delimiter=';')
                next(reader) # Pula o cabeçalho
                for linha in reader:
                    palavra, lista_docs_str = linha
                    # ast.literal_eval converte a string '[1, 2, 2]' para uma lista Python [1, 2, 2]
                    lista_docs = ast.literal_eval(lista_docs_str)
                    self.lista_invertida[palavra] = lista_docs
                    self.documentos.update(lista_docs)
            
            self.logger.info(f"Lista invertida carregada. Total de {len(self.lista_invertida)} palavras e {len(self.documentos)} documentos únicos.")

        except FileNotFoundError:
            self.logger.error(f"Arquivo de lista invertida '{self.arquivo_leitura}' não encontrado.")
            raise
        except Exception as e:
            self.logger.error(f"Erro ao carregar ou processar a lista invertida: {e}")
            raise

    def _calcular_pesos(self):
        """
        Calcula os pesos TF-IDF para cada termo em cada documento e as normas dos vetores.
        """
        self.logger.info("Iniciando cálculo dos pesos TF-IDF...")
        
        num_total_documentos = len(self.documentos)
        if num_total_documentos == 0:
            self.logger.warning("Nenhum documento encontrado. O cálculo de pesos será pulado.")
            return

        for palavra, lista_docs in self.lista_invertida.items():
            # ni: número de documentos que contêm o termo i
            num_docs_com_palavra = len(set(lista_docs))
            
            # Cálculo do IDF (Inverse Document Frequency)
            idf = math.log10(num_total_documentos / num_docs_com_palavra)
            
            # Contagem da frequência do termo em cada documento (f_ij)
            frequencia_termo_doc = defaultdict(int)
            for doc_id in lista_docs:
                frequencia_termo_doc[doc_id] += 1

            for doc_id, freq in frequencia_termo_doc.items():
                # Cálculo do TF (Term Frequency) com normalização logarítmica
                tf = 1 + math.log10(freq)
                
                # Cálculo do peso TF-IDF
                peso_tfidf = tf * idf
                
                # Armazena o peso no modelo
                self.modelo[palavra][doc_id] = peso_tfidf

        self.logger.info("Cálculo dos pesos TF-IDF concluído.")
        self.logger.info("Iniciando cálculo das normas dos vetores dos documentos...")

        # Cálculo da norma de cada vetor de documento para uso na similaridade de cosseno
        for palavra in self.modelo:
            for doc_id, peso in self.modelo[palavra].items():
                self.normas_documentos[doc_id] += peso ** 2
        
        for doc_id in self.normas_documentos:
            self.normas_documentos[doc_id] = math.sqrt(self.normas_documentos[doc_id])

        self.logger.info("Cálculo das normas concluído.")

    def _salvar_modelo(self):
        """Salva a estrutura de dados do modelo em um arquivo usando pickle."""
        if not self.modelo:
            self.logger.warning("O modelo está vazio. Nenhum arquivo será salvo.")
            return

        self.logger.info(f"Salvando o modelo em '{self.arquivo_escrita}'...")
        
        try:
            # Garante que o diretório de saída exista
            diretorio_saida = os.path.dirname(self.arquivo_escrita)
            if diretorio_saida and not os.path.exists(diretorio_saida):
                self.logger.info(f"Criando diretório de saída: {diretorio_saida}")
                os.makedirs(diretorio_saida)

            # O objeto a ser salvo contém o modelo TF-IDF e as normas
            modelo_completo = {
                'modelo_tfidf': self.modelo,
                'normas_documentos': self.normas_documentos
            }

            with open(self.arquivo_escrita, 'wb') as f:
                pickle.dump(modelo_completo, f)
            
            self.logger.info("Modelo salvo com sucesso.")
        except Exception as e:
            self.logger.error(f"Erro ao salvar o modelo: {e}")
            raise

    def executar(self):
        """
        Orquestra a execução completa do módulo Indexador.
        """
        start_time = time.time()
        self.logger.info("Executando o pipeline do Indexador.")
        try:
            self._ler_config()
            self._carregar_lista_invertida()
            self._calcular_pesos()
            self._salvar_modelo()
        except Exception as e:
            self.logger.critical(f"Pipeline do Indexador falhou com um erro irrecuperável: {e}")
        finally:
            end_time = time.time()
            total_time = end_time - start_time
            self.logger.info(f"Pipeline do Indexador concluído em {total_time:.2f} segundos.")


if __name__ == "__main__":
    # Ponto de entrada do script.
    indexador = Indexador(config_path="INDEX.CFG")
    indexador.executar()