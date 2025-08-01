import xml.etree.ElementTree as ET
import unicodedata
import logging
import re
import csv
import time
import os
from collections import defaultdict

class ProcessadorConsultas:
    """
    Classe responsável por processar o arquivo de consultas em formato XML.
    Gera um arquivo com as consultas processadas e outro com os resultados esperados.
    """
    def __init__(self, config_path="PC.CFG"):
        """
        Inicializa o Processador de Consultas.

        :param config_path: Caminho para o arquivo de configuração.
        """
        self.config_path = config_path
        self.arquivo_leitura = ""
        self.arquivo_consultas = ""
        self.arquivo_esperados = ""
        
        self.consultas_processadas = {}
        self.resultados_esperados = defaultdict(list)

        # Configuração do logging
        self.logger = logging.getLogger('ProcessadorConsultas')
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)

        self.logger.info("Módulo ProcessadorConsultas instanciado.")

    def _ler_config(self):
        """Lê o arquivo de configuração (PC.CFG)."""
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
                    elif chave.upper() == 'CONSULTAS':
                        self.arquivo_consultas = valor
                    elif chave.upper() == 'ESPERADOS':
                        self.arquivo_esperados = valor
            
            if not all([self.arquivo_leitura, self.arquivo_consultas, self.arquivo_esperados]):
                raise ValueError("Arquivo de configuração incompleto. Instruções LEIA, CONSULTAS e ESPERADOS são obrigatórias.")

            self.logger.info(f"Arquivo de consultas a ser lido: {self.arquivo_leitura}")
            self.logger.info(f"Arquivo de saída para consultas processadas: {self.arquivo_consultas}")
            self.logger.info(f"Arquivo de saída para resultados esperados: {self.arquivo_esperados}")

        except FileNotFoundError:
            self.logger.error(f"Arquivo de configuração '{self.config_path}' não encontrado.")
            raise
        except Exception as e:
            self.logger.error(f"Erro ao ler arquivo de configuração: {e}")
            raise

    def _normalizar_texto(self, texto):
        """
        Normaliza o texto da consulta usando as mesmas regras dos módulos anteriores
        para garantir consistência.
        """
        # Converte para minúsculas e remove acentos
        texto_sem_acentos = ''.join(c for c in unicodedata.normalize('NFD', texto.lower()) if unicodedata.category(c) != 'Mn')
        # Remove caracteres de pontuação e outros símbolos, mantendo apenas letras e espaços
        texto_limpo = re.sub(r'[^a-z\s]', '', texto_sem_acentos)
        # Remove espaços extras
        return ' '.join(texto_limpo.split())

    def _processar_arquivo_consultas(self):
        """
        Processa o arquivo XML de consultas, extraindo as consultas e os resultados esperados.
        """
        self.logger.info(f"Iniciando processamento do arquivo de consultas: {self.arquivo_leitura}")
        try:
            tree = ET.parse(self.arquivo_leitura)
            root = tree.getroot()
            
            num_consultas_lidas = 0
            for query_element in root.findall('QUERY'):
                num_consultas_lidas += 1
                query_number = int(query_element.find('QueryNumber').text.strip())
                query_text = query_element.find('QueryText').text.strip()
                
                # Processa e armazena a consulta
                consulta_normalizada = self._normalizar_texto(query_text)
                self.consultas_processadas[query_number] = consulta_normalizada.upper() # Conforme especificado: letras maiúsculas
                
                # Processa e armazena os resultados esperados
                records_element = query_element.find('Records')
                if records_element is not None:
                    for item_element in records_element.findall('Item'):
                        score = int(item_element.get('score'))
                        if score > 0: # Considera qualquer score > 0 como um voto
                            doc_number = int(item_element.text.strip())
                            self.resultados_esperados[query_number].append(doc_number)

            self.logger.info(f"Total de {num_consultas_lidas} consultas lidas e processadas.")

        except ET.ParseError as e:
            self.logger.error(f"Erro de parsing no XML '{self.arquivo_leitura}': {e}")
            raise
        except Exception as e:
            self.logger.error(f"Erro inesperado ao processar o arquivo de consultas: {e}")
            raise

    def _escrever_arquivos_saida(self):
        """
        Escreve os dados processados nos respectivos arquivos CSV de saída.
        """
        self.logger.info("Iniciando escrita dos arquivos de saída.")
        
        # Garante que o diretório de saída exista para ambos os arquivos
        for path in [self.arquivo_consultas, self.arquivo_esperados]:
            diretorio_saida = os.path.dirname(path)
            if diretorio_saida and not os.path.exists(diretorio_saida):
                self.logger.info(f"Criando diretório de saída: {diretorio_saida}")
                os.makedirs(diretorio_saida)

        # Escreve o arquivo de consultas processadas
        try:
            with open(self.arquivo_consultas, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f, delimiter=';')
                writer.writerow(['QueryNumber', 'QueryText'])
                for num, texto in sorted(self.consultas_processadas.items()):
                    # Elimina ponto e vírgula do texto para não quebrar o CSV
                    texto_sem_ponto_virgula = texto.replace(';', '')
                    writer.writerow([num, texto_sem_ponto_virgula])
            self.logger.info(f"Arquivo de consultas processadas salvo em '{self.arquivo_consultas}'.")
        except IOError as e:
            self.logger.error(f"Erro ao escrever o arquivo de consultas: {e}")
            raise

        # Escreve o arquivo de resultados esperados
        try:
            with open(self.arquivo_esperados, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f, delimiter=';')
                writer.writerow(['QueryNumber', 'DocNumber', 'DocVotes'])
                for query_num, doc_list in sorted(self.resultados_esperados.items()):
                    # Conta os votos para cada documento
                    doc_votes = defaultdict(int)
                    for doc_id in doc_list:
                        doc_votes[doc_id] += 1
                    
                    for doc_num, votes in sorted(doc_votes.items()):
                        writer.writerow([query_num, doc_num, votes])
            self.logger.info(f"Arquivo de resultados esperados salvo em '{self.arquivo_esperados}'.")
        except IOError as e:
            self.logger.error(f"Erro ao escrever o arquivo de esperados: {e}")
            raise

    def executar(self):
        """
        Orquestra a execução completa do módulo Processador de Consultas.
        """
        start_time = time.time()
        self.logger.info("Executando o pipeline do Processador de Consultas.")
        try:
            self._ler_config()
            self._processar_arquivo_consultas()
            self._escrever_arquivos_saida()
        except Exception as e:
            self.logger.critical(f"Pipeline do Processador de Consultas falhou: {e}")
        finally:
            end_time = time.time()
            total_time = end_time - start_time
            self.logger.info(f"Pipeline do Processador de Consultas concluído em {total_time:.2f} segundos.")


if __name__ == "__main__":
    # Ponto de entrada do script.
    processador = ProcessadorConsultas(config_path="PC.CFG")
    processador.executar()