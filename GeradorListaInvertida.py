import xml.etree.ElementTree as ET
import unicodedata
import logging
import re
import csv
import time
import os
from collections import defaultdict

class GeradorListaInvertida:
    """
    Classe responsável por gerar uma lista invertida a partir de uma coleção de
    documentos em formato XML, conforme especificado no trabalho.
    """
    def __init__(self, config_path="GLI.CFG"):
        """
        Inicializa o gerador.

        :param config_path: Caminho para o arquivo de configuração.
        """
        self.config_path = config_path
        self.arquivos_leitura = []
        self.arquivo_escrita = "resultados\lista_invertida.csv"
        # Usar defaultdict(list) simplifica a adição de novos documentos a uma palavra
        self.lista_invertida = defaultdict(list)
        
        # Configuração do logging conforme especificado no documento
        self.logger = logging.getLogger('GeradorListaInvertida')
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)

        self.logger.info("Módulo GeradorListaInvertida instanciado.")

    def _ler_config(self):
        """
        Lê o arquivo de configuração (GLI.CFG) para obter os caminhos dos arquivos.
        A instrução ESCREVA deve aparecer depois de todas as instruções LEIA.
        """
        self.logger.info(f"Lendo arquivo de configuração: {self.config_path}")
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                for linha in f:
                    linha = linha.strip()
                    if not linha:
                        continue
                    
                    # As instruções são no formato "CHAVE=VALOR"
                    chave, valor = linha.split('=', 1)
                    if chave.upper() == 'LEIA':
                        self.arquivos_leitura.append(valor)
                    elif chave.upper() == 'ESCREVA':
                        self.arquivo_escrita = valor
            
            if not self.arquivos_leitura or not self.arquivo_escrita:
                raise ValueError("Arquivo de configuração incompleto. Instruções LEIA e ESCREVA são obrigatórias.")

            self.logger.info(f"Arquivos a serem lidos: {self.arquivos_leitura}")
            self.logger.info(f"Arquivo de saída: {self.arquivo_escrita}")

        except FileNotFoundError:
            self.logger.error(f"Arquivo de configuração '{self.config_path}' não encontrado.")
            raise
        except Exception as e:
            self.logger.error(f"Erro ao ler arquivo de configuração: {e}")
            raise

    def _normalizar_palavra(self, texto):
        """
        Normaliza o texto: remove acentos, converte para minúsculas,
        remove caracteres não alfabéticos e retorna uma lista de palavras (tokens).
        As regras de normalização do Indexador são aplicadas aqui para consistência.
        """
        # Converte para minúsculas
        texto = texto.lower()
        # Remove acentos
        texto_sem_acentos = ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn')
        # Mantém apenas letras e espaços, e quebra em palavras
        palavras = re.findall(r'\b[a-z]{2,}\b', texto_sem_acentos) # Apenas palavras com 2 letras ou mais
        return palavras

    def _processar_arquivos(self):
        """
        Processa os arquivos XML, extrai os textos e monta a lista invertida.
        """
        self.logger.info("Iniciando processamento dos arquivos XML.")
        total_documentos_processados = 0
        
        for caminho_arquivo in self.arquivos_leitura:
            try:
                self.logger.info(f"Processando arquivo: {caminho_arquivo}")
                tree = ET.parse(caminho_arquivo)
                root = tree.getroot()
                
                for record in root.findall('RECORD'):
                    record_num_element = record.find('RECORDNUM')
                    if record_num_element is None:
                        continue
                    
                    record_num = int(record_num_element.text.strip())
                    
                    texto = ""
                    abstract_element = record.find('ABSTRACT')
                    if abstract_element is not None and abstract_element.text:
                        texto = abstract_element.text
                    else:
                        extract_element = record.find('EXTRACT') # Se não houver ABSTRACT, usa EXTRACT
                        if extract_element is not None and extract_element.text:
                            texto = extract_element.text
                        else:
                            self.logger.warning(f"Documento {record_num} não contém ABSTRACT nem EXTRACT.")
                            continue # Pula para o próximo registro
                    
                    palavras_normalizadas = self._normalizar_palavra(texto)
                    
                    for palavra in palavras_normalizadas:
                        # Adiciona o número do documento à lista daquela palavra.
                        # Se uma palavra aparece várias vezes, o número do doc é adicionado várias vezes.
                        self.lista_invertida[palavra].append(record_num)
                    
                    total_documentos_processados += 1

            except ET.ParseError as e:
                self.logger.error(f"Erro de parsing no XML '{caminho_arquivo}': {e}")
            except Exception as e:
                self.logger.error(f"Erro inesperado ao processar '{caminho_arquivo}': {e}")
        
        self.logger.info(f"Total de documentos processados: {total_documentos_processados}")
        self.logger.info(f"Número de palavras únicas na lista invertida: {len(self.lista_invertida)}")

    def _escrever_saida(self):
        """
        Escreve o conteúdo da lista invertida em um arquivo CSV.
        O caractere de separação será o ";".
        """
        if not self.lista_invertida:
            self.logger.warning("A lista invertida está vazia. Nenhum arquivo de saída será gerado.")
            return

        self.logger.info(f"Escrevendo lista invertida para o arquivo: {self.arquivo_escrita}")
        
        try:
            # **NOVA LÓGICA**: Garante que o diretório de saída exista
            diretorio_saida = os.path.dirname(self.arquivo_escrita)
            if diretorio_saida and not os.path.exists(diretorio_saida):
                self.logger.info(f"Criando diretório de saída: {diretorio_saida}")
                os.makedirs(diretorio_saida)

            with open(self.arquivo_escrita, 'w', newline='', encoding='utf-8') as f:
                # O CSV usará ; como delimitador
                writer = csv.writer(f, delimiter=';')
                
                # Escreve o cabeçalho
                writer.writerow(['Palavra', 'Documentos'])
                
                # Escreve os dados da lista invertida, ordenando as palavras
                for palavra, doc_ids in sorted(self.lista_invertida.items()):
                    # A lista de documentos é salva como uma string no formato de lista Python
                    writer.writerow([palavra.upper(), str(doc_ids)]) # Palavra em letras maiúsculas
            
            self.logger.info("Arquivo de saída gerado com sucesso.")
        except IOError as e:
            self.logger.error(f"Erro de I/O ao escrever o arquivo '{self.arquivo_escrita}': {e}")
            raise

    def executar(self):
        """
        Orquestra a execução completa do módulo, seguindo o princípio de processamento em batch.
        1. Ler todos os dados
        2. Fazer todo o processamento
        3. Salvar todos os dados
        """
        start_time = time.time()
        self.logger.info("Executando o pipeline do Gerador de Lista Invertida.")
        try:
            self._ler_config()
            self._processar_arquivos()
            self._escrever_saida()
        except Exception as e:
            self.logger.critical(f"Pipeline falhou com um erro irrecuperável: {e}")
        finally:
            end_time = time.time()
            total_time = end_time - start_time
            self.logger.info(f"Pipeline concluído em {total_time:.2f} segundos.")


if __name__ == "__main__":
    # Ponto de entrada do script.
    # Cria uma instância da classe e chama o método principal de execução.
    gerador = GeradorListaInvertida(config_path="GLI.CFG")
    gerador.executar()
