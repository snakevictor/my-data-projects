#!/usr/bin/env python

import time
from datetime import datetime
from glob import glob
from os import getcwd
from sys import exit
from typing import Sequence

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, when

"""
Código python para leitura de arquivos txt e tratamento de dados com pyspark
Realizado como teste para o processo seletivo de engenheiro de dados da NAVA
"""

__author__ = "Victor Monteiro Ribeiro"
__version__ = "1.0.0"
__email__ = "victormribeiro.py@gmail.com"

DIRETORIO_LOCAL = getcwd()


def arquivos_para_DFs(
    arquivos: list, spark: SparkSession
) -> dict[datetime | Sequence[str], DataFrame] | str:
    """
    Cria um dicionário de DataFrames a partir de uma lista de arquivos.
    Chaves do dicionário são datas em formato datetime ou nomes de arquivos.
    """
    dict_dfs: dict[datetime | Sequence[str], DataFrame] = {}
    for arquivo in arquivos:
        try:
            df = spark.read.options(
                header="true", inferSchema="true", delimiter=";"
            ).csv(
                # monta o caminho absoluto do arquivo a ser carregado
                DIRETORIO_LOCAL + "\\" + arquivo
            )
            nome = (
                (arquivo.split("\\")[-1]).split(".")[0].split("_")
                if "movimentacao" in arquivo
                else arquivo.split("\\")[-1]
            )
        except Exception as e:
            return f"Não foi possível ler o arquivo {arquivo}: {e}"
        # se nome for lista, então é uma data
        if isinstance(nome, list):
            # formata o nome pra datetime "YYYY-MM-DD"
            nome = "-".join([nome[-1], nome[-2], nome[-3]])
            nome_data = datetime.strptime(nome, "%Y-%m-%d")
            dict_dfs[nome_data] = df
        else:
            dict_dfs[nome] = df
    return dict_dfs


def reformatar_dataframe(chave: datetime | Sequence[str], df: DataFrame) -> DataFrame:
    """
    Reformatando dataframes para formato largo.
    Se a chave for uma data, o dataframe terá as colunas "data" e "Movimentacao_dia".
    Se a chave for um nome, o dataframe terá as colunas "data" e "Saldo_Inicial_CC".
    """
    if isinstance(chave, datetime):
        try:
            # pivotando o dataframe para ter as colunas "data" e "Movimentacao_dia"
            pivoted_df = (
                df.groupBy("CPF", "Nome").pivot("data").agg({"Movimentacao_dia": "sum"})
            )
        except Exception as e:
            print(f"Não foi possível pivotar o dataframe: {e}")
            exit
    else:
        try:
            # pivotando o dataframe para ter as colunas "data" e "Saldo_Inicial_CC"
            pivoted_df = (
                df.groupBy("CPF", "Nome").pivot("data").agg({"Saldo_Inicial_CC": "max"})
            )
        except Exception as e:
            print(f"Não foi possível pivotar o dataframe: {e}")
            exit
    return pivoted_df


def join_dfs(dfs: list[DataFrame], spark: SparkSession) -> DataFrame | None:
    """
    Recebe uma lista ordenada de DFs e retorna um DF mesclado com as informações sobrescritas conforme a regra estabelecida
    na documentação (sempre permanece o valor do arquivo mais recente).
    """
    full_join_df = None
    for df in dfs:
        if full_join_df is None:
            full_join_df = df
        else:
            # Obtém as colunas do full_join_df e do DataFrame
            # atual e encontra a interseção de colunas entre eles
            colunas_full_join = set(full_join_df.columns)
            columns_df_atual = set(df.columns)
            interseccao = colunas_full_join.intersection(columns_df_atual)
            interseccao.remove("Nome"), interseccao.remove("CPF")
            if interseccao:
                # Se houver colunas comuns,
                # renomeia-as no DataFrame atual para
                # evitar conflitos
                for coluna in interseccao:
                    df = df.withColumnRenamed(coluna, f"{coluna}_atual")
            try:
                full_join_df = full_join_df.join(df, how="full", on=["Nome", "CPF"])
                for coluna in interseccao:
                    # atualiza os valores da coluna com os
                    # valores da coluna atual se disponíveis
                    full_join_df = full_join_df.withColumn(
                        coluna,
                        when(
                            col(f"{coluna}_atual").isNotNull(),
                            col(f"{coluna}_atual"),
                        ).otherwise(col(coluna)),
                    ).drop(f"{coluna}_atual")

            except Exception as e:
                print(f"Não foi possível mesclar os dataframes: {e}")
                exit
    return full_join_df


def main():
    """
    Função principal do programa. Carrega arquivos em formato txt
    em um dicionário, reformatando e mesclando as informações
    de acordo com a data de criação dos arquivos.
    """
    spark = SparkSession.builder.appName("spark-test").getOrCreate()

    # puxando lista de arquivos txt
    arquivos = glob("files/*.txt")

    # criando dicionário e carregando os arquivos como dataframes
    # para dentro dele
    if len(arquivos) <= 1:
        return "Não há arquivos suficientes para processar."

    dict_dfs = arquivos_para_DFs(arquivos, spark)
    dict_reformatados = {}
    for chave, valor in dict_dfs.items():
        dict_reformatados[chave] = reformatar_dataframe(chave, valor)

    lista_ordenada = sorted(
        list(
            filter(
                lambda item: not isinstance(item, str), list(dict_reformatados.keys())
            )
        )
    )
    lista_ordenada = [dict_reformatados["tabela_saldo_inicial.txt"]] + [
        dict_reformatados[chave] for chave in lista_ordenada
    ]

    df_joined = join_dfs(lista_ordenada, spark)
    colunas = df_joined.columns
    for i in range(3, len(colunas)):
        df_joined = df_joined.withColumn(
            colunas[i],
            F.when(df_joined[colunas[i]].isNull(), df_joined[colunas[i - 1]]).otherwise(
                df_joined[colunas[i]]
            ),
        )

    df_joined.show()
    return None


if __name__ == "__main__":
    start_time = time.time()
    main()
    end_time = time.time()
    print(f"Execution time: {end_time - start_time} seconds")
