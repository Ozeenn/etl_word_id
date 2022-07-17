"""
@author: patrick_alan
@created_date: 17/07/2022
"""

from datetime import datetime
from re import sub
from Config.Postgre import PostgreConfig
from Connections.Postgre import PostgreConnection
import os

##############################################
pst_config = PostgreConfig('db_etl')
pst_conn = PostgreConnection(pst_config)
base_path = os.getcwd().split('mains')[0] + '\dataset'
##############################################


def get_words(file_num):
    full_path = base_path + f'\\{file_num}'
    with open(full_path, encoding='latin-1') as file:
        words = sub(r',|;|\.|\'|\"|\!|\?|\:|\<|\>|\/|\\|\[|\]|\(|\)|\{|\}|\´|\_|\-|\^|\~|\=|\`|\*|\#|\+|\|', '',
                    file.read().lower()).split()
    return words


def get_files_words():
    dict_words = {}
    for num in range(45):
        dict_words[num] = get_words(num)

    return dict_words


def set_word_id():
    dict_ids = {}
    value_ids = {}
    file_word_dict = get_files_words()
    current_id = 1
    ids_list = []
    print('     ' + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - Inserindo dados")
    for key in file_word_dict.keys():
        print('          ' + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + f" - Coletando informações arquivo: {key}")
        if key not in dict_ids.keys():
            dict_ids[key] = []

        for value in file_word_dict[key]:
            if value not in value_ids.keys():
                value_ids[value] = {key: current_id}
                dict_ids[key].append((key, value, value_ids[value][key]))
                current_id += 1
            elif value in value_ids.keys() and key not in value_ids[value].keys():
                id_value = [word_id for word_id in value_ids[value].values()]
                if (key, value, id_value[0]) not in dict_ids[key]:
                    dict_ids[key].append((key, value, id_value[0]))
                    current_id += 1

    for value in dict_ids.values():
        for valor in value:
            ids_list.append(valor)

    insert_word_ids('staging.stg_word_id', ids_list)
    call_procs()


def insert_word_ids(table, data):
    print('     ' + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - Inserindo dados staging")
    pst_conn.insert_into(
        table=table,
        data=data,
        flg_trunc_before=True
    )


def call_procs():
    print('     ' + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - Populando tabelas")
    pst_conn.call_proc('proc_insert_tb_word_files')
    pst_conn.call_proc('proc_insert_tb_word_id')


if __name__ == '__main__':
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - Iniciando script")
    set_word_id()
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - Finalizando script")
