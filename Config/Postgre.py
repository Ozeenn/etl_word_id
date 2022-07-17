# Criar função de conexão com banco de dados postgresql local

from Config.ConfigReader import Configs

class PostgreConfig:
    def __init__(self, database):
        credentials = Configs.get_credentials('postgresql')
        self.user = credentials['user']
        self.host = credentials['host']
        self.password = credentials['password']
        self.database = database



