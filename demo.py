import configparser

config = configparser.ConfigParser()
config.read(r'C:\Users\91832\PycharmProjects\snowconnect\venv\configurations.ini')

print(config.get('Logger', 'logfilepath'))
