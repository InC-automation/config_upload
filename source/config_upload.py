import grpc
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))
# from data import api_gateway_pb2, api_gateway_pb2_grpc
import api_gateway_pb2, api_gateway_pb2_grpc
import configparser
import time

class file_uploader:
    def __init__(self, server_address):
        self.server_address = server_address
        self.channel = None
        self.stub = None

    def connect(self):
        """Устанавливает соединение с сервером."""
        self.channel = grpc.insecure_channel(self.server_address)
        self.stub = api_gateway_pb2_grpc.ApiGatewayStub(self.channel)

    def close_connection(self):
        """Закрывает активное подключение к серверу."""
        if self.channel is not None:
            self.channel.close()

    @staticmethod
    def create_file_part_generator(file_path, chunk_size = 1 * 1024 * 1024):
        """
        Генерирует части файла для передачи по сети.
        
        :param file_path: Путь к файлу
        :param chunk_size: Размер блока (по умолчанию 1 MB)
        :return: Итератор частей файла
        """
        with open(file_path, 'rb') as f:
            while True:
                data_chunk = f.read(chunk_size)
                if not data_chunk:
                    break
                
                yield api_gateway_pb2.FilePart(buffer = data_chunk)

    def upload_file(self, file_path):
        """
        Загружает указанный файл на удаленный сервер.
        
        :param file_path: Путь к файлу
        :return: Статус успешной загрузки (True/False) и сообщение
        """
        try:
            # Устанавливаем соединение перед отправкой файла
            self.connect()
            
            # Читаем конфигурационное состояние
            config_state = self.stub.GetConfigState(api_gateway_pb2.Empty())
            
            #print(f"Текущее состояние конфигурации:\n{dir(config_state)}")
            print(f"Статус конфигурации: {api_gateway_pb2.ConfigState.State.Name(config_state.last_state)}")
            print(f"Версия конфигурации: {config_state.last_version.value}")
            print(f"Загрузка файла конфигурации...")
            
            # Отсылаем файл частями
            upload_result = self.stub.SetConfig(
                iter(file_uploader.create_file_part_generator(file_path))
            )
            
            # time.sleep(1)
            # Обрабатываем результат отправки
            if upload_result.value != api_gateway_pb2.SetConfigResult.SUCCESS:
                return False, f"Ошибка при загрузке файла: {load_result.error}"
                # return True, f"Процедура загрузки завершена."
            else:
                print(f"Загрузка файла прошла успешно.")

            print(f"Применение конфигурации ...")
            upload_state = api_gateway_pb2.ConfigState.State.IN_PROGRESS
            while upload_state != api_gateway_pb2.ConfigState.State.SUCCESS:
                time.sleep(1)
                config_state = self.stub.GetConfigState(api_gateway_pb2.Empty())
                upload_state = config_state.last_state
                print(f"Статус конфигурации: {api_gateway_pb2.ConfigState.State.Name(config_state.last_state)}")
            print(f"Версия конфигурации: {config_state.last_version.value}")
            return True, f"Применение конфигурации прошло успешно."
        except Exception as e:
            return False, f"Произошла ошибка: {e}"
        finally:
            # Закрываем соединение
            self.close_connection()

# главная функция
def main():    
    config = configparser.ConfigParser()
    files_read = config.read("settings.ini")
    if not files_read:
        print(f"Ошибка: конфигурационный файл '{config_file_path}' не найден.")
        return
    cs_address = config["Default"]["APIGATEWAY"]
    file_path = config["Default"]["FILE_PATH"]
    uploader = file_uploader(server_address = cs_address)
    upload_result, upload_message = uploader.upload_file(file_path)
    print(upload_message)

# запуск программы
if __name__ == "__main__":
	main()