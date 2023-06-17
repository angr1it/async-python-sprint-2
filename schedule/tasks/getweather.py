from http.client import HTTPResponse
from io import BytesIO
import socket

from ..scheduler import Scheduler, TimeOutGenerator
from ..job import Job
from ..utils import write_to_file

CITIES = {
    "MOSCOW": "code.s3.yandex.net/async-module/moscow-response.json",
    "PARIS": "code.s3.yandex.net/async-module/paris-response.json",
    "LONDON": "code.s3.yandex.net/async-module/london-response.json",
}

class FakeSocket():
    def __init__(self, response_str):
        self._file = BytesIO(response_str)
    def makefile(self, *args, **kwargs):
        return self._file


class GetWeather(Job):

    def __init__(self, result_folder_path: str, scheduler: Scheduler, get_scheduler = True, **kwargs) -> None:
        self.result_folder_path = result_folder_path
        self.scheduler = scheduler
        self.get_scheduler = get_scheduler
        super().__init__(**kwargs)

    def as_dict(self):
        data = {
            'start_at': self.start_at,
            'max_working_time': self.max_working_time,
            'tries': self.tries,
            'dependencies': self.dependencies,
            'status_folder': self.status_folder,
            'result_folder_path': self.result_folder_path,
            'get_scheduler': self.get_scheduler
        }

        return data
    
    def _run(self, start_with = 0):
        names = list(CITIES.keys())
        for i in range(start_with, len(names)):

            hostname, path = CITIES[names[i]].split('/', 1)
            port = 80
            path = '/' + path

            #Надеюсь, всё это подразумевалось в задании...
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            
            #TODO: do u actually need yield before connect?
            yield 'wait_write', client
            client.connect((hostname, port))

            yield from self.scheduler.sock_sendall(client, f"GET {path} HTTP/1.1\r\nHost:{hostname}\r\n\r\n".encode())

            response_str = b''
            while True:
                data = yield from self.scheduler.sock_recv(client, 4096)
                self.logger.debug(f'GetWeather: receiving {len(data)} bytes data...')
                response_str += data
                if not data:
                    client.close()
                    break
            
            source = FakeSocket(response_str)
            response = HTTPResponse(source)
            response.begin()
            response_content = response.read(len(response_str)).decode('utf-8')
    
            write_to_file(self.result_folder_path + f'/{names[i]}.json', response_content)

            # with open(self.result_folder_path + f'/{names[i]}.json', 'w') as file:
            #     file.write(response_content)

            self._pass_stage(i)
            yield 'none', None