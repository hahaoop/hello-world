import _thread
import json
import os
import time
import queue

import requests
import tornado.ioloop
from tornado.web import RequestHandler, Application
from tornado.httpserver import HTTPServer
from tornado.options import options, define
from multiprocessing import Manager
from concurrent.futures import ThreadPoolExecutor

import bit_api
import constant
import gpt_utils
from logger import Logger

log = Logger()
# options.log_file_prefix = os.path.join(os.path.dirname(__file__), 'log/tornado_client.log')


class SetTaskHandler(RequestHandler):
    async def post(self):
        data = json.loads(self.request.body)
        log.logger.info(f'run_task start:{data}')
        if 'browser_id' in data and data['browser_id'] in all_window:
            queue_map[all_window[data['browser_id']]].put(data)
        else:
            browser_res = bit_api.openBrowser(data['browser_id'])
            time.sleep(1)
            http_str = browser_res['data']['http']
            new_port = http_str[http_str.index(':') + 1: len(http_str)]
            all_window[data['browser_id']] = new_port
            queue_map[new_port] = queue.Queue()
            running_map[new_port] = queue.Queue()
            queue_map[new_port].put(data)
            _thread.start_new_thread(task_consumer, (new_port,))
        values = {'success': True}
        log.logger.info(f'run_task end')
        return self.write(json.dumps(values))


def handle_task(task):
    log.logger.info(f'run_task real start:{task}')
    result = gpt_utils.get_gpt_answer(int(all_window[task['browser_id']]), task['question'], task['conversation_id'])
    running_map[all_window[task['browser_id']]].get()
    values = {'task_id': task['task_id'], 'conversation_id': result['conversation_id'], 'browser_id': task['browser_id'],
              'answer': result['answer']}
    data = json.dumps(values).encode(encoding='UTF8')
    requests.post(f'http://{constant.server_ip}:{constant.server_port}/update_task', data=data)
    log.logger.info(f'run_task real end')


def task_consumer(b_port):
    while True:
        if running_map[b_port].qsize() > 0:
            time.sleep(1)
            continue
        task = queue_map[b_port].get()
        running_map[b_port].put('running')
        pool.submit(handle_task, task)


def main():
    define('port', default=constant.client_port, help='监听端口')
    app = make_app()
    http_server = HTTPServer(app)
    http_server.listen(options.port)
    log.logger.info('client start')
    tornado.ioloop.IOLoop.current().start()


def make_app():
    options.parse_command_line()
    handlers_routes = [
        (r'/run_task', SetTaskHandler),
    ]
    app = Application(handlers=handlers_routes)
    return app


if __name__ == '__main__':
    all_window = bit_api.get_all_window()
    manager = Manager()
    queue_map = {}
    running_map = {}
    pool = ThreadPoolExecutor(max_workers=10)
    for port in all_window.values():
        queue_map[port] = queue.Queue()
        running_map[port] = queue.Queue()
        _thread.start_new_thread(task_consumer, (port,))
    main()
