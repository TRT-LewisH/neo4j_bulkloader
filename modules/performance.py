import os
import csv
import time
from threading import Thread
import docker

class PerformanceMeasurement:
    def __init__(self, func):
        self.func = func

    def __call__(self, *args, **kwargs):
        start_datetime = time.time()
        result = self.func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_datetime
        self.save_execution_time(execution_time, args[1], start_datetime)
        return result

    def save_execution_time(self, execution_time, file, start_datetime):
        with open(f'performance_results.csv', 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            if csvfile.tell() == 0:  # Check if file is empty
                writer.writerow(['file', 'execution_time', 'start_datetime'])
            writer.writerow([file, execution_time, start_datetime])

class ResourceLogger(Thread):
    # constructor
    def __init__(self, event, container_name):
        # call the parent constructor
        super().__init__()
        self.container_name = container_name

        client = docker.from_env()
        self.container = client.containers.get(self.container_name)
        self.event = event
        try:
            os.remove('resource_results.csv')
        except:
            pass
 
    def calculate_cpu_percent(self, d):
        cpu_count = len(d["cpu_stats"]["cpu_usage"]["percpu_usage"])
        cpu_percent = 0.0
        cpu_delta = float(d["cpu_stats"]["cpu_usage"]["total_usage"]) - \
                    float(d["precpu_stats"]["cpu_usage"]["total_usage"])
        system_delta = float(d["cpu_stats"]["system_cpu_usage"]) - \
                    float(d["precpu_stats"]["system_cpu_usage"])
        if system_delta > 0.0:
            cpu_percent = cpu_delta / system_delta * 100.0 * cpu_count
        return cpu_percent
    
    def save_results(self):
        results = self.container.stats(decode=None, stream = False)
        with open(f'resource_results.csv', 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            if csvfile.tell() == 0:  # Check if file is empty
                writer.writerow([
                    'mem', 'mem_max', 'mem_limit',
                    'cpu',
                ])
            writer.writerow([
                results['memory_stats']['usage'],
                results['memory_stats']['max_usage'], 
                results['memory_stats']['limit'], 
                self.calculate_cpu_percent(results),
            ])
    
    # execute task
    def run(self):
        # execute a task in a loop
        print('Thread has started')
        while True:
            self.save_results()
            if self.event.is_set():
                break
        print('Thread is closing down')
        