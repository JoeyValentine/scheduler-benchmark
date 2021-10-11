import json
import pathlib
import datetime

from dateutil import parser
from typing import Union, Hashable, Any
from os import listdir
from os.path import isfile, join

get_json_file_path_list = [pathlib.Path("results-5-5/new-scheduler/sysbench/get"), pathlib.Path("results-5-5/new-scheduler/fio/get")]

def get_schedule_start_time(dirs: list[pathlib.Path]) -> datetime.datetime:

    time_list = []

    for dir in dirs:
        file_paths = [join(dir, f) for f in listdir(dir) if isfile(join(dir, f))]

        for path in file_paths:
            with open(path, "r") as file:
                json_obj = json.load(file)
                date = parser.parse(json_obj["status"]["containerStatuses"][0]["state"]["terminated"]["startedAt"])
                time_list.append(date)

    time_list.sort()

    return time_list[0]

def get_schedule_end_time(dirs: list[pathlib.Path]) -> datetime.datetime:
    time_list = []

    for dir in dirs:
        file_paths = [join(dir, f) for f in listdir(dir) if isfile(join(dir, f))]

        for path in file_paths:
            with open(path, "r") as file:
                json_obj = json.load(file)
                date = parser.parse(json_obj["status"]["containerStatuses"][0]["state"]["terminated"]["finishedAt"])
                time_list.append(date)

    time_list.sort()

    return time_list[-1]

def get_schedule_makespan(start_time: datetime.datetime, end_time: datetime.datetime) -> datetime.datetime:
    return end_time - start_time

def get_avg_job_exec_time(dirs: list[pathlib.Path]) -> float:
    time_list = []

    for dir in dirs:
        file_paths = [join(dir, f) for f in listdir(dir) if isfile(join(dir, f))]

        for path in file_paths:
            with open(path, "r") as file:
                json_obj = json.load(file)
                started_time = parser.parse(json_obj["status"]["containerStatuses"][0]["state"]["terminated"]["startedAt"]).timestamp()
                finished_time = parser.parse(json_obj["status"]["containerStatuses"][0]["state"]["terminated"]["finishedAt"]).timestamp()
                time_list.append(finished_time - started_time)

    avg_job_exec_time = sum(time_list) / float(len(time_list))

    return avg_job_exec_time

if __name__ == '__main__':
    start_time = get_schedule_start_time(get_json_file_path_list)
    end_time = get_schedule_end_time(get_json_file_path_list)

    makespan = get_schedule_makespan(start_time, end_time)

    avg_job_exec_time = get_avg_job_exec_time(get_json_file_path_list)

    print(makespan, datetime.timedelta(seconds=avg_job_exec_time))
