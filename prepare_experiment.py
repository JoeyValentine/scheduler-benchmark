import os
import yaml
import random
import numpy as np
import configparser
import pathlib
from typing import Union, Hashable, Any
from pprint import pprint
from itertools import repeat

n_pods = 50
schedule_interval = 15
nodes = ["slave1", "slave3", "slave4"]
resources = ["cpu", "memory", "bandwidth"]
allocatalbe_resources = {"cpu": 24, "memory": 64, "bandwidth": 150}
io_type_ratio = [1.0 / 4, 1.0 / 4, 1.0 / 4, 1.0 / 4]
io_type_list = ["read", "write", "randread", "randwrite"]
workload_type_ratio = [2.0 / 10, 8.0 / 10]
workload_types_list = ["CPU intensive", "I/O intensive"]
workload_scale_ratio = [2.0 / 10, 6.0 / 10, 2.0 / 10]
workload_scale_list = [("low", "medium", "high"), ("low", "medium", "high")]
workload_resource_request_range = [{"cpu": (0.1, 0.3), "memory": (0.1, 0.2), "bandwidth": (0.0, 0.0)},
                                   {"cpu": (0.05, 0.1), "memory": (0.1, 0.2), "bandwidth": (0.1, 0.3)}]
seq_read_param_list = ["6G", "8G", "10G"]
seq_write_param_list = ["6G", "8G", "10G"]
rand_read_param_list = ["128m", "256m", "512m"]
rand_write_param_list = ["6G", "8G", "10G"]
sysbench_param_list = [["--cpu-max-prime=100000000", "--threads=2"],
                       ["--cpu-max-prime=150000000", "--threads=2"],
                       ["--cpu-max-prime=200000000", "--threads=2"]]

def gen_pod_resources(request_ratio_range: dict[str, tuple[float, float]]) -> list[float]:
    ret = []

    for i, resource in enumerate(resources):
        bound = request_ratio_range[resource]
        request = allocatalbe_resources[resource] * random.uniform(bound[0], bound[1])
        ret.append(request)

    return ret

def gen_workload_schedule(n: int, pvals: list[float]) -> tuple[list[int], list[int]]:
    samples = np.random.multinomial(n, pvals).tolist()

    ret = []

    for i, count in enumerate(samples):
        ret.append(np.full(count, i))

    return np.random.permutation(np.concatenate(ret, axis=0)).tolist(), samples

def gen_workload_scale(n: int, pvals: list[float]) -> list[int]:
    samples = np.random.multinomial(n, pvals).tolist()

    ret = []

    for i, count in enumerate(samples):
        ret.append(np.full(count, i))

    return np.random.permutation(np.concatenate(ret, axis=0)).tolist()

def gen_schedule() -> list[list]:
    workload_schedule, samples = gen_workload_schedule(n_pods, workload_type_ratio)
    io_type_schedule, _ = gen_workload_schedule(samples[1], io_type_ratio)
    sched_intervals = gen_pod_schedule_interval(schedule_interval, 1, n_pods)

    ret = []
    workload_scales = []
    workload_idx = []
    io_type_idx = 0

    for i in range(len(samples)):
        workload_scales.append(gen_workload_scale(samples[i], workload_scale_ratio))
        workload_idx.append(0)

    for i in range(len(workload_schedule)):
        workload_type = workload_schedule[i]
        workload_scale = workload_scales[workload_type][workload_idx[workload_type]]
        workload_idx[workload_type] += 1
        if workload_types_list[workload_type] == "I/O intensive":
            ret.append([sched_intervals[i],
                        gen_pod_resources(workload_resource_request_range[workload_type]),
                        workload_types_list[workload_type],
                        workload_scale_list[workload_type][workload_scale],
                        io_type_list[io_type_schedule[io_type_idx]]])
            io_type_idx += 1
        else:
            ret.append([sched_intervals[i],
                        gen_pod_resources(workload_resource_request_range[workload_type]),
                        workload_types_list[workload_type],
                        workload_scale_list[workload_type][workload_scale]])

    return ret

def gen_pod_schedule_interval(lamb: float, interval: float, size: int) -> list[float]:
    t = 0.0
    ret = []

    ret.append(t)

    for _ in repeat(None, size - 1):
        t += random.expovariate(interval / lamb)
        ret.append(t)

    return ret

def gen_fio_job_file(pod_idx: int, resource_requests: list[float], workload_type: str, workload_scale_idx: int) -> configparser.ConfigParser:
    config = configparser.ConfigParser(delimiters="=")
    config.read('manifests/fio-seq-write.fio')

    config["global"]["name"] = str(pod_idx + 1) + "-fio-job.fio"
    config["global"]["filename"] = str(pod_idx + 1) + "-fio-job.fio"
    config["global"]["rw"] = workload_type

    if workload_type == "read":
        config["global"]["bs"] = "128K"
        config["file1"]["size"] = seq_read_param_list[workload_scale_idx]
    elif workload_type == "write":
        config["global"]["bs"] = "128K"
        config["file1"]["size"] = seq_write_param_list[workload_scale_idx]
    elif workload_type == "randread":
        config["global"]["bs"] = "4K"
        config["file1"]["size"] = rand_read_param_list[workload_scale_idx]
    elif workload_type == "randwrite":
        config["global"]["bs"] = "4K"
        config["file1"]["size"] = rand_write_param_list[workload_scale_idx]

    config["global"]["rate"] = str(int(resource_requests[2])) + "m," + str(int(resource_requests[2])) + "m"

    return config

def gen_fio_pod(pod_idx: int, resource_requests: list[float], workload_scale_idx: int) -> Union[dict[Hashable, Any], list, None]:
    with open("manifests/fio-pod.yaml", "r") as file:
        pod_yaml = yaml.load(file.read())

        pod_yaml["metadata"]["name"] = str(pod_idx + 1) + "-fio-" + workload_scale_list[1][workload_scale_idx]

        pod_yaml["spec"]["containers"][0]["resources"]["requests"]["cpu"] = str(round(resource_requests[0], 2))
        pod_yaml["spec"]["containers"][0]["resources"]["requests"]["memory"] = str(round(resource_requests[1], 2)) + "Gi"
        pod_yaml["spec"]["containers"][0]["resources"]["requests"]["sogang.ac.kr/bandwidth"] = str(int(resource_requests[2])) + "Mi"
        pod_yaml["spec"]["containers"][0]["resources"]["limits"]["cpu"] = str(round(resource_requests[0], 2))
        pod_yaml["spec"]["containers"][0]["resources"]["limits"]["memory"] = str(round(resource_requests[1], 2)) + "Gi"
        pod_yaml["spec"]["containers"][0]["resources"]["limits"]["sogang.ac.kr/bandwidth"] = str(int(resource_requests[2])) + "Mi"

        mount_dir = pathlib.Path(pod_yaml["spec"]["containers"][0]["args"][0])
        job_file_dir = pathlib.Path(pod_yaml["spec"]["volumes"][2]["hostPath"]["path"])

        pod_yaml["spec"]["containers"][0]["args"][0] = str(mount_dir.parent / (str(pod_idx + 1) + "-fio-job.fio"))
        pod_yaml["spec"]["containers"][0]["volumeMounts"][2]["mountPath"] = str(mount_dir.parent / (str(pod_idx + 1) + "-fio-job.fio"))
        pod_yaml["spec"]["volumes"][0]["persistentVolumeClaim"]["claimName"] = str(pod_idx + 1) + "-fio-pvc"
        pod_yaml["spec"]["volumes"][2]["hostPath"]["path"] = str(job_file_dir.parent / (str(pod_idx + 1) + "-fio-job.fio"))

    return pod_yaml

def gen_fio_pvc(pod_idx: int) -> Union[dict[Hashable, Any], list, None]:
    with open("manifests/local-pvc.yaml", "r") as file:
        pod_yaml = yaml.load(file.read())

        pod_yaml["metadata"]["name"] = str(pod_idx + 1) + "-fio-pvc"

    return pod_yaml

# For pv that supports dynamic-provisioning, it must be implemented differently.
def gen_fio_pv(pod_idx: int, node: str) -> Union[dict[Hashable, Any], list, None]:
    pv_file_path = pathlib.Path("pv")

    if not os.path.exists(pv_file_path):
        os.makedirs(pv_file_path)

    with open("manifests/local-pv.yaml", "r") as file:
        pod_yaml = yaml.load(file.read())

        pod_yaml["metadata"]["name"] = str(pod_idx + 1) + "-" + node + "-fio-pv"
        pod_yaml["spec"]["local"]["path"] = "/var/lib/fio/volume" + str(pod_idx + 1)
        pod_yaml["spec"]["nodeAffinity"]["required"]["nodeSelectorTerms"][0]["matchExpressions"][0]["values"][0] = node

    return pod_yaml

def gen_sysbench_pod(pod_idx: int, resource_requests: list[float], workload_scale_idx: int) -> Union[dict[Hashable, Any], list, None]:
    with open("manifests/sysbench-pod.yaml", "r") as file:
        pod_yaml = yaml.load(file.read())

        pod_yaml["metadata"]["name"] = str(pod_idx + 1) + "-sysbench-" + workload_scale_list[0][workload_scale_idx]
        pod_yaml["spec"]["containers"][0]["resources"]["requests"]["cpu"] = str(round(resource_requests[0], 2))
        pod_yaml["spec"]["containers"][0]["resources"]["requests"]["memory"] = str(round(resource_requests[1], 2)) + "Gi"
        pod_yaml["spec"]["containers"][0]["resources"]["limits"]["cpu"] = str(round(resource_requests[0], 2))
        pod_yaml["spec"]["containers"][0]["resources"]["limits"]["memory"] = str(round(resource_requests[1], 2)) + "Gi"
        pod_yaml["spec"]["containers"][0]["args"][0] = sysbench_param_list[workload_scale_idx][0]
        pod_yaml["spec"]["containers"][0]["args"][1] = sysbench_param_list[workload_scale_idx][1]

    return pod_yaml

def get_workload_scale_idx(workload_type: str, workload_scale: str) -> int:
    idx = 0

    if workload_type == "CPU intensive":
        if workload_scale == "low":
            idx = 0
        elif workload_scale == "medium":
            idx = 1
        elif workload_scale == "high":
            idx = 2
    elif workload_type == "I/O intensive":
        if workload_scale == "low":
            idx = 0
        elif workload_scale == "medium":
            idx = 1
        elif workload_scale == "high":
            idx = 2

    return idx

def gen_experiment_dir(schedule: list[list]) -> None:
    job_file_path = pathlib.Path("fio-jobs")

    if not os.path.exists(job_file_path):
        os.makedirs(job_file_path)

    pv_path = pathlib.Path("pv")

    if not os.path.exists(pv_path):
        os.makedirs(pv_path)

    for node in nodes:
        if not os.path.exists(pv_path / node):
            os.makedirs(pv_path / node)

    for i in range(n_pods):

        for node in nodes:
            pv_pod_obj = gen_fio_pv(i, node)
            with open(pv_path / node / (str(i + 1) + "-local-pv.yaml"), "w") as file:
                yaml.dump(pv_pod_obj, file)

        pod_path = pathlib.Path("pods/pod" + str(i + 1))

        if not os.path.exists(pod_path):
            os.makedirs(pod_path)

        workload_scale_idx = get_workload_scale_idx(schedule[i][2], schedule[i][3])

        if schedule[i][2] == "CPU intensive":
            sysbench_pod_obj = gen_sysbench_pod(i, schedule[i][1], workload_scale_idx)
            with open(pod_path / "sysbench-pod.yaml", "w") as file:
                yaml.dump(sysbench_pod_obj, file)

        elif schedule[i][2] == "I/O intensive":
            fio_pod_obj = gen_fio_pod(i, schedule[i][1], workload_scale_idx)
            with open(pod_path / "fio-pod.yaml", "w") as file:
                yaml.dump(fio_pod_obj, file)

            pvc_pod_obj = gen_fio_pvc(i)
            with open(pod_path / "fio-pvc.yaml", "w") as file:
                yaml.dump(pvc_pod_obj, file)

            config_obj = gen_fio_job_file(i, schedule[i][1], schedule[i][4], workload_scale_idx)
            with open(job_file_path / (str(i + 1) + "-fio-job.fio"), "w") as configfile:
                config_obj.write(configfile, space_around_delimiters=False)

def gen_intervals(schedule: list[list]) -> list[str]:

    intervals = []

    for i, job in enumerate(schedule):
        if i != 0:
            intervals.append(str(job[0] - schedule[i - 1][0]))
        else:
            intervals.append(str(job[0]))

    return intervals

if __name__ == '__main__':
    # [sched_time, resource_requests, workload_type, workload_scale, (I/O type)]
    schedule = gen_schedule()

    intervals = gen_intervals(schedule)

    with open("intervals.txt", "w") as file:
        file.write("\n".join(intervals))

    gen_experiment_dir(schedule)
