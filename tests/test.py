import re
import subprocess
import time

import pytest

import mgitools.bsub as bsub

DEFAULT_LOG_FP = 'job.log'


def test_bsub_hello_world():
    command = 'echo "hello, world!"'

    bsub_script_fp = 'test.sh'
    bsub.generate_bsub_bash_script([command], 'python:3.6-jessie', bsub_script_fp)

    result = subprocess.check_output(['bash', bsub_script_fp]).decode('utf-8')

    assert 'is submitted to queue' in result

    job_id = re.sub(r'^.*<([0-9]*)>.*$', r'\1', result).replace('\n', '')

    # wait for setup
    time.sleep(5)

    while True:
        bjob_output = subprocess.check_output(['bjobs']).decode('utf-8')
        if job_id not in bjob_output:
            break
        time.sleep(5)

    f = open(DEFAULT_LOG_FP)
    assert 'Successfully completed' in f.read()

def test_bsub_hello_world_multiple_commands():
    commands = ['echo "hello, world! 1"',
            'echo "hello, world! 2"',
            'echo "hello, world! 3"']

    output_fps = ['test.1.output', 'test.2.output', 'test.3.output']
    log_fps = ['test.1.log', 'test.2.log', 'test.3.log']

    bsub_script_fp = 'test.sh'
    bsub.generate_bsub_bash_script(commands, 'python:3.6-jessie', bsub_script_fp,
            output_files=output_fps, log_files=log_fps)

    result = subprocess.check_output(['bash', bsub_script_fp]).decode('utf-8')

    assert 'is submitted to queue' in result

    # wait for setup
    time.sleep(1)

    bjob_output = subprocess.check_output(['bjobs']).decode('utf-8')
    job_ids = []
    for line in bjob_output.split('\n')[2:]:
        job_id = re.sub(r'^([0-9]+).*$', r'\1', line).replace('\n', '')

        if job_id.isdigit():
            job_ids.append(job_id)

    while True:
        bjob_output = subprocess.check_output(['bjobs']).decode('utf-8')
        if len([job_id for job_id in job_ids
            if job_id in bjob_output]) == 0:
            break
        time.sleep(5)

    for log_fp in log_fps:
        f = open(log_fp)
        assert 'Successfully completed' in f.read()
        f.close()

def test_bsub_hello_world_with_pipe():
    command = 'echo "hello, world!" | egrep "hello, world"'

    bsub_script_fp = 'test.sh'
    bsub.generate_bsub_bash_script([command], 'python:3.6-jessie', bsub_script_fp)

    result = subprocess.check_output(['bash', bsub_script_fp]).decode('utf-8')

    assert 'is submitted to queue' in result

    job_id = re.sub(r'^.*<([0-9]*)>.*$', r'\1', result).replace('\n', '')

    # wait for setup
    time.sleep(5)

    while True:
        bjob_output = subprocess.check_output(['bjobs']).decode('utf-8')
        if job_id not in bjob_output:
            break
        time.sleep(5)

    f = open(DEFAULT_LOG_FP)
    assert 'Successfully completed' in f.read()
