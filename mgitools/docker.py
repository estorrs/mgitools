import argparse
import logging
import os
import re
import subprocess
import sys
import time
import uuid

CONTAINER_ID_LENGTH = 7

def remap_command(command, container_prefix='/inputs'):
    new_command = []
    volume_map = {}
    pieces = re.split(r'\s+', command)
    for piece in pieces:
        if piece[0] != '-' and re.findall(r'\.|/', piece):
            original_fp = os.path.abspath(piece)
            trimmed_original_fp = original_fp
            if '.' in original_fp.split('/')[-1]:
                trimmed_original_fp = '/'.join(original_fp.split('/')[:-1])

            if trimmed_original_fp in volume_map:
                if '.' in original_fp.split('/')[-1]:
                    container_fp = os.path.join(volume_map[trimmed_original_fp], original_fp.split('/')[-1])
                else:
                    container_fp = volume_map[trimmed_original_fp]
            else:
                u_id = str(uuid.uuid4())
                if '.' in original_fp.split('/')[-1]:
                    container_fp = os.path.join(container_prefix, u_id, original_fp.split('/')[-1])
                else:
                    container_fp = os.path.join(container_prefix, u_id)
                volume_map[trimmed_original_fp] = os.path.join(container_prefix, u_id)

            new_command.append(container_fp)
        else:
            new_command.append(piece)

    return ' '.join(new_command), volume_map

def generate_docker_command(command, image, max_memory=2, return_list=False,
        additional_volumes={}):
    """
    """
    args = ['docker', 'run', '-d']

    new_command, volume_map = remap_command(command)

    if additional_volumes is not None:
        volume_map.update(additional_volumes)

    for original_fp, container_fp in volume_map.items():
        args += ['-v', f'{original_fp}:{container_fp}']

    args += [
            '-m', f'{max_memory}G',
            image,
            ]

    args += new_command.split(' ')

    if return_list:
        return args
    return ' '.join(args)

def get_running_container_ids():
    output = subprocess.check_output(('docker', 'ps')).decode('utf-8')

    lines = output.split('\n')[1:]

    container_ids = set()
    for line in lines:
        c_id = re.split(r'\s+', line.strip())[0][:CONTAINER_ID_LENGTH]
        if c_id:
            container_ids.add(c_id)

    return container_ids

def execute_command(command, image, max_memory=2, log_fp=None, wait_time=10):
    waiting = set()

    docker_command = generate_docker_command(command, image, max_memory=max_memory,
            return_list=True)
    logging.debug(f'docker command: {docker_command}')
    container_id = subprocess.check_output(docker_command).decode('utf-8').strip()
    container_id = container_id[:CONTAINER_ID_LENGTH]
    logging.info(f'container id: {container_id} is executing: {command}')
    waiting.add(container_id)

    logging.info(f'currently waiting for: {waiting}')
    while waiting:
        running_container_ids = get_running_container_ids()
        finished = waiting.difference(running_container_ids)
        waiting = waiting - finished
        time.sleep(wait_time)

    logging.info('gathering container logs')
    container_logs = subprocess.run(('docker', 'logs', container_id), stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE).stderr.decode('utf-8')
    f = open(log_fp, 'w')
    f.write(container_logs)
    f.close()
    logging.info('job successfully completed')

def execute_commands(commands, image, max_memory=2, batch_size=10, logs_dir=os.getcwd(),
        wait_time=100):
    waiting = set()
    logs_to_container_ids = {}
    while commands:
        if len(waiting) >= batch_size:
            logging.info(f'waiting {wait_time} seconds for jobs to finish...')
            running_container_ids = get_running_container_ids()
            logging.info(f'containers currently running: {running_container_ids}')
            time.sleep(wait_time)
        else:
            command = commands.pop(0)
            docker_command = generate_docker_command(command, image, max_memory=max_memory,
                    return_list=True)

            try:
                container_id = subprocess.check_output(docker_command).decode('utf-8').strip()
                container_id = container_id[:CONTAINER_ID_LENGTH]
                logging.info(f'container id: {container_id} is executing: {command}')
                waiting.add(container_id)

                log_fp = os.path.join(logs_dir, f'{container_id}.log')
                logs_to_container_ids[log_fp] = container_id
            except subprocess.CalledProcessError:
                logging.exception(f'command failed: {command}')


        running_container_ids = get_running_container_ids()
        logging.debug(f'running containers: {running_container_ids}')
        finished = waiting.difference(running_container_ids)
        waiting = waiting - finished
        logging.debug(f'currently waiting for: {waiting}')

    while waiting:
        running_container_ids = get_running_container_ids()
        logging.debug(f'running containers: {running_container_ids}')
        logging.info(f'currently waiting for: {waiting}')
        finished = waiting.difference(running_container_ids)
        waiting = waiting - finished
        time.sleep(wait_time)

    logging.info('gathering container logs')
    for log_fp, container_id in logs_to_container_ids.items():
        try:
            container_logs = subprocess.run(('docker', 'logs', container_id), stdout=subprocess.DEVNULL,
                    stderr=subprocess.PIPE).stderr.decode('utf-8')
            f = open(log_fp, 'w')
            f.write(container_logs)
            f.close()
        except subprocess.CalledProcessError:
            logging.warning(f'failed to get logs for container: {container_id}')

    logging.info('jobs finished')

def check_arguments(args):
    if args.commands_file is None and args.command is None:
        raise ValueError('Either a commands file or a command must be specified. \
Provide either --command or --commands-file')
    if args.commands_file is not None and args.command is not None:
        raise ValueError('Command file and command cannot both be specified. \
Provide either --command or --commands-file')

if __name__ == '__main__':

    parser = argparse.ArgumentParser()

    parser.add_argument('--commands-file', type=str,
            help='File with commands to execute. One command per line.')
    parser.add_argument('--command', type=str,
            help='Command to execute')
    parser.add_argument('image', type=str,
            help='Image to run commands in')
    parser.add_argument('--logs-dir', type=str, default=os.getcwd(),
            help='Directory to store logs in.')
    parser.add_argument('--log-file', type=str, default='job.log',
            help='Filepath to store job logs at.')
    parser.add_argument('--max-memory', type=int, default=1,
            help='Max memory (in Gb) per job. Must be an integer.')
    parser.add_argument('--batch-size', type=int, default=10,
            help='Max number of jobs to execute at one time.')
    parser.add_argument('--verbose', action='store_true',
            help='Set logging level to debug for verbose output')

    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.DEBUG)
    else:
        logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

    check_arguments(args)

    # make log directory
    if not os.path.isdir(args.logs_dir):
        os.mkdir(args.logs_dir)

    if args.commands_file is not None:
        f = open(args.commands_file)
        commands = []
        for line in f:
            commands.append(line.strip())
        f.close()

        execute_commands(commands, args.image, max_memory=args.max_memory, batch_size=args.batch_size,
                logs_dir=args.logs_dir, wait_time=100)
    else:
        execute_command(args.command, args.image, max_memory=args.max_memory, log_fp=args.log_file,
                wait_time=10)
