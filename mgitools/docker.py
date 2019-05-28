import argparse
import logging
import os
import re
import subprocess
import sys
import time
import uuid

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)

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
        c_id = re.split(r'\s+', line.strip())[0]
        if c_id:
            container_ids.add(c_id)

    return container_ids

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
            command = commands.pop()
            docker_command = generate_docker_command(command, image, max_memory=max_memory,
                    return_list=True)

            try:
                container_id = subprocess.check_output(docker_command).decode('utf-8').strip()
            except subprocess.CalledProcessError:
                logging.warning(f'command failed: {command}')

            logging.info(f'container id: {container_id} is executing: {command}')
            waiting.add(container_id)

            log_fp = os.path.join(logs_dir, f'{container_id}.log')
            logs_to_container_ids[log_fp] = container_id

        running_container_ids = get_running_container_ids()
        logging.debug(f'running containers: {running_container_ids}')
        finished = waiting.difference(running_container_ids)
        waiting = running_container_ids - finished
        logging.debug(f'currently waiting for: {waiting}')

    while waiting:
        running_container_ids = get_running_container_ids()
        logging.info(f'running containers: {running_container_ids}')
        finished = waiting.difference(running_container_ids)
        waiting = running_container_ids - finished
        logging.info(f'currently waiting for: {waiting}')
        time.sleep(wait_time)

    for log_fp, container_id in logs_to_container_ids.items():
        try:
            container_logs = subprocess.run(('docker', 'logs', container_id), stdout=subprocess.DEVNULL,
                    stderr=subprocess.PIPE).stderr.decode('utf-8')
            f = open(log_fp, 'w')
            f.write(container_logs)
            f.close()
        except subprocess.CalledProcessError:
            logging.warning(f'failed to get logs for container: {container_id}')


    logging.info('jobs complete')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('commands_file', type=str,
            help='File with commands to execute. One command per line.')
    parser.add_argument('image', type=str,
            help='Image to run commands in')
    parser.add_argument('--logs-dir', type=str, default=os.getcwd(),
            help='Directory to store logs in.')
    parser.add_argument('--max-memory', type=int, default=1,
            help='Max memory (in Gb) per job. Must be an integer.')
    parser.add_argument('--batch-size', type=int, default=10,
            help='Max number of jobs to execute at one time.')

    args = parser.parse_args()

    # make log directory
    if not os.path.isdir(args.logs_dir):
        os.mkdir(args.logs_dir)

    f = open(args.commands_file)
    commands = []
    for line in f:
        commands.append(line.strip())
    f.close()

    execute_commands(commands, args.image, max_memory=args.max_memory, batch_size=args.batch_size,
            logs_dir=args.logs_dir, wait_time=100)
