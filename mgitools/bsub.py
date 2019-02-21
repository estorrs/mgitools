def format_stats(min_memory, max_memory, num_processes):
    return f"'select[mem>{min_memory * 1000}] rusage[mem={min_memory * 1000}] \
span[hosts={num_processes}]'"

def generate_bsub_command(command, image, min_memory=1, max_memory=2, num_processes=1,
        output_file='job.output', log_file='job.log', return_list=False, queue='research-hpc'):
    """
    returns a bsub command
    
    command
        - actual command to be run. i.e. python hello_world.py arg1 arg2 ....
    image
        - docker image name to use with bsub
    min_memory
        - minimum memory for bsub. Takes an int. Units are Gb
    num_processes
        - number of process to tell bsub to use
    return_list
        - If true, returns a list of commands. Otherwise returns the commands in a string.
    """
    args = ['bsub', '-R']

    args += [
            format_stats(min_memory, max_memory, num_processes),
            '-M', str(max_memory * 1000000),
            '-q', queue,
            '-o', output_file,
            '-oo', log_file,
            f"-a 'docker({image})'",
            f"'{command}'"
            ]

    if return_list:
        return args
    return ' '.join(args)

def generate_bsub_bash_script(commands, image, fp, min_memory=1, max_memory=2, num_processes=1,
        output_files=None, log_files=None, queue='research-hpc'):
    """Returns an executable bash script of bsub commands"""
    if output_files is None:
        output_files = []
        for i in range(len(commands)):
            output_files.append(f'job.{i}.output')

    if log_files is None:
        log_files = []
        for i in range(len(commands)):
            log_files.append(f'job.{i}.log')

    f = open(fp, 'w')
    for command, output_file, log_file in zip(commands, output_files, log_files):
        bsub_command = generate_bsub_command(command, image, min_memory=min_memory,
                max_memory=max_memory, num_processes=num_processes, output_file=output_file,
                log_file=log_file, return_list=False, queue=queue)
        f.write(bsub_command + '\n')
    f.close()
