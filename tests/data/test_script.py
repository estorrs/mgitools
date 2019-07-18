import argparse
import logging
import time

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)


parser = argparse.ArgumentParser()

parser.add_argument('input_file', type=str,
        help='A file to use')

parser.add_argument('--cat-output', type=str,
        default='cat.txt', help='location to cat output to')

parser.add_argument('--verbose', action='store_true',
        default='cat.txt', help='location to cat output to')

args = parser.parse_args()

if args.verbose:
    f = open(args.input_file)
    for line in f:
        print(line)
        logging.info(line)
    f.close()

time.sleep(15)

f = open(args.input_file)
out_f = open(args.cat_output, 'w')
for line in f:
    out_f.write(line)
f.close()
out_f.close()



    
