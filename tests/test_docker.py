import re
import subprocess
import time

import pytest

import mgitools.bsub as bsub

def test_single_command():
    output = subprocess.check_output("python3 mgitools/docker.py --command 'python test.py --verbose --cat-output temp.txt test.txt' python:3.6", shell=True) 
    assert True
