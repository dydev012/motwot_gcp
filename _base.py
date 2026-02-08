from dotenv import load_dotenv
from pathlib import Path
import os

DATA_DIR = Path(__file__).parent / 'data'

class ENV:

    def __init__(self, **kwargs):
        envdir = Path(__file__).parent
        print(envdir)
        load_dotenv(os.path.join(envdir, '.env'), override=True)
