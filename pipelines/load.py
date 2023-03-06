import sys
import importlib.machinery

from .utils import print_error


def load_pipeline():
    source_dir = '.'
    source_file_path = './pipeline.py'

    sys.path.insert(0, source_dir)
    loader = importlib.machinery.SourceFileLoader('pipeline', source_file_path)

    try:
        module = loader.load_module()
    except FileNotFoundError:
        print_error(f"File not found: {source_file_path}")
        sys.exit(1)

    try:
        return module.pipeline
    except AttributeError:
        print_error(f"There is no pipeline object defined in this file: {source_file_path}")
        sys.exit(1)
