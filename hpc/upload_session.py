import argparse
import pathlib
import importlib

try:
    from utils import DataJointSession
except ImportError:
    pass # in case this filename changes we'll import all files in the dir and check them for DataJointSession

def parse_args() -> argparse.Namespace:
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--session', type=str, required=True)
    parser.add_argument('--paths', nargs='+', type=str, required=True)
    parser.add_argument('--probes', type=str, required=True)
    parser.add_argument('--no-sorting', dest='without_sorting', action='store_true')
    
    args = parser.parse_args()
    args.paths = tuple(pathlib.Path(p) for p in args.paths)
    
    return args


if __name__ == "__main__":   
    args = parse_args()
    
    modules = []
    for p in (this_file := pathlib.Path(__file__)).parent.iterdir():
        if p.is_file() and p.suffix == '.py' and p != this_file:
            modules.append(importlib.import_module(p.stem))
            
    for module in modules:
        try:
            session = module.DataJointSession(args.session)
            break
        except AttributeError:
            continue
    
    session.upload(
        paths=args.paths,
        probes=args.probes,
        without_sorting=args.without_sorting
        )