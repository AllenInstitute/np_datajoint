import argparse
import pathlib
import sys

from np_datajoint.classes import DataJointSession

def parse_args() -> argparse.Namespace:
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--session', type=str, required=True)
    parser.add_argument('--paths', nargs='+', type=str, required=True)
    parser.add_argument('--probes', type=str, required=True)
    parser.add_argument('--no-sorting', dest='without_sorting', action='store_true')
    
    args = parser.parse_args(sys.argv[1:])
    args.paths = tuple(pathlib.Path(p) for p in args.paths)
    return args


if __name__ == "__main__":   
    
    args = parse_args()
    print(args)
    session = DataJointSession(args.session)

    session.upload(
        paths=args.paths,
        probes=args.probes,
        without_sorting=args.without_sorting
        )