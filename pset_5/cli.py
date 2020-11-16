"""
Module that contains the command line app.

Why does this file exist, and why not put this in __main__?

  You might be tempted to import things from __main__ later, but that will cause
  problems: the code will get executed twice:

  - When you run `python -mcsci_utils` python will execute
    ``__main__.py`` as a script. That means there won't be any
    ``csci_utils.__main__`` in ``sys.modules``.
  - When you import __main__ it will get executed again (as a module) because
    there's no ``csci_utils.__main__`` in ``sys.modules``.

  Also see (1) from http://click.pocoo.org/5/setuptools/#setuptools-integration
"""
import argparse
from luigi import build
from pset_5.tasks.yelp import ByDecade, ByStars

parser = argparse.ArgumentParser(description="Yelp reviews.")
parser.add_argument(
    "-f", "--full", action="store_false", dest="full"
)



def main(args=None):
    args = parser.parse_args(args=args)
    build([ByStars(subset=args.full), ByDecade(subset=args.full)], local_scheduler=True)