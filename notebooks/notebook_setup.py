
import os
import pathlib
import sys
import traceback
import warnings

import IPython

DEBUG = True
ipython = IPython.get_ipython()
show_traceback = ipython.showtraceback
def hide_traceback(exc_tuple=None, filename=None, tb_offset=None,
                exception_only=False, running_compiled_code=False):
    etype, value, tb = sys.exc_info()
    return ipython._showtraceback(etype, value, ipython.InteractiveTB.get_exception_only(etype, value))
ipython.showtraceback = hide_traceback if not DEBUG else show_traceback

def hide_warning_lines(msg:str,category:str,*args,**kwargs):
    print("\n{}: {}\n".format(category.__name__, msg))
warnings.showwarning = hide_warning_lines