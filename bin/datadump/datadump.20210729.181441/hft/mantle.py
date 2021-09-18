import ctypes
import os
import datetime
import pandas
import importlib
import subprocess

_libhft_mantle_handle = ctypes.CDLL(os.path.dirname(os.path.abspath(__file__))+'/../'+'libhft_mantle.so', mode=ctypes.RTLD_LOCAL)
_open_hft_mantle_contracts_log  = ctypes.CFUNCTYPE(None, ctypes.c_char_p)(('open_hft_mantle_contracts_log', _libhft_mantle_handle))
_close_hft_mantle_contracts_log = ctypes.CFUNCTYPE(None)(('close_hft_mantle_contracts_log', _libhft_mantle_handle))
_add_hft_mantle_instruments    = ctypes.PYFUNCTYPE(None, ctypes.py_object)(('add_hft_mantle_instruments', _libhft_mantle_handle))
_add_hft_overnight_positions    = ctypes.PYFUNCTYPE(None, ctypes.py_object)(('add_hft_overnight_positions', _libhft_mantle_handle))
_add_hft_instrument_data        = ctypes.PYFUNCTYPE(None, ctypes.py_object)(('add_hft_instrument_data', _libhft_mantle_handle))
_prepare_hft_mantle_core = ctypes.PYFUNCTYPE(None)(('prepare_hft_mantle_core', _libhft_mantle_handle))

def open_contracts_log(s):
    _open_hft_mantle_contracts_log(s.encode('ascii'))

close_contracts_log = _close_hft_mantle_contracts_log
add_instruments    = _add_hft_mantle_instruments
add_overnight_positions    = _add_hft_overnight_positions
add_instrument_data = _add_hft_instrument_data
prepare = _prepare_hft_mantle_core

def previous_day(date, exchange):
    filename = '/data/Dates/all.days'
    with open(filename, 'r') as file:
        int_dates = eval(file.read())
        dates = map(lambda x: datetime.datetime.strptime(str(x), '%Y%m%d').date(), int_dates)
        return max(filter(lambda x: x < date, dates))

