import ctypes, ctypes.util
import fcntl
import os
import threading
import atexit
import psutil

_libc = ctypes.CDLL(ctypes.util.find_library('c'), use_errno=True)
_SYS_gettid = 186

_libhft_base_handle        = ctypes.CDLL(os.path.dirname(os.path.abspath(__file__))+'/../'+'libhft_base.so', mode=ctypes.RTLD_LOCAL)
_init_hft_base             = ctypes.CFUNCTYPE(None, ctypes.c_bool, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_int64, ctypes.c_int64 )(('init_hft_base', _libhft_base_handle))
_get_hft_base_channel_names   = ctypes.PYFUNCTYPE(ctypes.py_object, ctypes.c_void_p)(('get_hft_base_channel_names', _libhft_base_handle))
_delete_hft_base_unit      = ctypes.CFUNCTYPE(None, ctypes.c_void_p)(('delete_hft_base_unit', _libhft_base_handle))
_connect_hft_base_channels    = ctypes.CFUNCTYPE(None, ctypes.c_void_p, ctypes.c_size_t, ctypes.c_void_p, ctypes.c_size_t)(('connect_hft_base_channels', _libhft_base_handle))
_prepare_hft_base_core = ctypes.PYFUNCTYPE(None)(('prepare_hft_base_core', _libhft_base_handle))
_run_hft_base_core     = ctypes.CFUNCTYPE(None)(('run_hft_base_core', _libhft_base_handle))
_pause_hft_base_core   = ctypes.CFUNCTYPE(None)(('pause_hft_base_core', _libhft_base_handle))

_pid_file = None

def init(realtime=True, recovery_prefix='', log_prefix='', datetime_ref=None, end_time=None):
    global _pid_file
    _pid_file = open(log_prefix+'pid', 'wb', 0)
    fcntl.flock(_pid_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
    _pid_file.write(str(os.getpid()).encode('ascii'))
    _init_hft_base(realtime, recovery_prefix.encode('ascii'), log_prefix.encode('ascii'), int(datetime_ref.timestamp())*1000000000 , int(end_time.timestamp())*1000000000 )

units = []

_threads = []

class Channel:
    def __init__(self, unit, id, producer_side):
        self.unit = unit
        self.id = id
        self.producer_side = producer_side

class Unit:
    def __init__(self, adapter, unit, **kwargs):
        print('initializing adapter: %s unit: %s' % (adapter, unit), flush=True)
        lib_handle = ctypes.CDLL(os.path.dirname(os.path.abspath(__file__))+'/../'+adapter+'.so', mode=ctypes.RTLD_LOCAL)
        prototype = ctypes.PYFUNCTYPE(ctypes.c_void_p,ctypes.py_object)
        func = prototype(('create_hft_base_unit_'+unit, lib_handle))
        self._px = func(kwargs)

        inputs, outputs = _get_hft_base_channel_names(self._px)
        self.inputs  = {inputs[i]:  Channel(self, i, False) for i in range(len(inputs))}
        self.outputs = {outputs[i]: Channel(self, i, True)  for i in range(len(outputs))}

        units.append(self)

        print('%s:%s' % (adapter, unit), ("(0x%x)" % self._px), 'inputs:', list(sorted(self.inputs.keys())), 'outputs:', list(sorted(self.outputs.keys())), flush=True)

def connect(producer, consumer):
    assert producer.producer_side == True
    assert consumer.producer_side == False
    _connect_hft_base_channels(producer.unit._px, producer.id, consumer.unit._px, consumer.id)

def TOI(start=0, end=9999999999999999, active=float('inf'), every=float('inf')):
    return dict(start=start, end=end, active=active, every=every)

prepare = _prepare_hft_base_core

def run(cpu_set=None, single_thread=True):
    def target():
        if cpu_set is not None:
            tid = _libc.syscall(_SYS_gettid)
            psutil.os.sched_setaffinity(tid, cpu_set)
        _run_hft_base_core()
    if single_thread:
        return target()
    t = threading.Thread(target=target)
    t.daemon = True
    _threads.append(t)
    t.start()

pause = _pause_hft_base_core

def join(timeout=None):
    while len(_threads) > 0:
        for t in _threads:
            if t.is_alive():
                t.join(timeout)
            else:
                _threads.remove(t)

@atexit.register
def cleanup():
    print('hft.base.cleanup', flush=True)
    print('hft.base.pause', flush=True)
    pause()
    print('hft.base.join', flush=True)
    join()
    global units
    for n in units:
        print('hft.base.delete_unit', ("(0x%x)" % n._px), flush=True)
        _delete_hft_base_unit(n._px)
    units = []

