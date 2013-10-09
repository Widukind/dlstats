import logging
import time
import daemon
import dlstats
import misc_func

Class Job(object):
    def __init__(self):
        self.stdin_path = '/dev/null'
        self.stdout_path = '/dev/tty'
        self.stderr_path = '/dev/tty'
        self.pidfile_path =  '/var/run/dlstats/dlstats.pid'
        self.pidfile_timeout = 5

   def run(self):
       while True:
           conf = misc_func.get_config()
           jobs = []
           i = 10
           while i > 0:
               for classname in conf.sections():
                   if conf[classname]['update_categories_db'] >= i:
                       jobs.append(classname + '.update_categories_db()')
                   if conf[classname]['update_series_db'] >= i:
                       jobs.append(classname + '.update_series_db()')
               i -= 1
           for job in jobs:
               eval(job)
           time.sleep(10)

job = Job()
lgr = logging.getLogger("dlstats")
lgr.setLevel(logging.INFO)
fh = logging.FileHandler("/var/log/dlstats/dlstats.log")
frmt = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
fh.setFormatter(frmt)
lgr.addHandler(fh)

daemon_runner = daemon.runner.DaemonRunner(job)
#This ensures that the logger file handle does not get closed during daemonization
daemon_runner.daemon_context.files_preserve=[handler.stream]
daemon_runner.do_action()
