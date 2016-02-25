try:
    from gevent.monkey import patch_all
    patch_all()
except ImportError:
    pass

from dlstats import client

def main():
    client.main()