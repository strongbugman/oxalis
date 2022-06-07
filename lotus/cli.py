from .base import Lotus

def run_worker(app: Lotus):
    app.run_worker_master()
