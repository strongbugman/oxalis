from .base import App

def run_worker(app: App):
    app.run_worker_master()
