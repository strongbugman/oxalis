from .base import App
from .beater import Beater

def run_worker(app: App):
    app.run_worker_master()

def run_beater(beater: Beater):
    beater.run()
