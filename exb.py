from ex import app, hello, hello2

from oxalis.beater import Beater
from oxalis.cli import run_beater


beater = Beater(app)

beater.register("*/1 * * * *", hello)
beater.register("*/2 * * * *", hello2)


if __name__ == "__main__":
    run_beater(beater)
