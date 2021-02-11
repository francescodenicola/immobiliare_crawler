from flask import Flask, render_template,session,redirect, Response
from loguru import logger
import datetime, time

# configure logger
logger.add("static/job.log", format="{time} - {message}")

app = Flask(__name__)

def flask_logger():
    """creates logging information"""
    with open("static/job.log") as log_info:
        for i in range(25):
            logger.info(f"iteration #{i}")
            data = log_info.read()
            yield data.encode()
            time.sleep(1)
        # Create empty job.log, old logging will be deleted
        open("static/job.log", 'w').close()


@app.route("/")
def main():
    return render_template('index.html')


@app.route("/log_stream/", methods=["GET"])
def stream():
    """returns logging information"""
    return Response(flask_logger(), mimetype="text/plain", content_type="text/event-stream")

@app.route('/scrapeNow/')
def scrapeNow():
    f = open("status.lock", "r")
    a = f.read()
    f.close()
    print(a)
    if a == 'IDLE':
        f = open("status.lock", "w+")
        f.truncate(0)
        f.write("IDLE")
        f.close()
        return render_template('scrapeNow.html')
    else:
        return redirect("/")


if __name__ == "__main__":
    app.run()



