from starlette.staticfiles import StaticFiles
from starlette.responses import RedirectResponse,Response,StreamingResponse
from starlette.templating import Jinja2Templates
from loguru import logger
import datetime, time
from fastapi import BackgroundTasks, FastAPI,Request, Form, HTTPException
import uvicorn
import crawler as crawler
from multiprocessing import Process

# configure logger
logger.add("static/job.log", format="{time} - {message}")


app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

def follow(thefile):
     while True:
        line = thefile.readline()
        if not line or not line.endswith('\n'):
            time.sleep(0.1)
            continue
        yield line

def sleeping(message="RUNNING"):
    print(message)
    time.sleep(15)
    print("finished")

def test_logger_window():
    """creates logging information"""
    # with open("static/job.log") as log_info:
    #     for i in range(25):
    #         logger.info(f"iteration #{i}")
    #         data = log_info.read()
    #         yield data.encode()
    #         time.sleep(1)
    #     # Create empty job.log, old logging will be deleted
    #     open("static/job.log", 'w').close()

    #AGGIUNGERE IL TAILING DI FILE.LOG
    logfile = open("file.log","r")
    loglines = follow(logfile)
    for line in loglines:
        data = line
        yield data.encode()
        time.sleep(0.1)
    logfile.close()


@app.get("/")
def root(request: Request):
    #return render_template('index.html')
    return templates.TemplateResponse(name='index.html',context={'request': request})

@app.get("/log_stream/")
def stream(request: Request):
    """returns logging information"""
    return StreamingResponse(test_logger_window(), media_type="text/event-stream")

@app.get("/scrapeNow")
def scrapeNow(request: Request,background_tasks: BackgroundTasks):
    f = open("status.lock", "r")
    a = f.read()
    f.close()
    print(a)
    if a == 'IDLE':
        f = open("status.lock", "w+")
        f.truncate(0)
        f.write("IN PROGRESS")
        f.close()
        background_tasks.add_task(crawler.scrape_and_insert)
        return templates.TemplateResponse(name='scrapeNow.html',context={'request': request})
    else:
        return RedirectResponse(url='/')

@app.post("/scrapeandinsert/")
def scrapeAndInsert(background_tasks: BackgroundTasks):
    f = open("status.lock", "r")
    a = f.read()
    f.close()
    if a == 'IDLE':
        f = open("status.lock", "w+")
        f.truncate(0)
        f.write("IN PROGRESS")
        f.close()
        background_tasks.add_task(crawler.scrape_and_insert)
        return {"message": "Notification sent in the background"}
    else:
        return HTTPException(status_code=423)

@app.post("/onlyinsert/")
def onlyInsert(background_tasks: BackgroundTasks):
    f = open("status.lock", "r")
    a = f.read()
    f.close()
    if a == 'IDLE':
        f = open("status.lock", "w+")
        f.truncate(0)
        f.write("IN PROGRESS")
        f.close()
        background_tasks.add_task(crawler.only_insert)
        return {"message": "Notification sent in the background"}
    else:
        return HTTPException(status_code=423)

@app.post("/switch/")
def switchOnFail():
    f = open("status.lock", "r")
    a = f.read()
    f.close()
    if a == 'IDLE':
        f = open("status.lock", "w+")
        f.truncate(0)
        f.write("IN PROGRESS")
        f.close()
        return {"message": "Forced switch to IN PROGRESS state"}
    elif a == 'IN PROGRESS':
        f = open("status.lock", "w+")
        f.truncate(0)
        f.write("IDLE")
        f.close()
        return {"message": "Forced switch to IDLE state"}
    else:
        return HTTPException(status_code=423)

@app.get("/status/")
def getStatus():
    f = open("status.lock", "r")
    a = f.read()
    f.close()
    return {"message": "The current state is "+a}

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=5000)


