from starlette.staticfiles import StaticFiles
from starlette.responses import RedirectResponse,Response,StreamingResponse
from starlette.templating import Jinja2Templates
from loguru import logger
import datetime, time
from fastapi import BackgroundTasks, FastAPI,Request, Form
import uvicorn
import crawler as crawler
from multiprocessing import Process

# configure logger
logger.add("static/job.log", format="{time} - {message}")


app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

def sleeping(message="RUNNING"):
    print(message)
    time.sleep(15)
    print("finished")

def test_logger_window():
    """creates logging information"""
    with open("static/job.log") as log_info:
        for i in range(25):
            logger.info(f"iteration #{i}")
            data = log_info.read()
            yield data.encode()
            time.sleep(1)
        # Create empty job.log, old logging will be deleted
        open("static/job.log", 'w').close()



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
        f.write("IDLE")
        f.close()
        background_tasks.add_task(crawler.launch)
        return templates.TemplateResponse(name='scrapeNow.html',context={'request': request})
    else:
        return RedirectResponse(url='/')

@app.post("/scrape/")
def scrape(background_tasks: BackgroundTasks):
    #background_tasks.add_task(crawler.launch)
    return {"message": "Notification sent in the background"}




if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=5000)


