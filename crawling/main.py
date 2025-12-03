from fastapi import FastAPI
from starlette.responses import RedirectResponse
from starlette.staticfiles import StaticFiles

from logger import Logger
from database import get_engine

# 외부파일에서 함수 가져오기
from crawl import crawl_rss
from section import crawl_sections

app = FastAPI()
logger = Logger().get_logger(__name__)

app.mount("/view", StaticFiles(directory="view"), name="view")

@app.get("/")
def root():
    return RedirectResponse(url="/view/index.html")

# 첫 번째 API: 조선일보 전체기사 30개 RSS (crawl.py)
@app.get("/crawl")
async def crawl_rss_api():
    count = await crawl_rss()
    return {"count": count}

# 두 번째 API: 섹션별 10개씩 (section.py)
@app.get("/section")
async def crawl_sections_api():
    count = await crawl_sections()
    return {"count": count}
