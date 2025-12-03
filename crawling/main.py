import httpx
from bs4 import BeautifulSoup
import pandas as pd
from fastapi import FastAPI
from starlette.responses import RedirectResponse
from starlette.staticfiles import StaticFiles

from logger import Logger  # logger.py (로그 기록)
from database import get_engine  # DB 연결 함수

app = FastAPI()
logger = Logger().get_logger(__name__)

# /view 폴더 정적 파일 설정
app.mount("/view", StaticFiles(directory="view"), name="view")


@app.get("/")
def root():
    return RedirectResponse(url="/view/index.html")

# /crawl : 조선일보 RSS 30개 크롤링 API
@app.get("/crawl")
async def crawl_rss():
    logger.info("조선일보 RSS 크롤링 시작")

    # RSS 주소
    url = "https://www.chosun.com/arc/outboundfeeds/rss/?outputType=xml"

    # 304 Not Modified 우회용 헤더
    headers = {
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "If-Modified-Since": "0",
        "User-Agent": "Mozilla/5.0"
    }

    # RSS XML 요청
    async with httpx.AsyncClient() as client:
        resp = await client.get(url, headers=headers)

    # BeautifulSoup XML 파싱
    soup = BeautifulSoup(resp.text, "xml")

    # <item> 30개만 선택
    items = soup.find_all("item")[:30]
    logger.info(f"가져온 RSS item 수: {len(items)}")

    # DB 저장 리스트
    guids, titles, links = [], [], []
    comments_list, pub_dates = [], []
    descs, contents = [], []
    creators, categories, images = [], [], []

    # 프론트 출력용
    info_list = []

    # RSS item 반복 처리
    for item in items:
        title = item.title.text if item.title else ""
        link = item.link.text if item.link else ""
        pub_date = item.pubDate.text if item.pubDate else ""

        category = item.category.text if item.category else ""

        creator_tag = item.find("dc:creator")
        creator = creator_tag.text if creator_tag else ""

        description = item.description.text if item.description else ""

        encoded = item.find("content:encoded")
        content_html = encoded.text if encoded else ""

        media = item.find("media:content")
        image = media["url"] if media and media.has_attr("url") else ""

        comment = item.comments.text if item.comments else ""

        guid = item.guid.text if item.guid else ""

        # 리스트 누적
        titles.append(title)
        links.append(link)
        pub_dates.append(pub_date)
        categories.append(category)
        creators.append(creator)
        descs.append(description)
        contents.append(content_html)
        images.append(image)
        comments_list.append(comment)
        guids.append(guid)

        # 프론트 표시용 간단 데이터
        info_list.append({
            "title": title,
            "link": link,
            "pub_date": pub_date
        })

    # DataFrame 생성 → DB 저장 (for문 밖)
    df = pd.DataFrame({
        "title": titles,
        "link": links,
        "pub_date": pub_dates,
        "category": categories,
        "creator": creators,
        "description": descs,
        "content_html": contents,
        "image": images,
        "comments": comments_list,
        "guid": guids
    })

    # MySQL 저장
    df.to_sql("chosun", con=get_engine(), if_exists="replace", index=False)

    logger.info(f"{len(df)}개의 뉴스 저장 완료")

    # 프론트엔드 응답 (for문 밖)
    return {"count": len(df), "list": info_list}
