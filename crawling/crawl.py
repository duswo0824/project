# crawl.py

import httpx
from bs4 import BeautifulSoup
import pandas as pd

from logger import Logger
from database import get_engine

logger = Logger().get_logger(__name__)

url = "https://www.chosun.com/arc/outboundfeeds/rss/?outputType=xml"

headers = {
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "If-Modified-Since": "0",
    "User-Agent": "Mozilla/5.0"
}

# main에서 호출될 함수 (FastAPI가 실행)
async def crawl_rss():
    logger.info("조선일보 메인 RSS 50개 크롤링 시작")

    async with httpx.AsyncClient() as client:
        resp = await client.get(url, headers=headers)
    # lxml-xml → RSS/Atom 파싱에 최적화
    soup = BeautifulSoup(resp.text, "lxml-xml")

    items = soup.find_all("item")[:50]
    logger.info(f"가져온 item 수: {len(items)}")

    # 데이터 저장 리스트
    titles, links, pub_dates = [], [], []
    categories, creators, descs = [], [], []
    contents, images, comments, guids = [], [], [], []
    contents_p = [] # ← 본문 텍스트만 저장 <p> 태그

    info_list = []

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

        # 첫 번째 <p> 태그만 추출
        soup2 = BeautifulSoup(content_html, "html.parser")
        first_p = soup2.find("p")
        content_p = first_p.get_text(strip=True) if first_p else ""

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
        contents_p.append(content_p)  # p태그 본문 한줄
        images.append(image)
        comments.append(comment)
        guids.append(guid)

        info_list.append({
            "title": title,
            "link": link,
            "pub_date": pub_date
        })

    # DataFrame → DB 저장
    df = pd.DataFrame({
        "title": titles,
        "link": links,
        "pub_date": pub_dates,
        "category": categories,
        "creator": creators,
        "description": descs,
        "content_html": contents,
        "contents_p": contents_p,  # 첫 번째 p 태그 텍스트
        "image": images,
        "comments": comments,
        "guid": guids
    })
    # DB 저장
    df.to_sql("chosun", con=get_engine(), if_exists="replace", index=False)
    # CSV 파일 저장
    df.to_csv("./driver/chosun.csv", index=False, encoding="utf-8-sig")

    logger.info(f"{len(df)}개 기사 저장 완료 (chosun 테이블)")

    return len(df)