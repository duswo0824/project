# section.py
import httpx
from bs4 import BeautifulSoup
import pandas as pd

from logger import Logger
from database import get_engine

logger = Logger().get_logger(__name__)

# 조선일보 섹션별 RSS URL 목록
rss_sources = {
    "정치": "https://www.chosun.com/arc/outboundfeeds/rss/category/politics/?outputType=xml",
    "경제": "https://www.chosun.com/arc/outboundfeeds/rss/category/economy/?outputType=xml",
    "사회": "https://www.chosun.com/arc/outboundfeeds/rss/category/national/?outputType=xml",
    "국제": "https://www.chosun.com/arc/outboundfeeds/rss/category/international/?outputType=xml",
    "문화": "https://www.chosun.com/arc/outboundfeeds/rss/category/culture-life/?outputType=xml",
    "오피니언": "https://www.chosun.com/arc/outboundfeeds/rss/category/opinion/?outputType=xml",
    "스포츠": "https://www.chosun.com/arc/outboundfeeds/rss/category/sports/?outputType=xml",
    "연예": "https://www.chosun.com/arc/outboundfeeds/rss/category/entertainments/?outputType=xml"
}

# 304 Not Modified 캐시 방지 헤더
headers = {
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "If-Modified-Since": "0",
    "User-Agent": "Mozilla/5.0"
}

# 섹션별 기사 10개씩 크롤링하는 메인 함수
async def crawl_sections():

    logger.info("조선일보 섹션별 RSS 10개씩 크롤링 시작")

    titles, links, pub_dates = [], [], []
    categories = []
    creators, descs, contents = [], [], []
    contents_p = []  # ← 본문 텍스트만 저장 <p> 태그
    images, comments_list, guids = [], [], []

    async with httpx.AsyncClient() as client:

        # RSS URL 하나씩 크롤링
        for section_name, rss_url in rss_sources.items():
            logger.info(f"[{section_name}] 요청중 → {rss_url}")

            resp = await client.get(rss_url, headers=headers)
            # Arc RSS는 resp.text 사용하면 깨져서 item 인식 안 됨
            xml_text = resp.content.decode("utf-8", errors="ignore")
            # MUST USE: lxml-xml → RSS/Atom 파싱에 최적화
            soup = BeautifulSoup(resp.text, "lxml-xml")

            # 각 섹션별 수집 최대 10개
            items = soup.find_all("item")[:10]
            logger.info(f"[{section_name}] item 수: {len(items)}")

            # item 반복 처리
            for item in items:
                title = item.title.text if item.title else ""
                link = item.link.text if item.link else ""
                pub_date = item.pubDate.text if item.pubDate else ""

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
                categories.append(section_name)  # 중요: 섹션 기록
                creators.append(creator)
                descs.append(description)
                contents.append(content_html)
                contents_p.append(content_p) # p태그 본문 한줄?
                images.append(image)
                comments_list.append(comment)
                guids.append(guid)

    # DataFrame 구성 (중요도 순서 정렬)
    df = pd.DataFrame({
        "title": titles,
        "link": links,
        "pub_date": pub_dates,
        "category": categories,
        "creator": creators,
        "description": descs,
        "content_html": contents,  # 전체 HTML
        "contents_p":contents_p, # 첫 번째 p 태그 텍스트
        "image": images,
        "comments": comments_list,
        "guid": guids
    })

    # DB 저장 (새로운 테이블 chosun_section)
    engine = get_engine()
    df.to_sql("chosun_section", con=engine, if_exists="replace", index=False)
    # CSV 파일 저장
    df.to_csv("./driver/chosun_section.csv", index=False, encoding="utf-8-sig")

    logger.info(f"총 {len(df)}개의 기사 저장 완료 (테이블명: chosun_section)")

    return len(df)