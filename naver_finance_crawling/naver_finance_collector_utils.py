import urllib3
import requests
from bs4 import BeautifulSoup
import logging.config
import json
from func_crawling_Common import Database
from s3_utils import S3Wrapper
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.config.fileConfig('logging.conf')
logger = logging.getLogger('naver_finance_collector')

# db connect
db = Database()
db.connect()
# Amazon S3
s3_wrapper = S3Wrapper()
MAX_WORKERS = 5  # 최대 스레드풀

class RequestsProxy:
    def requests_proxy(url: str):
        # warning문구 안뜨게 하기
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        apikey = '6a2b50ab4d95f0541c94d3ffd44fc64b'
        proxies = {
            "http": f"http://scraperapi:{apikey}@proxy-server.scraperapi.com:8001"
        }
        # scraper api의 프록시를 이용해 get
        try:
            r = requests.get(url, proxies=proxies, verify=False)
            return r
        except Exception as e:
            logger.error(
                f'API 응답데이터 파싱 실패 \nURL: {url} \nERROR: {e}'
            )


class DataParsing:
    # 리포트 리스트 페이지에서 정보 얻기
    # url ex) https://finance.naver.com/research/market_info_list.naver
    def extract_basic_info(response, category: list, host: str):
        try:
            total_data = {
                'common': {},
                'reportList': []
            }
            soup = BeautifulSoup(
                response.content, 'html.parser', from_encoding='utf-8')
            total_data['common']['site'] = 'naver finance'
            total_data['common']['menu'] = 'research'
            total_data['common']['typeName'] = soup.select_one(
                'title').text
            total_data['common']['type'] = category[1]
            total_data['common']['category'] = f'증권홈 > 리서치 > {category[0]} 리포트'
            table = soup.select_one('.box_type_m').select_one('.type_1')
            trs = table.select('tr')
            if category[0] == '시황정보' or category[0] == '투자정보' or category[0] == '경제분석' or category[0] == '채권분석':
                total_data = DataParsing.get_list_info_v1(
                    trs, total_data, host)
            elif category[0] == '종목분석' or category[0] == '산업분석':
                total_data = DataParsing.get_list_info_v2(
                    trs, total_data, host)
            return total_data
        except Exception as e:
            logger.error(f'기본정보 추출 실패\nError: %s', e)

    # 각각의 tr에서 list 정보 얻어내기
    # v1: 시황정보 리포트, 투자정보 리포트, 경제분석 리포트, 채권분석 리포트
    # v2: 종목분석 리포트, 산업분석 리포트
    def get_list_info_v1(trs: list, total_data: dict, host: str):
        null_td = False
        for tr_idx, tr in enumerate(trs):
            try:
                if tr_idx == 0:
                    continue
                tmp_data = {}
                tmp_data['category'] = total_data['common']['category']
                tds = tr.select('td')
                for td_idx, td in enumerate(tds):
                    if td_idx == 0:
                        title = td.text
                        if title == '':
                            null_td = True
                            continue
                        else:
                            page_url = td.find('a')['href']
                            tmp_data['title'] = title
                            tmp_data['pageUrl'] = host+page_url
                            null_td = False
                    if null_td == True:
                        continue
                    if td_idx == 1:
                        firm = td.text
                        tmp_data['publishedBy'] = firm
                    if td_idx == 2:
                        if td == None:
                            continue
                        try:
                            pdfUrl = td.find('a')['href']
                            tmp_data['pdfUrl'] = pdfUrl
                            last_slash_num = pdfUrl.find(
                                'research/') + len('research/')
                            reportId_start = pdfUrl.find(
                                '/', last_slash_num) + 1
                            reportId_end = pdfUrl.find('.pdf')
                            reportId = pdfUrl[reportId_start:reportId_end]
                            tmp_data['reportId'] = reportId
                        except Exception as e:
                            logger.error(
                                f'PDF FILE non exist error: \nPDF PAGE URL::: {host}{page_url}')
                            tmp_data['pdfUrl'] = 'None'
                            tmp_data['reportId'] = 'None'
                    if td_idx == 3:
                        date = td.text
                        tmp_data['publishedDate'] = date
                if null_td == False:
                    total_data['reportList'].append(tmp_data)
            except Exception as e:
                logger.error(f'기본정보 - tr에서 리포트 리스트 정보 추출 실패\nError: %s', e)
        return total_data

    # 각각의 tr에서 list 정보 얻어내기
    # v1: 시황정보 리포트, 투자정보 리포트, 경제분석 리포트, 채권분석 리포트
    # v2: 종목분석 리포트, 산업분석 리포트
    def get_list_info_v2(trs: list, total_data: dict, host: str):
        null_td = False
        for tr_idx, tr in enumerate(trs):
            try:
                if tr_idx == 0:
                    continue
                tmp_data = {}
                tmp_data['category'] = total_data['common']['category']
                tds = tr.select('td')
                for td_idx, td in enumerate(tds):
                    if td_idx == 0:  # v2에선 td_idx에 종목명이나 산업명이 있음
                        sort = td.text
                        if sort == '':
                            null_td = True
                            continue
                        else:
                            sort = sort.replace('\n', '')
                            tmp_data['sort'] = sort
                    if td_idx == 1:
                        title = td.text
                        page_url = td.find('a')['href']
                        tmp_data['title'] = title
                        tmp_data['pageUrl'] = host+page_url
                        null_td = False
                    if null_td == True:
                        continue
                    if td_idx == 2:
                        firm = td.text
                        tmp_data['publishedBy'] = firm
                    if td_idx == 3:
                        if td == None:
                            continue
                        try:
                            pdfUrl = td.find('a')['href']
                            tmp_data['pdfUrl'] = pdfUrl
                            last_slash_num = pdfUrl.find(
                                'research/') + len('research/')
                            reportId_start = pdfUrl.find(
                                '/', last_slash_num) + 1
                            reportId_end = pdfUrl.find('.pdf')
                            reportId = pdfUrl[reportId_start:reportId_end]
                            tmp_data['reportId'] = reportId
                        except Exception as e:
                            logger.error(
                                f'PDF FILE non exist error: \nPDF PAGE URL::: {host}{page_url}')
                            tmp_data['pdfUrl'] = 'None'
                            tmp_data['reportId'] = 'None'
                    if td_idx == 4:
                        date = td.text
                        tmp_data['publishedDate'] = date
                if null_td == False:
                    total_data['reportList'].append(tmp_data)
            except Exception as e:
                logger.error(f'기본정보 - tr에서 리포트 리스트 정보 추출 실패\nError: %s', e)
                logger.debug(tr)
        return total_data

    def convert_list_to_json(data: list):
        try:
            dump_list = json.dumps(data)
            json_load = json.loads(dump_list)
            return json_load
        except Exception as e:
            logger.error(f'json 변환 실패\nError: %s', e)

    # 상세페이지에서 metadata얻기
    # url ex) https://finance.naver.com/research/market_info_read.naver?nid=25886&page=1
    def get_metadata(soup, tmp_meta: dict, page_link):
        try:
            arrMeta = []
            metadatas = soup.find_all('meta')
            for metadata in metadatas:
                if 'property' in metadata.attrs and 'content' in metadata.attrs:
                    tmp_meta['property'] = metadata.attrs['property']
                    tmp_meta['content'] = metadata.attrs['content']
                    arrMeta.append(tmp_meta)
                    # append후 tmp_meta 초기화
                    tmp_meta = {}
            return arrMeta
        except Exception as e:
            logger.error(f'메타데이터 생성 실패\nError: %s\nPAGE LINK:::{page_link}', e)

    # 상세페이지에서 summary얻기
    # url ex) https://finance.naver.com/research/market_info_read.naver?nid=25886&page=1
    def get_summary(soup, page_link):
        try:
            summary_data = []
            summary_data_tmp = {}
            # 경우의 수 : 1) summary_title 0개, 2) summary_title 1개, 3) summary_title 2개 이상
            # summary: [{title:'', content:''},{},{}]
            table = soup.select_one('.type_1')
            tr = table.select_one('.view_cnt') # summary에 해당하는 tr
            tr_ver2 = False  # title과 contents의 내용이 p에 담겨 있는 경우와 div에 담겨있는 경우가 있음
            ps = tr.select('p')
            ps_len = len(ps)
            if ps_len == 0:
                tr_ver2 = True
            summary_data_tmp['title'] = ''  # summary_title이 0개일 때를 대비
            summary_data_tmp['content'] = ''  # 첫 번째 요약인지 확인
            # 1) p안에 title과 content담겨있는 경우
            if tr_ver2 == False:
                for p_index, p in enumerate(ps):
                    if ('<strong>' and '</strong>') in str(p):
                        # 둘 다 ''이면 첫 번째 요약
                        if summary_data_tmp['title'] == '' and summary_data_tmp['content'] == '':
                            title = p.select_one('strong').text
                            summary_data_tmp['title'] = title
                            # 하나의 p 안에 소제목과 내용 함께 있는 경우가 있다.
                            title_len = len(title)
                            content = p.text[title_len+1:]
                            summary_data_tmp['content'] += content
                        else:  # 아니면 두 번째 이상의 요약 내용이므로, 현재까지 있었던 내용을 summary_data에 append 후 새로운 dict로 시작
                            summary_data.append(summary_data_tmp)
                            summary_data_tmp = {}
                            summary_data_tmp['title']= ''
                            summary_data_tmp['content'] = ''
                            title = p.select_one('strong').text
                            summary_data_tmp['title'] = title
                            # 하나의 p 안에 소제목과 내용 함께 있는 경우가 있다.
                            title_len = len(title)
                            content = p.text[title_len+1:]
                            summary_data_tmp['content'] += content
                    else:
                        summary_data_tmp['content'] += p.text
                    if p_index + 1 == ps_len:
                        # if문이 끝나면 summary_data에 tmp내용 append
                        summary_data.append(summary_data_tmp)
            if tr_ver2 == True:
                try:
                    summary_data_tmp['title'] = tr.select_one('font').text
                except:
                    try:
                        summary_data_tmp['title'] = tr.select_one('b').text
                    except:
                        summary_data_tmp['title'] = ''
                summary_data_tmp['content'] = tr.text
                title_len = len(summary_data_tmp['title'])
                summary_data_tmp['content'] = summary_data_tmp['content'][title_len+2:]
                summary_data.append(summary_data_tmp)
            return summary_data
        except Exception as e:
            logger.error(f'summary 생성 실패\nError: %s\nPAGE LINK:::{page_link}', e)

    # get_summary와 get_metadata를 합침
    def get_detailPage_data(total_data: dict, tmp_meta: dict, page: int):
        try:
            for data in total_data['reportList']:
                page_link = data['pageUrl']
                page_r = RequestsProxy.requests_proxy(page_link)
                page_soup = BeautifulSoup(
                    page_r.content, 'html.parser', from_encoding='utf-8')
                data['page'] = page
                meta = DataParsing.get_metadata(page_soup, tmp_meta, page_link)
                data['meta'] = meta
                summary = DataParsing.get_summary(page_soup, page_link)
                data['summary'] = summary
            return total_data
        except Exception as e:
            logger.error(f'detailPage 파싱 실패\nError: %s', e)
            logger.debug(total_data['reportList'])

class SaveData:
    # db에 각각의 데이터 저장
    def save_data_to_db(cat_idx, arrData):
        db_insert_list = arrData['reportList']
        for data in db_insert_list:
            try:
                db.insert(cat_idx, data)
            except:
                logger.error(f'db insert 실패 \nPAGEURL:{data["pageUrl"]}')

    # aws에 pdf 저장
    def save_data_to_aws():
        arrList = db.getSelectUrl()
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                            executor.submit(SaveData.save_data_to_aws_common, list) : list for list in arrList
                        }
            for future in as_completed(futures):
                if future.exception() is not None:
                    logging.error(
                        f"Thread caused an error: {future.exception()}")

    def save_data_to_aws_common(list):
        pdf_link = list['pdfURL']
        category = list['cat_en_name']
        if pdf_link != 'None':
            pdf_r = RequestsProxy.requests_proxy(url=pdf_link)
            if pdf_r.status_code == 200:
                pdf_data = pdf_r.content
                try:
                    s3_wrapper.save_pdf(category, list['reportId'], pdf_data)
                except:
                    logger.error(f'aws upload error: {pdf_link}')
            else:
                logger.error(f'aws response error: {pdf_link}')

    # json 형태로 저장
    def save_data_to_json(limit:int):
        # 카테고리 정보 각 [0]: 한글이름 [1]: 영어이름 [2]: 마지막 페이지(루프 돌 때 +1하기)
        categories = {
            #0: [1, "시황정보", "market_info", 768],
            #1: [2, "투자정보", "invest", 855],
            2: [3, "종목분석", "company", 1977],
            #3: [4, "산업분석", "industry", 1018],
            #4: [5, "경제분석", "economy", 285],
            5: [6, "채권분석", "debenture", 193],
        }
        # 각 카테고리 별로 데이터 수집 및 저장
        for category in categories.values():
            SaveData.save_json_common(category, limit)
            
    def save_json_common(category, limit):
        cat_idx = category[0]
        total_pdf_num = category[3] * 30
        if cat_idx == 3 or cat_idx == 4:
            common_data = {'site':'naver finance', 'menu':'research'}
            for idx, limit_start in enumerate(range(0, total_pdf_num, limit)):
                reportList = []
                report_tmp = {}
                arrList = db.getSelectAllList(cat_idx, limit_start, limit)
                if limit_start == 0:
                    common_data['typeName'] = f'{arrList[0]["cat_kor_name"]} 리포트 : 네이버 증권'
                    common_data['type'] = f'{arrList[0]["cat_en_name"]}'
                    common_data['category'] = f'증권홈 > 리서치 > {arrList[0]["cat_kor_name"]} 리포트'
                for list in arrList:
                    report_tmp['title'] = list['title']
                    report_tmp['publishedBy'] = list['publishedBy']
                    report_tmp['pdfUrl'] = list['pdfUrl']
                    report_tmp['reportId'] = list['reportId']
                    report_tmp['publishedDate'] = list['publishedDate']
                    report_tmp['page'] = list['page']
                    report_tmp['meta'] = list['meta']
                    report_tmp['summary'] = list['summary']
                    reportList.append(report_tmp)
                    report_tmp['sort'] = list['sort']
                arrJsonData = {'common_data' : common_data, 'reportList' : reportList}
                b = DataParsing.convert_list_to_json(arrJsonData)
                with open(f'{category[2]}_{idx+1}.json', "w", encoding='utf-8-sig') as f:
                            json.dump(b, f, ensure_ascii=False)
        else:
            common_data = {'site':'naver finance', 'menu':'research'}
            for idx, limit_start in enumerate(range(0, total_pdf_num, limit)):
                reportList = []
                report_tmp = {}
                arrList = db.getSelectAllList(cat_idx, limit_start, limit)
                if limit_start == 0:
                    common_data['typeName'] = f'{arrList[0]["cat_kor_name"]} 리포트 : 네이버 증권'
                    common_data['type'] = f'{arrList[0]["cat_en_name"]}'
                    common_data['category'] = f'증권홈 > 리서치 > {arrList[0]["cat_kor_name"]} 리포트'
                for list in arrList:
                    report_tmp['title'] = list['title']
                    report_tmp['publishedBy'] = list['publishedBy']
                    report_tmp['pdfUrl'] = list['pdfUrl']
                    report_tmp['reportId'] = list['reportId']
                    report_tmp['publishedDate'] = list['publishedDate']
                    report_tmp['page'] = list['page']
                    report_tmp['meta'] = list['meta']
                    report_tmp['summary'] = list['summary']
                    reportList.append(report_tmp)
                arrJsonData = {'common_data' : common_data, 'reportList' : reportList}
                b = DataParsing.convert_list_to_json(arrJsonData)
                with open(f'{category[2]}_{idx+1}.json', "w", encoding='utf-8') as f:
                            json.dump(b, f, ensure_ascii=False)