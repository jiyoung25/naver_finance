import time
import logging
import logging.config
from concurrent.futures import ThreadPoolExecutor, as_completed
from naver_finance_collector_utils import RequestsProxy, DataParsing, SaveData

"""
PC 여러대 나누어 돌리기
1) 주임님 : 시황정보, 투자정보
2) 나: 종목분석, 채권분석
3) 앞컴퓨터: 산업분석, 경제분석

# 주의사항
1) main의 categories, utils의 categories, func_crawling_Common의 getSelectUrl부분을 해당 category에 맞게 조정하고 돌리기
2) aws 경로 확인하기
3) db 테이블이 맞는지 확인
"""

# 로깅 설정 파일 로드
logging.config.fileConfig('logging.conf')
# 로거 생성
logger = logging.getLogger('naver_finance_collector')

MAX_WORKERS = 5  # 최대 스레드풀

def collect_and_save_data(category, page, host):
    logging.info(f"Report Type: {category[0]}, Value: {category[1]}")
    # 1. 페이지 접속
    try:
        category_url = f"{host}{category[1]}_list.naver?&page={page}"
        response_data = RequestsProxy.requests_proxy(category_url)
    except Exception as e:
        logging.error(
            f"페이지 접속 실패\n URL: {category_url}\n Error: {str(e)}")
        return

    # 2. arrData 변수에 list페이지에서 추출한 기본 정보
    arrData = DataParsing.extract_basic_info(
        response_data, category, host)

    # 3. arrData 변수에 상세 페이지에서 추출한 metadata와 summary저장하기
    arrData = DataParsing.get_detailPage_data(arrData, {}, page)

    # 4. db 저장
    if category[0] == '시황정보':
        cat_idx = 1
    elif category[0] == '투자정보':
        cat_idx = 2
    elif category[0] == '종목분석':
        cat_idx = 3
    elif category[0] == '산업분석':
        cat_idx = 4
    elif category[0] == '경제분석':
        cat_idx = 5
    elif category[0] == '채권분석':
        cat_idx = 6
    try:
        SaveData.save_data_to_db(cat_idx, arrData)
    except Exception as e:
        logging.error(f'{category[0]}-page:{page}::: db insert 실패' + str(e))
        breakpoint()


def save_pdf_to_aws():
    # 5. AWS에 저장
    SaveData.save_data_to_aws()


def save_data_to_json(limit: int):
    SaveData.save_data_to_json(limit)


def main():
        try:
            # 시간 체크
            start_time = time.strftime('%Y-%m-%d %I:%M:%S %p')

            # url 기본 정보
            host = 'https://finance.naver.com/research/'

            # 카테고리 정보 각 [0]: 한글이름 [1]: 영어이름 [2]: 마지막 페이지(루프 돌 때 +1하기)
            categories = {
                #0: ["시황정보", "market_info", 768],
                #1: ["투자정보", "invest", 855],
                2: ["종목분석", "company", 1977],
                #3: ["산업분석", "industry", 1018],
                #4: ["경제분석", "economy", 285],
                5: ["채권분석", "debenture", 193],
            }

            # 1. db에 저장
            # 모든 카테고리 및 페이지에 대한 작업을 futures 리스트에 저장
            futures = []
            for category in categories.values():
                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    futures = {
                        executor.submit(collect_and_save_data, category, page, host) : page for page in range(1,category[2]+1)
                    }
                    for future in as_completed(futures):
                        if future.exception() is not None:
                            logging.error(
                                f"Thread caused an error: {future.exception()}")

            # pdf 정보를 db저장 완료하고 실행되는 작업
            # 2. aws에 저장
            save_pdf_to_aws()
            # 3. json으로 변환
            save_data_to_json(1000)

            end_time = time.strftime('%Y-%m-%d %I:%M:%S %p')
            logging.info(f'start time: {start_time}')
            logging.info(f'end time: {end_time}')

        except Exception as exc:
            logging.error(f"Processing for generated an exception: {exc}")


if __name__ == '__main__':
    main()
