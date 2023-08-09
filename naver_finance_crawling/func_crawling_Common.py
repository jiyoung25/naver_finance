from threading import Lock
import pymysql
import json


class Database:

    def __init__(self):
        self.con = None
        self.cursor = None
        self.buffered_data = []
        self.lock = Lock()  # add a lock object

    def connect(self):
        if self.con is None:
            self.con = pymysql.connect(
                host='123.214.171.156',
                user='root',
                password='metrix',
                db='LG_Chemeo',
                charset='utf8mb4',  # add charset type
                cursorclass=pymysql.cursors.DictCursor)
            self.cursor = self.con.cursor()

    def disconnect(self):
        self.con.close()

    def insert(self, cat_idx: int, data):
        try:
            with self.lock:  # acquire the lock
                self.buffered_data.append(data)
                if len(self.buffered_data) >= 1:  # batch size
                    with self.con.cursor() as cursor:
                        sql = """
                        INSERT INTO LG_NAVER_FINANCE.naver_finance_db
                        (cat_idx, category, title, pageUrl, publishedBy, pdfUrl, reportId, publishedDate, meta, summary, sort, page, s_date) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, DATE_FORMAT(NOW(),'%%Y%%m%%d%%H%%i%%s'))
                        """
                        cursor.executemany(sql, [
                            (
                                cat_idx,
                                item['category'],
                                item['title'],
                                item['pageUrl'],
                                item['publishedBy'],
                                item['pdfUrl'],
                                item['reportId'],
                                item['publishedDate'],
                                json.dumps(item['meta'], ensure_ascii=False).encode('utf-8'),
                                json.dumps(item['summary'], ensure_ascii=False).encode('utf-8'),
                                item['sort'] if cat_idx == 3 or cat_idx == 4 else 'None',
                                item['page']
                            ) for item in self.buffered_data
                        ])
                        self.con.commit()
                        print(f'{cat_idx}/{data["pageUrl"]}success commit!')
        except Exception as e:
            print(e)
        finally:
            self.buffered_data = []

    def batch_insert(self, data, meta, summary):
        try:
            with self.lock:  # acquire the lock
                self.buffered_data.append(data)
                if len(self.buffered_data) >= 100:  # batch size
                    with self.con.cursor() as cursor:
                        sql = """
                        INSERT INTO LG_NEWS_2023.cnbc_data 
                        (site, type, tags, title, category, writer, dateCreated, datePublished, meta, content, url, s_date) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, DATE_FORMAT(NOW(),'%%Y%%m%%d%%H%%i%%s'))
                        """
                        cursor.executemany(sql, [
                            (
                                item["site"],
                                item["type"],
                                item["tags"],
                                item['Title'],
                                item['category'],
                                # 'Writer' 필드의 'content'를 문자열로 변환
                                item['Writer'],
                                item['dateCreated'],
                                item['datePublished'],
                                item["meta"],
                                item["Contents"],
                                item['URL']
                            ) for item in self.buffered_data
                        ])
                        self.con.commit()
                        print('success commit!')

                        # Update URL state for each data item
                        for item in self.buffered_data:
                            with self.con.cursor() as cursor:
                                url = item['URL']
                                sql = 'update LG_NEWS_2023.cnbc_url set ul_state = 1 where ul_url = "%s"' % url
                                cursor.execute(sql)
                                self.con.commit()
                                print(f'Updated URL: {url}')

                        print('All updates successful')
        except Exception as e:
            print('Error occurred:', str(e))
            print(data)
            return None
        finally:  # Clear the buffer even if an exception occurred
            self.buffered_data = []

    def updateNewsComplete(self, url):
        with self.con.cursor() as self.cursor:
            sql = 'update LG_NEWS_2023.cnbc_url set ul_state = 1 where ul_url = "%s"' % (
                url)
            self.cursor.execute(sql)
            self.con.commit()
        print('updateNewsComplete success')

    def getSelectUrl(self):
        sql = '''SELECT cat_en_name, pdfURL, reportId
            FROM LG_NAVER_FINANCE.naver_finance_db t1
            LEFT OUTER JOIN LG_NAVER_FINANCE.naver_finance_common_info t2 ON t1.cat_idx = t2.cat_idx
            WHERE t1.cat_idx = 3 or t1.cat_idx = 6
            '''

        self.cursor.execute(sql)
        self.con.commit()
        rows = self.cursor.fetchall()
        return rows

    def getSelectAllList(self, cat_idx: int, limit_start: int, limit_num: int):
        sql = f'''SELECT *
                FROM (SELECT cat_kor_name, cat_en_name, title, pageUrl, publishedBy, publishedDate, pdfUrl, reportId, meta, summary, sort, page
                        FROM LG_NAVER_FINANCE.naver_finance_db t1
                        LEFT OUTER JOIN LG_NAVER_FINANCE.naver_finance_common_info t2 ON t1.cat_idx = t2.cat_idx
                        WHERE t1.cat_idx = {cat_idx}
                        limit {limit_start}, {limit_num}) t1
                ORDER BY page, pageUrl asc'''

        self.cursor.execute(sql)
        self.con.commit()
        rows = self.cursor.fetchall()
        return rows
