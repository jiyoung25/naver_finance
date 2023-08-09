import boto3
import configparser
import uuid
import datetime
import io
import tempfile


class S3Wrapper(object):

    def __init__(self):
        self.parser = configparser.ConfigParser()
        # 해당 경로로 이동 후 실행
        self.parser.read("cred.conf")
        self.conn = None

    def connect(self):
        access_key = self.parser.get("aws_boto_credentials", "access_key")
        secret_key = self.parser.get("aws_boto_credentials", "secret_key")

        self.conn = boto3.client(
            's3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key)

    def save(self, from_path, bucket_name, to_path):
        try:
            self.conn.upload_file(
                from_path,
                bucket_name,
                to_path)
            return 0

        except Exception as e:
            print(e)
            return -1

    def get_all(self, bucket_name, prefix):
        ctoken = None
        contents = list()
        count_keys = 0
        while True:
            if ctoken is not None:
                response = self.conn.list_objects_v2(
                    Bucket=bucket_name,
                    Prefix=prefix,
                    ContinuationToken=ctoken)
            else:
                response = self.conn.list_objects_v2(
                    Bucket=bucket_name,
                    Prefix=prefix)

                contents += response['Contents']
                count_keys += len(response['Contents'])

            if response['IsTruncated'] is False:
                break
            else:
                ctoken = response['NextContinuationToken']

        result = {
            'Bucket': bucket_name,
            'Prefix': prefix,
            'Contents': contents,
            'KeyCount': count_keys,
            'IsTruncated': response['IsTruncated']
        }

        return result

    def save_pdf(self, category: str, reportId, pdf_data):
        try:
            uuid = self.generate_uuid()
            collect_date = datetime.date.today().strftime("%Y%m%d")
            filename = f'{reportId}.pdf'
            path = f'{collect_date}/{category}/{filename}'
            # 메모리 파일로 pdf 저장
            # mem_file = io.BytesIO(pdf_data)

            # 임시 파일 생성 및 내용 쓰기
            with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                temp_file.write(pdf_data)
                temp_file_path = temp_file.name

            # AWS S3에 업로드
            self.connect()

            # self.conn 객체가 올바르게 설정되었는지 확인
            if self.conn:
                self.save(
                    from_path=temp_file_path,
                    bucket_name='futurecast-metrix-seoul',
                    to_path='naver_finance/' + path
                )

            self.close()
            print(f'AWS S3 upload sucess! reportID:{reportId}')
        except Exception as e:
            print('AWS S3 upload False!\n ERROR:', e)
            breakpoint()

    def generate_uuid(self):
        # UUID 생성
        unique_id = uuid.uuid4()  # 무작위 UUID 생성
        uuid_str = str(unique_id)
        return uuid_str

    def exists(self, bucket_name, to_path):
        response = self.conn.list_objects_v2(
            Bucket=bucket_name,
            Prefix=to_path
        )
        return response['KeyCount']

    def close(self):
        self.conn.close()
