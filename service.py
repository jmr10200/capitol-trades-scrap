import traceback
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime

import appException
import loggerConfig as loggerConfig
import re
import os
import sys
import math

# 상세 데이터 아래의 url 에서 json ID 변경으로 데이터 취득 가능
# https://www.capitoltrades.com/_next/data/UPA0FyPj9azKpAA871BFM/en/politicians/W000804.json?id=W000804


# 페이지 마다 데이터 추출
def stock_page_data(politicianId):
    try:
        url = 'https://www.capitoltrades.com/_next/data/UPA0FyPj9azKpAA871BFM/' \
              'en/politicians/{politicianId}.json?id={politicianId}'.format(politicianId=politicianId)
        res = requests.get(url)
        json_data = res.json()

        # FIXME 데이터 확인 추출
        # TODO JSON 데이터 확인 필요함
        df = pd.json_normalize(json_data['queries'])

        return df

    except Exception:
        msg_type = '[crawling page data failed] '
        msg = '페이지 데이터 취득에 실패하였습니다.'
        raise appException(msg_type, msg)
    return None


def crawling_stock_data(df, politician_df):
    try:
        # ID
        politicianId = politician_df['uuid']
        # stock_page_data
        url = 'https://www.capitoltrades.com/_next/data/UPA0FyPj9azKpAA871BFM/' \
              'en/politicians/{politicianId}.json?id={politicianId}'.format(politicianId=politicianId)
        res = requests.get(url)
        json_data = res.json()

        for pId in politicianId:
            page_df = stock_page_data(pId)
            if df is None:
                df = page_df
            else:
                df = pd.concat([df, page_df])

        # FIXME 컬럼명 변경 : 왜 이따위로 지어놨나 확인
        # df = df.rename(columns={
        #     '_politicianId': 'uuid',
        #     'stats.volume': 'volume'
        # })

        return df

    except Exception:
        msg_type = '[crawling stock data by politician failed] '
        msg = '주식 거래 데이터 취득에 실패하였습니다.'
        raise appException(msg_type, msg)
    return None


# 페이지 마다 데이터 추출
def page_data(page, page_size):
    try:
        # url = 'https://www.capitoltrades.com/politicians?page={page}'.format(page=page
        # pageSize 주의 필요
        url = 'https://bff.capitoltrades.com/politicians?page={page}&pageSize={page_size}&' \
               'metric=dateLastTraded&metric=countTrades&metric=countIssuers&metric=volume'.format(page=page, page_size=page_size)
        res = requests.get(url)
        json_data = res.json()

        # FIXME 데이터 확인 추출
        # df['stats.dateLastTraded']
        df = pd.json_normalize(json_data['data'])

        return df

    except Exception:
        msg_type = '[crawling page data failed] '
        msg = '페이지 데이터 취득에 실패하였습니다.'
        raise appException(msg_type, msg)
    return None


# 크롤링 실행
def crawling_politician_data(df, last_page, page_size):
    try:
        # FIXME +1 부터 추출 되도록 리팩토링
        pg = 1
        while pg <= last_page:
            logger.info(str(pg) + '페이지 데이터를 추출합니다.')
            page_df = page_data(pg, page_size)
            if df is None:
                df = page_df
            else:
                df = pd.concat([df, page_df])
            pg += 1

        # FIXME 컬럼명 변경 : 왜 이따위로 지어놨나 확인
        df = df.rename(columns={
            '_politicianId': 'uuid',
            '_stateId': 'stateId',
            # 'party': 'party',
            # 'firstName': 'firstName',
            # 'lastName': 'firstName',
            'stats.dateLastTraded': 'lastTrade',
            'stats.countTrades': 'trades',
            'stats.countIssuers': 'issuers',
            'stats.volume': 'volume'
        })
        # FIXME 업데이트 날짜 추가 createdAt (수집일) , updatedAt (갱신일)
        # FIXME 수집일, 변경일 : DB 등록시 or 갱신시 적용
        return df
    except Exception:
        msg_type = '[crawling stock data failed] '
        msg = '데이터 추출에 실패하였습니다.'
        raise appException(msg_type, msg)
    # return None


# politician 크롤링
# def politician():


def print_csv(df):
    try:
        # FIXME 불 필요한 행 삭제 (fullName) 확인 필요
        df.drop(['partyOther', 'district', 'nickname', 'middleName', 'fullName', 'dob', 'gender', 'socialFacebook',
                 'socialTwitter', 'socialYoutube', 'website', 'chamber', 'committees'], axis=1, inplace=True)

        # FIXME 저장할때 수집일, 갱신일
        # UTC 출력
        df['createdAt'] = datetime.utcnow()
        df['updatedAt'] = datetime.utcnow()
        # FIXME END

        # 파일명 : capitol-trades_yyyy-mm-dd.csv
        directory = '../tmp/capitol-trades-csv'
        os.makedirs(directory, exist_ok=True)
        # 현재시간 (UTC X)
        filename = 'capitol-trades_' + datetime.now().strftime('%Y-%m-%d %Hh%Mm%Ss')
        # CSV 파일 출력
        df.to_csv(directory + '/{filename}.csv'.format(filename=filename), index=False)
        logger.info('{filename}.csv'.format(filename=filename))
    except Exception:
        msg_type = '[csv file print failed] '
        msg = 'csv 파일 출력에 실패하였습니다.'
        raise appException(msg_type, msg)


def execute():
    # 実行
    logger.info('[start] stock crawling execute')
    try:
        # TODO validation check 필요?
        page = 1

        url = 'https://www.capitoltrades.com/politicians?page={page}'.format(page=page)

        res = requests.get(url)

        # res.status_code
        html = res.text
        soup = BeautifulSoup(html, 'html.parser')

        # 총 페이지 수 산출
        if not isinstance(soup.select_one('div.pagination'), type(None)):
            view_count = int(soup.select_one('div.pagination').p.text.split(' ')[-4])
            page_size = int(soup.select_one('div.pagination').p.text.split(' ')[-2])
            last_page = math.ceil(page_size/view_count)
        else:
            last_page = 1

        logger.info('[start] 크롤링을 시작합니다.')

        logger.info('[..ing] crawling politician data')
        # politician data
        politician_df = None
        politician_df = crawling_politician_data(politician_df, last_page, page_size)
        logger.info('success crawling politician data')

        logger.info('[..ing] crawling stock data')
        # stock data by politician
        stock_df = None
        stock_df = crawling_stock_data(stock_df, politician_df)

        logger.info('[end] 크롤링이 완료되었습니다.')

        # TODO DB 저장
        # TODO 확인할 내용
        # 간단 아니잖아 시발;
        # 1. UUID : json 데이터 보면 _politicianId 를 얻을 수 있다. 그대로 사용
        # 2. DB 항목명 : stateId 으로 데이터 제공됨 -> state name 로 표기하고 싶은가?
        # 3. party (정당) / state (지역) 인데 이걸 다 party 라는 항목에 넣고 싶은가?
        # 4. firstName, lastName, middleName, fullName 모두 주어진다.
        #    화면상에는 firstName + lastName 으로 표시된다. 화면 그대로 저장하고 싶은가?
        # 5. 수집일, 갱신일은 DB등록 or csv 출력시 하면됨

        # CSV 파일 출력
        logger.info('[start] csv 파일 출력을 시작합니다.')
        print_csv(politician_df)
        logger.info('[end] csv 파일 출력이 완료되었습니다.')

        logger.info('[end] stock crawling finished')
    except appException as se:
        logger.error(se)
    except Exception as e:
        # 예상하지 못한 예외는 trace 로그 출력
        traceback.print_exc()
        logger.error(e)


if __name__ == '__main__':
    logger = loggerConfig.logger
    execute()
