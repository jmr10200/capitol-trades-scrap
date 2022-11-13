import traceback
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime

import appException
import loggerConfig as loggerConfig
import os
import math


def stock_page_data(page, page_size, politicianId):
    url = 'https://bff.capitoltrades.com/trades?page={page}&pageSize={page_size}&politician={politicianId}' \
        .format(page=page, page_size=page_size, politicianId=politicianId)
    res = requests.get(url)
    json_data = res.json()

    # FIXME 데이터 확인 추출
    # df['stats.dateLastTraded']
    df = pd.json_normalize(json_data['data'])
    # FIXME 불필요한 데이터 삭제 (fullName) 확인 필요
    df.drop(
        ['_txId', '_assetId', '_issuerId', 'filingDate', 'txTypeExtended', 'hasCapitalGains', 'owner', 'chamber',
         'size', 'sizeRangeHigh', 'sizeRangeLow', 'filingId', 'filingURL', 'comment', 'committees', 'labels',
         'asset.assetType', 'asset.assetTicker', 'asset.instrument', 'issuer._stateId', 'issuer.c2iq',
         'issuer.country', 'issuer.sector', 'politician._stateId', 'politician.chamber', 'politician.dob',
         'politician.firstName', 'politician.gender', 'politician.lastName', 'politician.nickname',
         'politician.party'], axis=1, inplace=True)
    return df


# 페이지 마다 데이터 추출
def politician_stock_data(politicianId, page_size):
    try:
        # 페이지 수 산출
        page_url = 'https://bff.capitoltrades.com/trades/digest?' \
                  'politician={politicianId}&metric=countTrades&metric=countFilings&' \
                  'metric=volume&metric=countPoliticians&metric=countIssuers'.format(politicianId=politicianId)
        res = requests.get(page_url)
        json_data = res.json()
        # 총 거래수
        count_Trades = int(json_data['data']['countTrades'])
        # 총 페이지 수 산출
        if json_data['data']['countTrades'] > page_size:
            last_page = math.ceil(count_Trades/page_size)
        else:
            last_page = 1  # 50건씩 페이징, 50건 이하면 1페이지만 존재

        df = None
        pg = 1
        while pg <= last_page:
            stock_page_df = stock_page_data(pg, page_size, politicianId)
            if df is None:
                df = stock_page_df
            else:
                df = pd.concat([df, stock_page_df])
            pg += 1

        logger.info('[' + politicianId + '] stock transaction data total count = ' + str(count_Trades))
        return df

    except Exception:
        msg_type = '[crawling page data failed] '
        msg = '페이지 데이터 취득에 실패하였습니다.'
        raise appException(msg_type, msg)
    return None


def crawling_stock_data(df, politician_df, page_size):
    try:
        # ID
        politicianId = politician_df['uuid']

        for pId in politicianId:
            # politicianId 로 루프, 전체 페이지 취득
            page_df = politician_stock_data(pId, page_size)
            if df is None:
                df = page_df
            else:
                df = pd.concat([df, page_df])

        # FIXME 컬럼명 변경 : 왜 이렇게 지어놨나 ( DB 명칭, 화면표시 명칭 따로 구분이 더 좋지않나)
        df = df.rename(columns={
            '_politicianId': 'uuid',
            'issuer.issuerName': 'name',
            'issuer.issuerTicker': 'code',
            'pubDate': 'publishedAt',
            'txDate': 'trade',
            'reportingGap': 'fieldAfter',
            'txType': 'tradeType',
            'value': 'volume',
            'price': 'price'
        })

        return df

    except Exception:
        msg_type = '[crawling stock data by politician failed] '
        msg = '주식 거래 데이터 취득에 실패하였습니다.'
        raise appException(msg_type, msg)
    return None


# 페이지 마다 데이터 추출
def page_data(page, page_size):
    try:
        url = 'https://bff.capitoltrades.com/politicians?page={page}&pageSize={page_size}&' \
               'metric=dateLastTraded&metric=countTrades&metric=countIssuers&metric=volume'.format(page=page, page_size=page_size)
        res = requests.get(url)
        json_data = res.json()

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
        pg = 1
        while pg <= last_page:
            page_df = page_data(pg, page_size)
            if df is None:
                df = page_df
            else:
                df = pd.concat([df, page_df])
            pg += 1

        # FIXME 불필요한 행 삭제 (fullName) 확인 필요
        df.drop(['partyOther', 'district', 'nickname', 'middleName', 'fullName', 'dob', 'gender', 'socialFacebook',
                 'socialTwitter', 'socialYoutube', 'website', 'chamber', 'committees'], axis=1, inplace=True)

        # FIXME 컬럼명 변경 : 왜 이렇게 지어놨나 ( DB 명칭, 화면표시 명칭 따로 구분이 더 좋지않나)
        df = df.rename(columns={
            '_politicianId': 'uuid',  # json 데이터가 제공하는 _politicianId 를 그대로 사용
            '_stateId': 'stateId',  # 참고 : state ID 표기 존재함. 해당 사이트에서는 id 이용해서 관리, 화면표시할때 이름표기
            # FIXME 화면표시 : [ party (정당) / state (지역) ] 인데 이걸 다 party 라는 항목에 넣고 싶은가?
            # 'party': 'party',
            # FIXME 화면표시 : [ firstName + lastName ] 화면표시 그대로 저장하고 싶은가?
            # firstName, lastName, middleName, fullName 모두 제공됨
            'stats.dateLastTraded': 'lastTrade',
            'stats.countTrades': 'trades',
            'stats.countIssuers': 'issuers',
            'stats.volume': 'volume'
        })
        # FIXME 업데이트 날짜 추가 createdAt (수집일) , updatedAt (갱신일) : DB 등록시 or 갱신시 적용
        return df
    except Exception:
        msg_type = '[crawling stock data failed] '
        msg = '데이터 추출에 실패하였습니다.'
        raise appException(msg_type, msg)


def print_politician_csv(df):
    try:
        # FIXME 시간 : 임의로 UTC 출력 , 형식은 YYYY-MM-ddTHH:mm:ssZ (ISO format)
        df['createdAt'] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        df['updatedAt'] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        #  END

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


def print_stock_csv(df):
    try:
        df['tUniqueId'] = df['uuid'] + df['publishedAt']

        # 파일명 : capitol-trades_yyyy-mm-dd.csv
        directory = '../tmp/capitol-trades-csv/stock'
        os.makedirs(directory, exist_ok=True)
        # 현재시간 (UTC X)
        filename = 'capitol-trades_stock_' + datetime.now().strftime('%Y-%m-%d %Hh%Mm%Ss')
        # CSV 파일 출력
        df.to_csv(directory + '/{filename}.csv'.format(filename=filename), index=False)
    except Exception:
        msg_type = '[csv file print failed] '
        msg = 'csv 파일 출력에 실패하였습니다.'
        raise appException(msg_type, msg)


def execute():
    # 실행
    logger.info('[start] stock crawling execute')
    try:
        # TODO validation check 필요?

        url = 'https://www.capitoltrades.com/politicians'
        res = requests.get(url)

        # res.status_code
        html = res.text
        soup = BeautifulSoup(html, 'html.parser')

        # 총 페이지 수 산출 (가장 많은 페이지 보기로 확인)
        if not isinstance(soup.select_one('div.pagination'), type(None)):
            count_politician = int(soup.select_one('div.pagination').p.text.split(' ')[-2])
            page_size = int(soup.select_one('div.page-size').contents[-1].text)
            last_page = math.ceil(count_politician/page_size)
        else:
            last_page = 1

        logger.info('[start] 크롤링을 시작합니다.')
        startTime = datetime.now()  # 시작 시간
        logger.info('[..ing] crawling politician data')
        # politician data
        politician_df = None
        politician_df = crawling_politician_data(politician_df, last_page, page_size)
        logger.info('success crawling politician data = total ' + str(count_politician) + ' politician')
        endTime = datetime.now()  # 종료 시간
        result_t = endTime - startTime
        resultTime = '(hh:mm:ss.ms) {}'.format(result_t)
        logger.info('crawling stock data result time = ' + resultTime)

        logger.info('[..ing] crawling stock data')
        # stock data by politician
        stock_df = None
        stock_df = crawling_stock_data(stock_df, politician_df, page_size)
        endTime = datetime.now()  # 종료 시간
        result_t = endTime - startTime
        resultTime = '(hh:mm:ss.ms) {}'.format(result_t)
        logger.info('crawling stock data result time = ' + resultTime)

        logger.info('[end] 크롤링이 완료되었습니다.')

        # TODO DB 저장

        # CSV 파일 출력
        logger.info('[start] csv 파일 출력을 시작합니다.')
        print_politician_csv(politician_df)
        print_stock_csv(stock_df)
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
