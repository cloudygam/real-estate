import streamlit as st
import pandas as pd
import requests
import xml.etree.ElementTree as ET
import urllib.parse
import os
import re
from datetime import datetime, timedelta
from fuzzywuzzy import process

#실행할 때 터미널에서 "streamlit run C:\Users\user\PycharmProjects\실거래가조회\.venv\Scripts\실거래가조회.py" 입력


# ✅ 기본 CSV 파일의 로컬 경로 (사용자가 직접 설정 가능)
LOCAL_CSV_PATH = "C:/Users/user/PycharmProjects/실거래가조회/.venv/Scripts/법정동코드_default.csv"


@st.cache_data
def load_data(uploaded_file):
    """CSV 파일을 자동으로 불러오는 함수"""

    # ✅ 1. 사용자가 파일 업로드한 경우 → 업로드된 파일 사용
    if uploaded_file is not None:
        df = pd.read_csv(uploaded_file, encoding='utf-8-sig')
        st.write("📂 업로드된 CSV 파일을 로드했습니다.")
        return df

    # ✅ 2. 로컬 컴퓨터의 지정된 경로에서 파일 자동 로드
    elif os.path.exists(LOCAL_CSV_PATH):
        df = pd.read_csv(LOCAL_CSV_PATH, encoding='utf-8-sig')
        st.write(f"📂 로컬 파일 자동 로드: {LOCAL_CSV_PATH}")
        return df

    # ✅ 3. 파일이 없으면 오류 메시지 출력
    else:
        st.error("⚠ 법정동 코드 CSV 파일을 찾을 수 없습니다. 파일을 업로드해 주세요.")
        return None

def find_best_match_juridical_code(address, df):
    """입력된 주소에서 법정동 코드 찾기"""
    #st.write(f"🔍 [법정동 코드 찾기] 입력된 주소: {address}")

    if df is None:
        st.error("⚠ 법정동 코드 데이터가 없습니다.")
        return None

    address_parts = address.split()
    matched_rows = df[df['법정동명'].apply(lambda x: any(part in x for part in address_parts))]

    #st.write(f"📌 [법정동 코드 검색] 필터링된 행 개수: {matched_rows.shape[0]}")
    #if not matched_rows.empty:
        #st.write("📋 [필터링된 법정동명 목록]:")
        #st.write(matched_rows[['법정동명', '법정코드_5자리']])

    if matched_rows.empty:
        st.error("⚠ 입력한 주소에 해당하는 법정동 코드가 없습니다.")
        return None

    matched_rows['match_count'] = matched_rows['법정동명'].apply(lambda x: sum(part in x for part in address_parts))
    best_match_row = matched_rows.sort_values(by=['match_count', '법정동명'], ascending=[False, False]).iloc[0]

    st.write(f"📍 [법정동 코드] 찾은 법정동명: {best_match_row['법정동명']} | 법정동 코드: {best_match_row['법정코드_5자리']}")
    return best_match_row[['법정동명', '법정코드_5자리']]

def extract_region_jibun(address):
    """주소에서 '읍/면/동/리' 값과 지번(숫자) 추출"""
    match = re.search(r'(\S+읍|\S+면|\S+동|\S+리)\s+(\d+)', address)
    if match:
        return match.group(1), match.group(2)
    return None, None

def get_real_estate_data(lawd_cd, deal_ymd_list, service_key, region, jibun, apt_name):
    """API를 호출하여 전체 페이지 데이터를 가져옴"""
    all_data = []

    for deal_ymd in deal_ymd_list:
        encoded_service_key = urllib.parse.quote(service_key, safe='')
        page_no = 1
        num_of_rows = 100  # 한 번에 가져올 데이터 개수 (최대 100)

        while True:
            url = f"http://apis.data.go.kr/1613000/RTMSDataSvcAptTrade/getRTMSDataSvcAptTrade?LAWD_CD={lawd_cd}&DEAL_YMD={deal_ymd}&serviceKey={encoded_service_key}&numOfRows={num_of_rows}&pageNo={page_no}"

            #st.write(f"🔍 [API 요청] 페이지 {page_no}: {url}")

            try:
                response = requests.get(url)
                response.raise_for_status()
                root = ET.fromstring(response.text)

                # ✅ totalCount 값을 확인하여 전체 페이지 수 계산
                if page_no == 1:
                    total_count = int(root.findtext(".//totalCount", "0"))
                    #st.write(f"📊 [총 데이터 개수]: {total_count}개")
                    if total_count == 0:
                        break  # 데이터가 없으면 바로 종료

                item_count = 0
                for i, item in enumerate(root.findall(".//item")):
                    try:
                        item_apt_name = item.findtext("aptNm", "").strip()
                        item_umd = item.findtext("umdNm", "").strip()
                        item_jibun = item.findtext("jibun", "").strip()

                        match_type = None

                        # ✅ 1순위: 읍/면/동/리 + 지번 매칭
                        if region and jibun and region in item_umd and jibun in item_jibun:
                            match_type = "읍/면/동/리 + 지번 매칭"
                        # ✅ 2순위: 아파트명 매칭
                        elif apt_name and apt_name in item_apt_name:
                            match_type = "아파트명 매칭"

                        if match_type:
                            all_data.append({
                                "매칭유형": match_type,
                                "아파트명": item_apt_name,
                                "연도": item.findtext("dealYear", "").replace(",", ""),
                                "월": int(item.findtext("dealMonth", "")),
                                "면적": float(item.findtext("excluUseAr", "")),
                                "층": int(item.findtext("floor", "")),
                                "거래금액(만원)": int(item.findtext("dealAmount", "").replace(",", ""))
                            })
                        item_count += 1
                    except Exception as row_error:
                        st.write(f"⚠ [데이터 추출 오류] 행 {i+1}: {row_error}")

                if item_count < num_of_rows:
                    break  # 현재 페이지 데이터 개수가 num_of_rows보다 작으면 마지막 페이지임
                page_no += 1  # 다음 페이지 요청

            except requests.exceptions.RequestException as e:
                st.error(f"⚠ API 요청 중 오류 발생: {e}")
                break  # 에러 발생 시 루프 중단

    return pd.DataFrame(all_data) if all_data else None

st.title("법정동 코드 검색 및 아파트 실거래가 조회 프로그램")

uploaded_file = st.file_uploader("법정동 코드 CSV 파일을 업로드하세요 (선택 사항)", type=["csv"])
df = load_data(uploaded_file)

address = st.text_input("주소를 입력하세요")
# 현재 연도와 월을 가져옴
current_year = datetime.today().year  # 2025
current_month = datetime.today().month  # 3

# 최근 5년간의 데이터를 가져오도록 리스트 생성
deal_ymd_list = [
    f"{y}{m:02d}"
    for y in range(current_year - 5, current_year + 1)  # 최근 5년 (2020 ~ 2025)
    for m in range(1, 13)  # 1월~12월 반복
    if not (y == current_year and m > current_month)  # 미래 데이터(2025년 4월 이후)는 제외
]

# ✅ Streamlit Cloud Secrets에서 API 키 가져오기
if "service_key" in st.secrets:
    service_key = st.secrets["service_key"]
    st.write("✅ API 키가 정상적으로 로드되었습니다!")
else:
    st.error("⚠ API 키를 불러올 수 없습니다. Streamlit Secrets 설정을 확인하세요!")
    st.stop()  # 실행 중단

if address and df is not None:
    region, jibun = extract_region_jibun(address)

    # ✅ 법정동 코드 찾기 (수정)
    result = find_best_match_juridical_code(address, df)

    if result is None:
        st.error("⚠ 법정동 코드를 찾을 수 없어 실거래가 데이터를 조회할 수 없습니다.")
        st.stop()

    lawd_cd = result['법정코드_5자리']

    real_estate_data = get_real_estate_data(lawd_cd, deal_ymd_list, service_key, region, jibun, None)

    if real_estate_data is not None and not real_estate_data.empty:
        st.write("🏢 아파트 실거래가 데이터 (면적별 정리):")
        grouped_data = real_estate_data.groupby("면적")
        st.write(f"🔹 면적별 테이블 개수: {len(grouped_data)}")
        for area, group in grouped_data:
            st.write(f"🏠 **면적: {area} m²**")
            st.dataframe(group.sort_values(by=['연도', '월'], ascending=[False, False]))
    else:
        st.write("⚠ 해당 지역의 실거래가 데이터를 찾을 수 없습니다.")