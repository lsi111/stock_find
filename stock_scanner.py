"""
한국 주식 거래량 급등 종목 스캐너 테스트
pip install finance-datareader pandas
"""

import FinanceDataReader as fdr
import pandas as pd
from datetime import datetime, timedelta

def get_all_stocks():
    """전체 상장 종목 가져오기"""
    print("📊 전체 종목 리스트 가져오는 중...")
    
    # KRX 전체 (코스피 + 코스닥)
    krx = fdr.StockListing('KRX')
    
    print(f"✅ 총 {len(krx)}개 종목 로드 완료")
    print(f"\n컬럼: {list(krx.columns)}")
    print(f"\n샘플 5개:")
    print(krx.head())
    
    return krx

def get_volume_surge_stocks(stocks_df, days=20, surge_ratio=2.0, limit=20):
    """
    거래량 급등 종목 필터링
    
    Args:
        stocks_df: 종목 리스트 DataFrame
        days: 평균 거래량 계산 기간
        surge_ratio: 급등 기준 배수 (2.0 = 평균의 2배)
        limit: 상위 N개 종목
    """
    print(f"\n🔍 거래량 급등 종목 스캔 중... (평균 {days}일 대비 {surge_ratio}배 이상)")
    
    results = []
    total = len(stocks_df)
    
    # 종목코드 컬럼 확인 (Code 또는 Symbol)
    code_col = 'Code' if 'Code' in stocks_df.columns else 'Symbol'
    name_col = 'Name' if 'Name' in stocks_df.columns else '종목명'
    
    for idx, row in stocks_df.iterrows():
        code = row[code_col]
        name = row.get(name_col, code)
        
        # 진행률 표시 (100개마다)
        if idx % 100 == 0:
            print(f"  진행: {idx}/{total} ({idx/total*100:.1f}%)")
        
        try:
            # 최근 데이터 가져오기
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days + 10)
            
            df = fdr.DataReader(code, start_date.strftime('%Y-%m-%d'))
            
            if df is None or len(df) < days:
                continue
            
            # 최근 거래량과 평균 거래량 계산
            recent_volume = df['Volume'].iloc[-1]
            avg_volume = df['Volume'].iloc[-(days+1):-1].mean()
            
            if avg_volume == 0:
                continue
            
            volume_ratio = recent_volume / avg_volume
            
            if volume_ratio >= surge_ratio:
                # 가격 변동률 계산
                price_change = (df['Close'].iloc[-1] - df['Close'].iloc[-2]) / df['Close'].iloc[-2] * 100
                
                results.append({
                    '종목코드': code,
                    '종목명': name,
                    '현재가': df['Close'].iloc[-1],
                    '거래량': recent_volume,
                    '평균거래량': int(avg_volume),
                    '거래량비율': round(volume_ratio, 2),
                    '등락률': round(price_change, 2)
                })
                
        except Exception as e:
            continue
    
    # 거래량 비율로 정렬
    result_df = pd.DataFrame(results)
    if len(result_df) > 0:
        result_df = result_df.sort_values('거래량비율', ascending=False).head(limit)
    
    return result_df

def main():
    print("=" * 50)
    print("📈 한국 주식 거래량 급등 스캐너")
    print("=" * 50)
    
    # 1. 전체 종목 가져오기
    stocks = get_all_stocks()
    
    # 2. 시가총액 상위 500개만 필터 (속도를 위해)
    # 실제 운영시에는 전체 대상으로 하거나 조건 조정
    if 'Marcap' in stocks.columns:
        stocks = stocks.nlargest(500, 'Marcap')
        print(f"\n⚡ 시가총액 상위 500개 종목으로 필터링")
    elif 'Market' in stocks.columns:
        # 코스피만 필터
        stocks = stocks[stocks['Market'] == 'KOSPI'].head(500)
        print(f"\n⚡ 코스피 500개 종목으로 필터링")
    
    # 3. 거래량 급등 종목 찾기
    surge_stocks = get_volume_surge_stocks(
        stocks,
        days=20,        # 20일 평균
        surge_ratio=1.5, # 1.5배 이상
        limit=20        # 상위 20개
    )
    
    # 4. 결과 출력
    print("\n" + "=" * 50)
    print("🚀 거래량 급등 종목 TOP 20")
    print("=" * 50)
    
    if len(surge_stocks) > 0:
        print(surge_stocks.to_string(index=False))
    else:
        print("조건에 맞는 종목이 없습니다.")
    
    return surge_stocks

if __name__ == "__main__":
    result = main()