# Quant Stock System

국내 주식 종목 데이터를 수집하고, 저평가 우량주 성격에 가까운 종목을 가격/거래량 기반 룰로 점수화한 뒤, 상위 종목을 이메일로 리포트하는 자동화 프로젝트입니다.  
데이터 수집부터 피처 생성, 스코어링, 종목 선정, 이메일 발송까지 하나의 파이프라인으로 구성되어 있으며 Airflow DAG로 실행할 수 있습니다.

## 프로젝트 개요

이 프로젝트는 `pykrx`를 사용해 한국 주식 시장의 OHLCV 데이터를 수집하고, 장기 추세를 유지하는 유동성 상위 종목 중 최근 조정이 들어온 후보를 골라 저평가 우량주 성격에 가깝게 선별합니다.

핵심 흐름은 아래와 같습니다.

1. 종목 리스트 기준으로 최근 약 220일 가격 데이터를 수집합니다.
2. 20일/60일/120일 이동평균, 거래대금, 중기 수익률, 변동성, 장기 고점 대비 조정 폭을 계산합니다.
3. 장기 추세 유지, 최근 눌림 구간, 높은 유동성, 과열되지 않은 거래량을 반영해 종목별 스코어를 만듭니다.
4. 최신 거래일 기준 상위 5개 종목을 선정합니다.
5. 결과를 이메일로 발송합니다.

## 실행 환경

- Python 3.10+
- macOS / Linux 권장
- Gmail 앱 비밀번호 사용 가능 계정

## Airflow 실행

Airflow로 전체 파이프라인을 실행하려면:

```bash
chmod +x run_airflow.sh
./run_airflow.sh
```

실행 후 Airflow에서 `quant_stock_pipeline` DAG를 트리거하면 아래 순서로 작업이 수행됩니다.

```text
fetch_price_data
-> build_features
-> score_stocks
-> select_top_stocks
-> send_email_report
```

## 사용 라이브러리

- `pandas`
- `pykrx`
- `python-dotenv`
- `apache-airflow`
- `pendulum`
