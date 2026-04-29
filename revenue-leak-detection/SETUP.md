# 셋업 가이드 (macOS)

처음 해보시는 분 기준으로 작성했습니다. 막히면 어느 단계에서 막혔는지 Claude에게 그대로 붙여넣어 주세요.

---

## 사전 준비

### 1. Docker Desktop 설치 (필수)

1. <https://www.docker.com/products/docker-desktop/> 접속
2. 본인 맥 종류에 맞는 버전 다운로드 (메뉴바 → 사과 → 이 Mac에 관하여 → 칩이 "Apple M..." 이면 Apple Silicon, "Intel ..." 이면 Intel)
3. 다운받은 .dmg 더블클릭 → Applications 폴더로 드래그
4. 런치패드에서 Docker 실행 → 첫 실행 시 권한 요청 다 허용
5. **메뉴바 우상단에 고래 아이콘이 살아있는지 확인**. 회색이거나 사라지면 아직 시작 중.

### 2. Python 3 확인

터미널 (⌘+Space → "Terminal")에서:

```bash
python3 --version
```

`Python 3.10` 이상 나오면 OK. 안 깔려있다고 나오면:

```bash
# Homebrew로 설치 (Homebrew도 없으면 https://brew.sh 부터)
brew install python@3.11
```

---

## 본 셋업 (한 번만 하면 됨)

터미널을 열고 그대로 따라해주세요. 한 줄씩 복붙해서 실행하시면 됩니다.

### 1. 프로젝트 폴더로 이동

```bash
cd ~/Documents/Claude/Projects/SQL\ 프로젝트
```

폴더 이름에 한글+공백이 있어서 `\ ` (백슬래시+공백)으로 escape 해야 해요. 이게 귀찮으면 따옴표도 됩니다:

```bash
cd "~/Documents/Claude/Projects/SQL 프로젝트"
```

확인:

```bash
pwd
ls
```

`README.md`, `docker-compose.yml`, `sql/`, `scripts/` 등이 보이면 OK.

### 2. 환경 변수 파일 만들기

```bash
cp .env.example .env
```

이렇게 하면 `.env` 파일이 생기는데, 기본 비밀번호(`analystpw`)가 들어있어요. 로컬 개발용이라 그대로 둬도 됩니다.

### 3. MySQL 컨테이너 실행

Docker Desktop이 실행 중인 상태에서:

```bash
docker compose up -d
```

처음에는 MySQL 8.0 이미지 다운로드하느라 1~2분 걸립니다. 완료되면 다음으로 상태 확인:

```bash
docker compose ps
```

`mysql`, `phpmyadmin` 둘 다 `STATUS` 가 `Up ... (healthy)` 로 나와야 정상입니다.
healthy 안 뜨면 30초 더 기다렸다가 다시 `docker compose ps` 실행.

### 4. phpMyAdmin으로 DB 확인

브라우저에서 <http://localhost:8080> 열기.

로그인:
- 사용자: `analyst`
- 비밀번호: `analystpw`

왼쪽 사이드바에 **portfolio** DB가 있어야 하고, 클릭해서 펼치면 다음 12개 테이블이 보여야 합니다:

```
pipeline_dq_results
pipeline_run_log
raw_olist_category_translation
raw_olist_customers
raw_olist_geolocation
raw_olist_order_items
raw_olist_order_payments
raw_olist_order_reviews
raw_olist_orders
raw_olist_products
raw_olist_sellers
raw_taxi_zone_lookup
raw_yellow_taxi
```

(아직 데이터는 없고, 빈 테이블 구조만 만들어진 상태)

### 5. Python 가상환경 + 패키지 설치

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

마지막 명령 끝나면 30~50개 패키지 설치 메시지가 주르륵 뜹니다. 에러 없이 끝나면 OK.

> 이후 새 터미널 탭/창에서 작업할 때마다 **`source .venv/bin/activate`** 해주셔야 합니다.
> 활성화되면 프롬프트 앞에 `(.venv)` 가 붙어요.

### 6. NYC Taxi 데이터 다운로드

```bash
bash scripts/download_nyc_taxi.sh
bash scripts/download_taxi_zones.sh
```

3개 parquet 파일 (~450MB, 5~10분) + zone lookup CSV가 `data/raw/nyc_taxi/` 에 받아져요.

### 7. Olist 데이터 다운로드 (수동)

Kaggle은 로그인이 필요해서 자동화가 까다로워요. 수동으로:

1. <https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce> 접속 (Kaggle 계정 없으면 무료 가입)
2. 우측 상단 **Download** 버튼 → archive.zip (~45MB) 받기
3. 압축 풀기:
   ```bash
   cd ~/Downloads
   unzip archive.zip -d "~/Documents/Claude/Projects/SQL 프로젝트/data/raw/olist/"
   ```
   (또는 Finder에서 archive.zip 더블클릭하고, 안에 들어있는 9개 CSV를 `data/raw/olist/` 로 옮기기)
4. 확인:
   ```bash
   cd "~/Documents/Claude/Projects/SQL 프로젝트"
   ls data/raw/olist/
   ```
   9개 CSV 파일이 보여야 합니다 (`olist_orders_dataset.csv`, `olist_customers_dataset.csv`, ...).

### 8. 데이터를 MySQL에 적재

```bash
# .venv 활성화 상태인지 확인 — 프롬프트에 (.venv) 가 있어야 함
python scripts/load_to_mysql.py --dataset all --truncate
```

NYC Taxi 30M 행 적재가 가장 오래 걸려요 (10~20분 정도). Olist는 1~2분이면 끝.

완료 후 phpMyAdmin 가서 `portfolio.raw_yellow_taxi` 클릭 → "Browse" 탭에 데이터가 보이면 성공!

---

## 자주 막히는 지점 트러블슈팅

### "docker: command not found"
→ Docker Desktop이 실행 안 된 상태입니다. 메뉴바에 고래 아이콘 살아있는지 확인.

### `docker compose ps` 에서 mysql이 `unhealthy` 또는 계속 `starting`
→ 30초 더 기다려보고, 그래도 안 되면:
```bash
docker compose logs mysql | tail -50
```
로그 마지막 50줄 보고 에러 메시지 Claude에게 붙여넣기.

### phpMyAdmin에서 portfolio DB가 안 보임
→ 컨테이너를 처음 만들 때만 schema가 자동 적용돼요. 만약 이전에 한 번 만든 적이 있다면:
```bash
docker compose down -v   # ← -v 가 핵심: 볼륨까지 삭제
docker compose up -d
```

### `python scripts/load_to_mysql.py` 에서 "Access denied" / "Can't connect"
→ `.env` 파일의 비밀번호와 docker-compose가 띄운 MySQL의 비밀번호가 다른 경우. `.env`를 다시 `cp .env.example .env`로 리셋하거나, 아니면 `docker compose down -v && docker compose up -d`로 컨테이너를 새로 만들기.

### `pip install` 중에 mysqlclient 같은 패키지 빌드 에러
→ 우리 requirements는 `PyMySQL`만 쓰니까 해당 안 될 거예요. 그래도 발생하면 에러 메시지 그대로 보여주세요.

---

## 여기까지 끝나면

`raw_yellow_taxi` 에 30M+ 행, `raw_olist_orders` 에 99,441 행이 들어있는 상태가 됩니다. 이걸로 Day 2 클리닝 작업을 시작할 준비 완료.
