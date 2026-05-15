# processing-server

Instagram URL 기반 콘텐츠 처리 파이프라인입니다.
Private API + Worker + Redis Queue + PostgreSQL 구조로 동작합니다.

## 🧩 구성

* **API 서버**: `uvicorn app.main:app`
* **Worker**: `python -m app.worker.runner`
* **Business Hours Worker**: `python -m app.worker.business_hours_runner`
* **Queue**: Redis
* **Storage**: PostgreSQL

## ⚙️ Requirements

* Python **3.12**
* PostgreSQL
* Redis
* Docker (Redis 실행용)
* Playwright (Chromium)


## 🚀 로컬 실행 순서

### 1. 가상환경 생성

```bash
py -3.12 -m venv .venv
```



### 2. 의존성 설치

```bash
./.venv/Scripts/python.exe -m pip install --upgrade pip
./.venv/Scripts/python.exe -m pip install -r requirements.txt
```



### 3. Redis 실행

```bash
docker compose up -d
```



### 4. (최초 1회) Playwright Chromium 설치

```bash
./.venv/Scripts/python.exe -m playwright install chromium
```


### 5. API 서버 실행 (터미널 1)

```bash
./.venv/Scripts/python.exe -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```



### 7. Worker 실행 (터미널 2)

```bash
./.venv/Scripts/python.exe -m app.worker.runner
```


### 8. Business Hours Worker 실행 (터미널 3)

```bash
./.venv/Scripts/python.exe -m app.worker.business_hours_runner
```



### Swagger

```text
http://127.0.0.1:8000/docs
```



## API

> 모든 요청에는 `X-Internal-Api-Key` 헤더가 필요합니다.



## 🚀 배포

### 구성

* **Web Service**: FastAPI (uvicorn)
* **Background Worker**: Render Background worker
* **Business Hours Worker**: Render Background worker
* **Redis**: Render Key/Value
* **PostgreSQL**: Aiven DB
