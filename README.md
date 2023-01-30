### Abstract

Django 기반 [opendata-finance-kr](https://github.com/ghkim9213/opendata-finance-kr) 데이터베이스 관리자입니다.

- **api/clients.py**: 공공데이터 원천으로부터 데이터를 받아오는 클라이언트 클래스입니다.

- **api/models.py**: django model 클래스로, 원천으로부터 받아온 공공데이터를 목록화하고, 산출된 결과를 저장하는 데이터베이스를 규정합니다.

- **api/managers.py**: django model을 위한 커스텀 manager 클래스로, 공공데이터 업데이트 상태를 확인하고 데이터베이스를 업데이트하는 프로세스를 규정합니다.

- **api/tasks.py**: batch task를 정의합니다.

- **api/src/{model_name}_configs.py**: model별 주요지표를 정의합니다.
