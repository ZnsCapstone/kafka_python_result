# Kafka Python 벤치마크 파일 구성

기존 `bench_final.py`에 모여 있던 설정, 시스템 준비, 성능 측정, 로깅, 결과 저장 코드를 역할별 파일로 분리했다. 실행 방법은 기존과 동일하다.

```bash
python3 bench_final.py <0=fixed|1=dynamic> [rounds]
```

예시:

```bash
python3 bench_final.py 0 3  # fixed 구현, 3회 반복
python3 bench_final.py 1 3  # dynamic 구현, 3회 반복
```

## 파일별 역할

### `bench_final.py`

벤치마크 프로그램의 진입점이다.

- `bench_runner.run()`을 호출한다.
- 기존 실행 명령과의 호환성을 유지한다.
- 실제 설정이나 측정 로직은 이 파일에 작성하지 않는다.

### `bench_runner.py`

전체 실험의 실행 순서를 관리한다.

- 명령행 인자에서 fixed/dynamic 구현과 반복 횟수를 읽는다.
- 결과를 저장할 자료 구조를 초기화한다.
- record size, round, filesystem, scenario 순서로 실험을 반복한다.
- 각 시나리오마다 Kafka 종료, 파일시스템 준비, Kafka 시작, 토픽 생성, 성능 측정을 순서대로 호출한다.
- 측정이 끝날 때마다 JSON, CSV, 요약 보고서를 저장한다.
- 정상 종료와 예외 발생 시 Kafka와 device-mapper를 정리한다.
- 각 round의 표준 출력을 `TeeLogger`를 통해 파일에도 기록한다.

실험의 반복 순서나 시나리오 실행 순서를 변경하려면 이 파일을 수정한다.

### `settings.py`

벤치마크에서 사용하는 모든 설정값과 실행 경로를 관리한다.

주요 설정:

- Kafka 설치 경로와 KRaft 설정 파일
- raw ZNS 장치, device-mapper 이름과 커널 모듈 경로
- 마운트 경로와 metadata 저장 경로
- filesystem 목록
- record size별 고정 OP/s
- producer 수와 consumer/dynamic topic 사용 여부
- warmup 및 실제 측정 시간
- bottleneck 판정 기준
- 결과, 로그, 모니터링 파일의 저장 경로
- 측정 종료 후 파일 압축 여부

dm-zns 커널 모듈의 기본 경로는 다음과 같다.

```text
~/dm-zns-base/src/dm-zns-base.ko
```

다른 위치의 모듈을 사용해야 하면 `DM_ZNS_MODULE_PATH` 환경변수로 기본 경로를 덮어쓸 수 있다.

현재 주요 시간 설정은 다음과 같다.

```python
WARMUP_SECONDS = 20
MEASURE_DURATION = 60
```

실험 조건이나 경로를 변경할 때는 우선 이 파일을 수정한다. `configure_dm_implementation()`은 명령행의 `0` 또는 `1`을 각각 fixed와 dynamic 구현으로 변환한다.

### `bench_logging.py`

터미널 출력과 round 로그 파일 출력을 동시에 처리한다.

- `TeeLogger.write()`가 같은 메시지를 터미널과 파일 양쪽에 기록한다.
- `flush()`로 양쪽 출력 버퍼를 비운다.
- `close()`로 로그 파일을 닫는다.

로그 출력 방식이나 포맷을 변경하려면 이 파일을 수정한다.

### `bench_utils.py`

여러 모듈에서 공통으로 사용하는 작은 유틸리티를 관리한다.

- shell 명령의 간단 실행, 전체 결과 수집, 실시간 출력
- Kafka 포트가 열리거나 닫힐 때까지 대기
- 안전한 숫자 변환
- 평균, 표준편차, 변동계수 계산
- 텍스트 파일 저장
- raw 출력과 모니터링 파일의 gzip 압축

특정 실험 단계에만 필요한 로직은 이 파일에 넣지 않고 해당 모듈에 둔다.

### `system_setup.py`

성능 측정 전에 필요한 시스템, 저장장치, 파일시스템과 Kafka 환경을 준비한다.

주요 역할:

- OS, CPU, 메모리, 디스크, Java, Kafka 등의 환경 정보 수집
- 기존 Kafka 프로세스 종료
- 마운트 해제와 기존 device-mapper 제거
- FEMU ZNS namespace reset
- dm-zns 커널 모듈 로드와 device-mapper target 생성
- ext4 또는 f2fs 포맷 및 마운트
- Kafka KRaft 실험용 설정 파일 생성
- Kafka broker 시작 및 종료
- 메인 벤치마크 토픽 삭제 및 재생성

장치 초기화, 파일시스템 mount option, Kafka broker option을 변경하려면 이 파일을 수정한다.

> 주의: 이 파일의 함수들은 ZNS reset, 파일시스템 포맷, 마운트 작업을 수행한다. 장치 경로를 변경할 때는 `settings.py`의 `RAW_ZNS_DEVICE`와 `FS_DEVICE`가 올바른지 반드시 확인해야 한다.

### `performance.py`

실제 성능 측정과 측정 결과 파싱을 담당한다.

주요 역할:

- `iostat`과 `vmstat` 모니터 프로세스 시작 및 종료
- Java Kafka benchmark 명령 생성 및 실행
- Java 출력에서 request 수, 처리량, app-level latency와 오류 수 파싱
- iostat에서 disk utilization, await, read/write 처리량 파싱
- vmstat에서 CPU 사용률과 iowait 파싱
- app, disk, CPU 지표 병합
- 설정된 기준에 따른 I/O bottleneck 판정
- Java raw output과 모니터링 파일 저장 및 압축

새로운 성능 지표를 추가하거나 파싱 규칙, bottleneck 판단 방법을 변경하려면 이 파일을 수정한다.

### `reporting.py`

측정 결과를 사람이 읽거나 후처리할 수 있는 형식으로 저장한다.

- 중첩된 실험 결과를 CSV 행으로 변환한다.
- 모든 round의 상세 결과를 `full_results.csv`에 저장한다.
- filesystem, record size, scenario 기준 통계를 요약 CSV로 저장한다.
- 진행 중인 전체 결과를 JSON snapshot으로 저장한다.
- app-level latency와 block-level latency를 포함한 텍스트 보고서를 만든다.

CSV 컬럼, 집계 통계 또는 텍스트 보고서 형식을 변경하려면 이 파일을 수정한다.

## 전체 실행 흐름

```text
bench_final.py
    └── bench_runner.run()
          ├── settings.py에서 설정과 결과 경로 로드
          ├── system_setup.py로 Kafka/ZNS/filesystem 준비
          ├── performance.py로 benchmark 및 시스템 지표 측정
          ├── reporting.py로 JSON/CSV/텍스트 보고서 저장
          └── bench_logging.py로 round별 실행 로그 기록
```

공통 명령 실행, 통계 계산, 파일 저장은 각 모듈에서 `bench_utils.py`를 사용한다.

## 수정 목적별 파일 선택

| 변경하려는 내용 | 수정할 파일 |
|---|---|
| record size, OP/s, warmup, 측정 시간 | `settings.py` |
| 시나리오 또는 filesystem 반복 순서 | `bench_runner.py` |
| Kafka, ZNS, dm-zns, mount 설정 | `system_setup.py` |
| Java 명령이나 성능 지표 파싱 | `performance.py` |
| bottleneck 판정 기준값 | `settings.py` |
| bottleneck 판정 로직 | `performance.py` |
| CSV 컬럼과 요약 통계 | `reporting.py` |
| 터미널 및 파일 로그 처리 | `bench_logging.py` |
| 공통 shell, 통계, 압축 기능 | `bench_utils.py` |
