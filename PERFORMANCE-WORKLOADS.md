# Kafka 성능 평가 워크로드

2026-10-12: `run-performance-suite.sh`는 탐색용으로 부하 1000/2000/3000,
회차 1회, warmup 60초, 측정 180초를 사용한다. 과거 3회/300초/600초 결과와
조건이 다르며 최종 안정성 검증이 아니다. 80% prefill 시간은 별도로 필요하다.
FEMU GC/NAND 계측은 `FEMU_STATS_FILE`로 활성화하며 CNS BBSSD 전용이다.
누적 카운터의 측정 구간 차이를 사용한다. WAF는 NAND program pages × page bytes
÷ host write bytes이며 이 카운터는 FTL에 들어온 요청 기준이지 Kafka ACK 기준이 아니다.
`gc_host_ns`는 GC 코드의 호스트 실행 시간이며 에뮬레이션 NAND 지연과 다르다.
NAND erase 카운터는 media timing bridge에 전달된 연산만 집계한다.
계측 OFF/ON 오버헤드는 아직 검증되지 않았다. p95는 Java 출력과 CSV에 추가했다.

기존 benchmark 4와 별도로 `run-performance-evaluation.sh`를 사용한다.
커밋 및 실장치 실행은 별도 단계다.

## fio prefill + Kafka (`prefill`, 기본값)

파일시스템을 초기화하고 실제 fio 쓰기로 80%까지 채운다. prefill 파일을 유지한
채 Kafka가 전체 2 GiB retention으로 쓰기와 삭제를 반복한다. 점유율은 Kafka
데이터 때문에 80%보다 증가할 수 있다.

## Kafka만으로 채우기 (`kafka-only`)

매 회차 새 파일시스템에서 Kafka만 실행한다. fio prefill을 호출하지 않는다.
기본 전체 topic retention은 16 GiB다. guest root에 분리한 KRaft metadata는
기존과 동일하다. 일반 파일시스템 초기화 및 Kafka 관리 메타데이터 쓰기는 발생한다.

기본 warmup 300초, 측정 600초는 **초기 채움 과정**을 측정한다. 이 시간에 80%
점유나 retention 정상상태 도달을 보장하지 않는다. 1 KiB 레코드를 2,000 ops/s로
기록해 payload 16 GiB를 쌓는 데만 약 2시간 20분이 필요하며 실제 시간은 부하,
Kafka 오버헤드, retention 삭제에 따라 달라진다. 채워진 상태를 보려면 warmup을
늘리고 결과 CSV의 `fs_used_pct_before/after`와 monitor를 확인해야 한다.
`fs_used_pct_before`는 warmup 전 값이다. 이 값만으로 측정 시작 점유율을 판정하지 않는다.
retention은 topic 로그 크기 제한이지 파일시스템 사용률의 정확한 제한이 아니다.
장치 여유 공간을 고려해 설정한다. 이 모드는 80% prefill 실험과 별도로 비교한다.

## 공통 조건 및 실행

1 KiB 레코드, producer 8개, 8 partitions, 부하별 3회 반복.
`--scenario`는 다음 세 워크로드를 선택한다. 기본값 `all`은 각각 독립적으로
부하 sweep을 실행하고 결과 디렉터리와 포화 경계를 구분한다.

| 옵션 | 동작 |
|---|---|
| `write` | producer만 실행 |
| `read-write` | producer + consumer 1개 |
| `read-write-topics` | producer + consumer + 별도 토픽 생성/삭제 |

토픽 부하는 1 partition 토픽을 생성한 후 삭제 요청의 완료를 기다린다.
목표 5주기/초지만 Admin API 지연 때문에 실제 주기율은 낮을 수 있다.
삭제 요청 완료와 파일시스템 공간 회수 완료는 다르다. 동적 토픽에 메시지는
쓰지 않으며 주 데이터 토픽은 삭제하지 않는다. KRaft metadata 디렉터리는
guest root에 있어 metadata 부하 전체가 비교 SSD로 향하는 것은 아니다.
Java 옵션 `-Dbenchmark.topicDeletion=true`를 이 시나리오에만 주입한다.
Java 원본 로그에 created/deleted/failed를 기록하며 생성 또는 삭제 실패는
`topics_failed`로 무효 처리한다. 쓰기 전용은 consumer 검증을 적용하지 않는다.
새 JAR를 빌드·배포한 뒤 실행해야 삭제 동작이 적용된다.

두 채움 방식 × 세 워크로드 = 파일시스템당 6가지 조합이다.
달성률 95% 이상, 정합성 오류 0, ACK stall 60초 미만을 안정 기준으로 사용하며
첫 포화 단계에서 상위 부하를 중단한다. filesystem 간 FEMU를 완전히 종료하고
재실행해 FTL 초기 상태를 맞춘다. 계측 바이너리와 GC/NAND 수집은 아직 별도 구현이 필요하다.

```bash
# 계획만 출력: 장치 변경 없음
./run-performance-evaluation.sh --backend cns --filesystem f2fs \
  --workload kafka-only --rates 1000,1500,2000,2500,3000 --dry-run

# Kafka 초기 채움 과정 평가
./run-performance-evaluation.sh --backend cns --filesystem f2fs \
  --workload kafka-only --rates 1000,1500,2000,2500,3000

# 긴 채움 기간을 포함한 단일 부하 평가 예시
./run-performance-evaluation.sh --backend cns --filesystem ext4 \
  --workload kafka-only --rates 2000 --warmup 10800 --measure 600
```

특정 워크로드만 실행하려면 `--scenario read-write` 등으로 지정한다.
기존 prefill + 쓰기/읽기 평가를 재현할 때도 `--scenario read-write`를 명시한다.

결과 디렉터리 이름에 `prefill` 또는 `kafka-only`가 포함되며 manifest에 fio 사용
여부와 retention을 기록한다. 기존 결과는 변경하지 않는다.
