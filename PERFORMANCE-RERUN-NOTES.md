# 성능 평가 재실행 대기

2026-10-12 15:42 KST: 계측 CNS에서 단축 EXT4 6조합
`six-workloads-20261012_064244` 시작. warmup 60초/측정 180초/1회 반복.
과거 간섭 회차 원본 3개 및 오염된 집계 6개는 사용자 요청으로 영구 삭제했다.
유효 raw/monitor 및 진단 runner 로그는 보존했다.
100 ops/s 쓰기·읽기·토픽 생성/삭제 smoke가 stable로 완료됐으며
토픽 created=deleted=231, failed=0, 커널 오류 0이었다.
FEMU SHA-256: `41fc34c696f4f17e925199dc6036ab864ea699c9bf7fb5cd17f0acdfc3c46688`.
JAR SHA-256: `f980b3e10c52b26f5afd0cc072ca61369907f4bfbdaafcfbb8836f4c1ebe6315`.
호스트 통계: `/home/zns-team4/results/cns-instrumented-20261012_153739/stats.csv`.
계측 OFF/ON 영향과 GC 실제 발동 경로는 아직 검증 중이며 정식 비교 결과가 아니다.

## 2026-10-12 CNS + EXT4, prefill + 쓰기 전용

- 실행: `six-workloads-20261012_003755/prefill-write`
- 부하: 1,000 ops/s, 3회 반복.
- 2회차 prefill fio에 별도 접속에서 TERM/KILL이 전송되고 다른 benchmark 및
  장치 정리가 겹쳤다. 측정 시작 점유율은 54.71%였으며 ACK 없이 fail-fast됐다.
- sweep의 `saturated` 판정과 평균 651.79 ops/s는 성능 한계의 근거로 사용하지 않는다.
- 사용자 요청: 현재 나머지 실행을 유지하고 **첫 조합은 나중에 재실행**한다.
- 재실행 시 CNS FEMU의 초기 상태를 맞추고 다른 실험과 동시 실행하지 않는다.
- 대기 상태: 미실행. 유효한 3회 반복 결과를 얻은 뒤 상태를 갱신한다.

## 2026-10-12 실행 중단

- 같은 suite의 `prefill-read-write` 첫 회차 진행 중 FEMU가 종료됐다.
- 이전 boot journal은 14:03:20 KST에 다량 NVMe timeout/abort 기록으로 끝난다.
  정상 shutdown 기록은 확인되지 않았다. QEMU 종료의 직접 원인(crash/kill/OOM)은 미확정이다.
- 15:00 KST경 team4 CNS FEMU를 동일 설정으로 재시작했다.
- 두 번째 조합은 미완료, 이후 4개 조합은 아직 실행되지 않았다.
- VM은 재시작했지만 benchmark는 자동 재개하지 않았다. 중단 결과는 보존한다.
