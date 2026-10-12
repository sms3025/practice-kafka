@echo off
REM 기업 제공 OHT 로그 생성기(대외비)를 Fluent Bit 이 읽는 폴더(docker\devices)에 바로 쓰게 하는 편의 스크립트.
REM 사용 전: ohtsim.pyz 를 프로젝트의 tools\ 폴더에 복사한다 (tools\ 는 .gitignore 되어 있다).
REM
REM   scripts\ohtsim.cmd --vehicles 3 --seed 1                 실시간, 3대
REM   scripts\ohtsim.cmd --vehicles 3 --seed 1 --speed 10      10배속
REM   scripts\ohtsim.cmd --vehicles 10 --duration 10m --speed 0 --start 2026-01-05T09:00:00   미리 생성
REM
setlocal
set ROOT=%~dp0..
if not exist "%ROOT%\tools\ohtsim.pyz" (
  echo [오류] %ROOT%\tools\ohtsim.pyz 가 없습니다. 생성기 파일을 tools\ 폴더에 복사하세요.
  exit /b 1
)
python "%ROOT%\tools\ohtsim.pyz" --out "%ROOT%\docker\devices" --retain-hours 2 %*
