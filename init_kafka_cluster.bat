@echo off
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\run.ps1" init
if errorlevel 1 pause
