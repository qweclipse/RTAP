@echo off
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\run.ps1" stop
if errorlevel 1 pause
