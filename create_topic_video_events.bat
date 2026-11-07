@echo off
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\run.ps1" topic
if errorlevel 1 pause
