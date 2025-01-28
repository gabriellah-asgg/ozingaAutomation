@echo off
set "PROJECT_DIR=%USERPROFILE%\ozingaAutomation"
if exist "%PROJECT_DIR%" (
	cd /d "%PROJECT_DIR%"

	REM activate virtual environment
	call .venv\Scripts\activate

	REM run the python script
	echo Running your python script
	python ozingaAutomation.py
	) else (
	echo Could not find .venv filepath, please try running setup_venv filepath
	)
echo Program finished running, you may close the window
pause >nul