@echo off

REM set the directory to given user's directory
set "PROJECT_DIR=%USERPROFILE%\ozingaAutomation"
if exist "%PROJECT_DIR%" (
  echo File path already exists 
  cd /d "%PROJECT_DIR%"
 
) else (
  echo Filepath for .venv cannot be found
  echo Creating filepath
  mkdir "%PROJECT_DIR%"
  cd /d "%PROJECT_DIR%"
)

REM Define source and destination paths
set "source=O:\DATA4\Projects\223002\DESIGN\ANALYSIS\2023.05.18 SCOPE 3\Envizi_Connector\Ozinga Automation\ozingaAutomation"
set "destination=%PROJECT_DIR%"

REM Copy directory using robocopy (with options for retry, logging, and progress)
robocopy "%source%" "%destination%" /E /Z /LOG+:copy_log.txt /R:3 /W:3

REM create the virtual environment
set "VENV_DIR=%PROJECT_DIR%\.venv"
if exist "%VENV_DIR%" (
	echo .venv already exists
	REM activate virtual environment
	call .venv\Scripts\activate

	REM install dependencies
	echo Installing project dependencies...
	python -m pip install -r requirements.txt

	echo Python virtual environment setup complete.
	echo Program finished running, you may close the window
) else (
	echo Setting up Python virtual environment...
	python -m venv .venv

	REM activate virtual environment
	call .venv\Scripts\activate

	REM install dependencies
	echo Installing project dependencies...
	python -m pip install -r requirements.txt

	echo Python virtual environment setup complete.
	echo Program finished running, you may close the window
)
pause >nul