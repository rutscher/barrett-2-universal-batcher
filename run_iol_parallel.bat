@echo off
echo Barrett Universal II Parallel Batch Calculator
echo ===============================================
echo.
echo This will run the calculator with 8 parallel browser tabs
echo to process your IOL data faster.
echo.
echo Make sure:
echo 1. Your input file is named "IOL_input_updated.xlsx"
echo 2. Chrome browser is installed and up to date
echo 3. All required Python packages are installed
echo.
pause

python barrett_batch_parallel.py

echo.
echo Processing complete! Check IOL_results.xlsx for results.
echo Check batch_parallel.log for detailed logs.
pause 