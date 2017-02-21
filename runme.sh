#/bin/sh
mkdir -p csvfiles

. .venv/bin/activate

python3 CSV_Export.py

zip -FSr csvfiles.zip csvfiles
