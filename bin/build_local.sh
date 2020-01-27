#!/bin/ksh

if [ -d test_env ]; then
    echo "Removing dir test_env"
    rm -fr test_env
fi

python -m venv test_env
chmod +x test_env/bin/activate
source ./test_env/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
