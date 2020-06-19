.PHONY: develop install clean test
SHELL = /bin/bash

default: bin/python3

bin/python3:
	virtualenv . -p python3 --no-site-packages
	bin/pip3 install --upgrade pip
	bin/pip3 install -r requirements.txt

install: develop
	bin/python3 setup.py install

develop: bin/python3
	bin/python3 setup.py develop

clean:
	# virtualenv
	rm -Rf bin include lib local
	# pip
	rm -Rf .pip-selfcheck.json src parts build
	# pytest
	rm -Rf .pytest_cache

test:
	bin/py.test -m 'not ignore' --pep8 --cov reliableredisqueue --cov-report term-missing tests -s

bench_consumer:
	bin/python3 benchmark/bench.py consumer

bench_producer:
	bin/python3 benchmark/bench.py producer