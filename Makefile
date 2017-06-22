CWD=$(shell pwd)
ENV=$(CWD)/env
CARGO:=$(shell cargo --version)

# Ignore errors
.IGNORE: do_rust

default: python rust

python: $(shell $(ENV)/bin/detect_route_leaks || echo do_python)

do_python:
	virtualenv --python=python2 --system-site-packages $(ENV)
	$(ENV)/bin/pip install -r requirements.txt
	$(ENV)/bin/pip install -r src/related_work_implem/requirements.txt
	$(ENV)/bin/pip install pytest pytest-capturelog mock
	$(ENV)/bin/python setup.py install


rust: $(shell python -c 'import deroleru' || echo do_rust)

do_rust:
ifdef CARGO
	cd src/deroleru/python ; \
	$(ENV)/bin/pip install -r requirements.txt ; \
	$(ENV)/bin/python setup.py install
else
	$(warning "cargo is not available. The faster implementation won't be used !")
endif

clean: py_clean rust_package_clean

py_clean:
	rm -rf build
	rm -rf $(ENV)

rust_package_clean:
	rm -rf $(env)/lib/python2.7/site-packages/deroleru-0.1.0-py2.7-linux-x86_64.egg/
	cd src/deroleru/python
	rm -rf src/deroleru.egg-info
	rm -rf src/dist
	rm -rf src/build
	rm -rf src/target

re: clean default

test:
	$(ENV)/bin/python setup.py install
	$(ENV)/bin/python -m pytest src/route_leaks_detection
