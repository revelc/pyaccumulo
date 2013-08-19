
clean:
	rm -rf build dist src/*.egg-info/

.PHONY : build
build: clean
	python setup.py build

build_eggs: build
	python setup.py bdist_egg

build_packages: build_eggs
	python setup.py bdist_rpm

install: build
	python setup.py clean --all
	python setup.py install

test:
	python runtests.py src

register:
	python setup.py sdist bdist_egg upload -r pypi

check:
	pylint -i y --output-format=parseable src/`git remote -v | grep origin | head -1 | cut -d':' -f 2 | cut -d'.' -f 1`

