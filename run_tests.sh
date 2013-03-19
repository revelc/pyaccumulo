export PYTHONPATH="."

coverage run --source=pyaccumulo --omit="pyaccumulo/proxy/*" tests/core_tests.py && \
coverage run --append --source=pyaccumulo --omit="pyaccumulo/proxy/*" tests/iterator_tests.py && \
(coverage html --omit="pyaccumulo/proxy/*" && echo "Wrote html report to htmlcov/index.html" ; coverage report --omit="pyaccumulo/proxy/*")
