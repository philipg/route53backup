set -x
mamba --no-color --enable-coverage
coverage html --include *.py --fail-under=95
