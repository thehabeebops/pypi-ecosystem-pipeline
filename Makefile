.PHONY: install run status test clean
export PYTHONPATH := src
install:
	pip install -r requirements.txt
run:
	python -m pypipe run --root data --seeds config/seeds.txt --max-packages 400
status:
	python -m pypipe status --root data
test:
	python -m pytest -q
clean:
	rm -rf data
