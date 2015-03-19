test:
	python librorum/testing.py

install:
	sudo pip install -r requirements.txt

lint:
	flake8 librorum --max-line-length=119
