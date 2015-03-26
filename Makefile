push: test lint
	git push

test:
	python librorum/testing.py

install:
	sudo pip install -r requirements.txt

lint:
	flake8 librorum --max-line-length=119

.PHONY: push test install lint
