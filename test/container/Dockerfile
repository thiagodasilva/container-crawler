FROM bouncestorage/swift-aio:latest

RUN pip install nose flake8==3.5.0 && pip uninstall -y hacking

CMD ["/bin/bash", "/container-crawler/test/container/launch.sh"]
