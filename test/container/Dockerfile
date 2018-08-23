FROM bouncestorage/swift-aio:latest

COPY requirements.txt requirements-test.txt /tmp/
RUN pip install -r /tmp/requirements.txt -r /tmp/requirements-test.txt && pip uninstall -y hacking

CMD ["/bin/bash", "/container-crawler/test/container/launch.sh"]
