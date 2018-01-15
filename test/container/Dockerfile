FROM bouncestorage/swift-aio:latest

RUN pip install nose flake8==3.5.0 && pip uninstall -y hacking

RUN locale-gen en_US.UTF-8
ENV LANG='en_US.UTF-8' LANGUAGE='en_US:en' LC_ALL='en_US.UTF-8'
CMD ["/bin/bash", "/container-crawler/test/container/launch.sh"]
