FROM bouncestorage/swift-aio:latest

COPY requirements.txt requirements-test.txt /tmp/
RUN pip install -r /tmp/requirements.txt -r /tmp/requirements-test.txt && pip uninstall -y hacking

# We need the locale, as we save our status files with non-ASCII characters (if
# they exist in the account/container names)
RUN apt-get update && apt-get install -y locales && locale-gen en_US.UTF-8
ENV LANG='en_US.UTF-8' LANGUAGE='en_US:en' LC_ALL='en_US.UTF-8'

CMD ["/bin/bash", "/container-crawler/test/container/launch.sh"]
