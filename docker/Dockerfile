FROM python:3.13-slim
WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir .
# No gh, auth mounts, host HOME, or token environment in the collector image.
ENV HOME=/tmp
ENTRYPOINT ["grvt-data"]
CMD ["depth", "--data-dir", "/data"]
