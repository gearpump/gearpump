## gearpump-docker-for-integration-test

Need Linux Kernel 3.10 (or higher) and Docker 1.7 (or higher). [Install instruction](https://github.com/gearpump/gearpump-docker)
```bash
sudo mount --bind /path/to/gearpump/dist gearpump
docker build -t gearpump/gearpump .
```