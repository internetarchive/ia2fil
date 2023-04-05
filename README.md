# Internet Archive to Filecoin

This dashboard shows progress of copying Internet Archive items to Filecoin.

To run it locally (in Docker), clone this repository and build a docker image:

```
$ docker image build -t filecoin .
```

Run a container from the freshly built Docker image using an OpenAI API key:

```
$ docker container run --rm -it -p 8501:8501 filecoin
```

Alternatively, use Docker Compose:

```
$ docker compose up -d
```

Access http://localhost:8501/ in a web browser for interactive insights.
