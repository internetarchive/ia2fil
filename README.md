# Internet Archive Data to Filecoin

This dashboard shows progress of replicating Internet Archive items to Filecoin.

To run it locally (in Docker), clone this repository and build a docker image:

```
$ docker image build -t ia2fil .
```

Rename `.env.example` file to `.env` and update missing values.

Run a container from the freshly built Docker image:

```
$ docker container run --rm -it -p 8501:8501 --env-file=.env ia2fil
```

Alternatively, use Docker Compose:

```
$ docker compose up -d
```

Access http://localhost:8501/ in a web browser for interactive insights.
