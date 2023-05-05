# Filecoin Data Onboarding Report

This dashboard shows the rate of data onboarding to Filecoin for a specific client.

To run it locally (in Docker), clone this repository and build a docker image:

```
$ docker image build -t fil-onboarding-report .
```

Rename `.env.example` file to `.env` and update missing values.

Run a container from the freshly built Docker image:

```
$ docker container run --rm -it -p 8501:8501 --env-file=.env fil-onboarding-report
```

Alternatively, use Docker Compose:

```
$ docker compose up -d
```

Access http://localhost:8501/ in a web browser for interactive insights.
