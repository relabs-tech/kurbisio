# Kurbisio 

Kurbisio is a golang framework for developing backends. Core features are:

- declarative infrastructure (in JSON)
- auto generation of interfaces and relational database models
- row-level based access control with different roles
- full type safety (with JSON schema)
- timer events and job processing
- abstracts underlying core AWS services (such as RDS and S3)


## History

Kurbisio was initially developed 2021 by Dalarub & Ettrich GmbH, a consultancy company founded
by Daniel Alarcon-Rubio and Matthias Ettrich. It was used for internal projects, including a
running and fitness app which later became twaiv.

Contributors:
Daniel Alarcon-Rubio
Matthias Ettrich
Gregory Lenoble
Dima Shelamov

## Unit Tests

The tests use PostgreSQL, MinIO (S3), and ElasticMQ (SQS):
```
docker run --rm --name some-postgres -p 5432:5432 -e POSTGRES_PASSWORD=docker -d postgres
docker compose -f docker/docker-compose.local-aws.yml up -d
```

MinIO serves S3 on `http://localhost:9000` and its console on `http://localhost:9001`.
The local credentials are `testuser` / `testpassword`. ElasticMQ serves SQS on
`http://localhost:9324` and its console on `http://localhost:9325`.
Wait for both services to be ready before running tests:

```sh
curl -f http://localhost:9000/minio/health/live
curl -f 'http://localhost:9324/?Action=ListQueues&Version=2012-11-05'
```

The S3 tests create their bucket automatically. For local KSS configuration, use
`EndpointURL: "http://localhost:9000"`, `SQSEndpointURL: "http://localhost:9324"`,
`UsePathStyle: true`, and `AutoCreateBucket: true`, with the credentials above.
If using `SQSNotificationQueue`, also set `SkipBucketNotifications: true`:
MinIO cannot publish directly to ElasticMQ. The queue integration test publishes
an S3 event explicitly to verify notification consumption; automatic upload
notifications require a separate bridge. On AWS, leave the endpoint overrides
and `SkipBucketNotifications` unset to retain normal S3-to-SQS configuration.

Then use standard go commands, like

```
POSTGRES="host=localhost port=5432 user=postgres dbname=postgres sslmode=disable" POSTGRES_PASSWORD="docker" go test ./... -count 1
```

Stop the local S3/SQS services with
`docker compose -f docker/docker-compose.local-aws.yml down`.

The -count 1 parameter disables test result caching. If you also specify -v you will see t.Log(...) output also for the 
passing unit tests. This can be handy for test-fist development.
