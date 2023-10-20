# Kurbisio 

Kurbisio is a golang framework for developing backends. Core features are:

- declarative infrastructure (in JSON)
- auto generation of interfaces and relational database models
- row-level based access control with different roles
- full type safety (with JSON schema)
- timer events and job processing
- abstracts underlying core AWS services (such as RDS and S3)


## Unit Tests

You need a postgres database:
```
docker run --rm --name some-postgres -p 5432:5432 -e POSTGRES_PASSWORD=docker -d postgres
```

Then use standard go commands, like

```
POSTGRES="host=localhost port=5432 user=postgres dbname=postgres sslmode=disable" POSTGRES_PASSWORD="docker" go test ./... -count 1
```

The -count 1 parameter disables test result caching. If you also specify -v you will see t.Log(...) output also for the 
passing unit tests. This can be handy for test-fist development.
