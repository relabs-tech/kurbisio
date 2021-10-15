#!/usr/bin/env bash
set -ex
echo 'mode: count' > coverage.out
packages=$(go list ./... | grep -v /models/ ) 
for d in $packages; do
    go test -covermode=count -coverprofile=profile.out -coverpkg=$(echo $packages| tr ' ' ,) $d
    if [ -f profile.out ]; then
        tail -q -n +2  profile.out >> coverage.out
        rm profile.out
    fi
done
go tool cover -func=coverage.out 