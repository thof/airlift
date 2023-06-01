FROM golang:1.20 as build

WORKDIR /go/src/app

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=$(go env GOARCH) go build -ldflags="-w -s" -o /go/bin/launcher2 ./

FROM scratch

COPY --from=build /go/bin/launcher2 /app/launcher2

WORKDIR /app

CMD ["/app/launcher2"]
