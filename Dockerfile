FROM golang AS builder

ADD . /app/
WORKDIR /app

RUN go mod tidy
RUN go build main.go

FROM ubuntu as runtime
WORKDIR /app
COPY --from=builder /app/main .

CMD ["./main"]
