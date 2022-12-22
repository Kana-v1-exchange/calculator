FROM golang AS builder

ADD . /app/
WORKDIR /app

RUN go mod tidy
RUN go build

FROM ubuntu as runtime
WORKDIR /app
COPY --from=builder /app/calculator .

CMD ["./calculator"]
