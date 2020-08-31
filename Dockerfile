FROM scratch

WORKDIR /app

COPY ./traffic-collector .

ENTRYPOINT ["/app/traffic-collector"]
