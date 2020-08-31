# traffic-collector

A service that collects incoming traffic, and stores it a to a file. 



### To run

Start the application by running the command below
```
go run main. go
```

Send traffic to the service using CUrl

```
curl 'localhost:8081/hello'  -d @some_json_file.json -v
```

