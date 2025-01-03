### Latency-Optimization Sample

This sample demonstrates some techniques about how to do latency optimization for a Temporal workflow.

By utilizing Update-with-Start, a client can start a new workflow and synchronously receive 
a response mid-workflow, while the workflow continues to run to completion.

By utilizing local activities you can optimize further.

### Steps to run this sample:
1) Run a [Temporal service](https://github.com/temporalio/samples-go/tree/main/#how-to-use).
2) Run the following command to start the worker
```
go run latency-optimization/worker/main.go
```
3) Run the following command to start the example
```
go run latency-optimization/starter/main.go
```
