package main

import (
	"log"

	"github.com/pborman/uuid"
	"github.com/temporalio/samples-go/latency-optimization"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func main() {
	// The client and worker are heavyweight objects that should be created once per process.
	c, err := client.Dial(client.Options{})
	if err != nil {
		log.Fatalln("Unable to create client", err)
	}
	defer c.Close()

	w := worker.New(c, latencyoptimization.TaskQueueName, worker.Options{})

	w.RegisterWorkflow(latencyoptimization.Workflow)
	w.RegisterWorkflow(latencyoptimization.WorkflowAllRegularActivities)
	w.RegisterWorkflow(latencyoptimization.WorkflowLocal)
	
	transaction := &latencyoptimization.Transaction{ID: uuid.New(), SourceAccount: "Bob", TargetAccount: "Alice", Amount: 100}
	w.RegisterActivity(transaction)

	err = w.Run(worker.InterruptCh())
	if err != nil {
		log.Fatalln("Unable to start worker", err)
	}
}
