package accumulator

import (
	"time"

	"go.temporal.io/sdk/workflow"
)

/**
 * This sample demonstrates how to accumulate many signals (business events) over a time period.
 * This sample implements the Accumulator Pattern: collect many meaningful things that
 *   need to be collected and worked on together, such as all payments for an account, or
 *   all account updates by account.
 * This sample models robots being created throughout the time period,
 *   groups them by what color they are, and greets all the robots of a color at the end.
 *
 * A new workflow is created per grouping. Workflows continue as new as needed.
 * A sample activity at the end is given, and you could add an activity to
 *   process individual events in the processGreeting() method.
 *
 * Because Temporal Workflows cannot have an unlimited size, Continue As New is used
 *   to process more signals that may come in.
 * You could create as many groupings as desired, as Temporal Workflows scale out to many workflows without limit.
 * You could vary the time that the workflow waits for other signals, say for a day, or a variable time from first
 *   signal with the GetNextTimeout() function.
 */

// signalToSignalTimeout is the time to wait after a signal is received
const signalToSignalTimeout = 30 * time.Second

// fromStartTimeout is the minimum time to wait from start even if a signal isn't received
const fromStartTimeout = 60 * time.Second

// exitTimeout is the time to wait after exit is requested to catch any last few signals
const exitTimeout = 1 * time.Second

type AccumulateGreeting struct {
	GreetingText string
	Bucket       string
	GreetingKey  string
}

type GreetingsInfo struct {
	BucketKey          string
	GreetingsList      []AccumulateGreeting
	UniqueGreetingKeys map[string]bool
	startTime          time.Time
}

// getNextTimeout returns the maximum time for a workflow to wait for the next signal.
// This waits for the greater of the remaining fromStartTimeout and  signalToSignalTimeout
// fromStartTimeout and signalToSignalTimeout can be adjusted to wait for the right amount of time as desired
// This resets with Continue As New
func getNextTimeout(ctx workflow.Context, startTime time.Time, exitRequested bool) (time.Duration, error) {
	if exitRequested {
		return exitTimeout, nil
	}
	if startTime.IsZero() {
		startTime = workflow.GetInfo(ctx).WorkflowStartTime // if you want to start from the time of the first signal, customize this
	}
	total := workflow.Now(ctx).Sub(startTime)
	totalLeft := fromStartTimeout - total
	if totalLeft <= 0 {
		return 0, nil
	}
	if signalToSignalTimeout > totalLeft {
		return signalToSignalTimeout, nil
	}
	return totalLeft, nil
}

// AccumulateSignalsWorkflow workflow definition
func AccumulateSignalsWorkflow(ctx workflow.Context, greetings GreetingsInfo) (allGreetings string, err error) {
	log := workflow.GetLogger(ctx)
	var a AccumulateGreeting
	if greetings.GreetingsList == nil {
		greetings.GreetingsList = []AccumulateGreeting{}
	}
	if greetings.UniqueGreetingKeys == nil {
		greetings.UniqueGreetingKeys = make(map[string]bool)
	}
	var unprocessedGreetings []AccumulateGreeting
	if greetings.startTime.IsZero() {
		greetings.startTime = workflow.Now(ctx)
	}
	exitRequested := false

	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 100 * time.Second,
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	// set up signal channels
	workflow.Go(ctx, func(ctx workflow.Context) {
		for {
			selector := workflow.NewSelector(ctx)
			selector.AddReceive(workflow.GetSignalChannel(ctx, "greeting"), func(c workflow.ReceiveChannel, more bool) {
				c.Receive(ctx, &a)
				unprocessedGreetings = append(unprocessedGreetings, a)
			})
			selector.Select(ctx)
		}
	})

	workflow.Go(ctx, func(ctx workflow.Context) {
		for {
			selector := workflow.NewSelector(ctx)
			selector.AddReceive(workflow.GetSignalChannel(ctx, "exit"), func(c workflow.ReceiveChannel, more bool) {
				c.Receive(ctx, nil)
				exitRequested = true
			})
			selector.Select(ctx)
		}
	})

	// wait for signals to come in
	for !workflow.GetInfo(ctx).GetContinueAsNewSuggested() {

		timeout, err := getNextTimeout(ctx, greetings.startTime, exitRequested)

		if err != nil {
			log.Error("Error calculating timeout")
			return "", err
		}
		log.Info("Awaiting", "timeout", timeout.String())
		
		sleepErr := workflow.Sleep(ctx, 10*time.Second)
		if sleepErr != nil {
			log.Error("Error sleeping")
			return "", err
		}

		if len(unprocessedGreetings) == 0 { // timeout without a signal coming in, so let's process the greetings and wrap it up!
			log.Info("Into final processing", "greeting count", len(greetings.GreetingsList))
			allGreetings = ""
			err := workflow.ExecuteActivity(ctx, ComposeGreeting, greetings.GreetingsList).Get(ctx, &allGreetings)
			if err != nil {
				log.Error("ComposeGreeting activity failed.", "Error", err)
				return allGreetings, err
			}

			// if !selector.HasPending() { // in case a signal came in while activity was running, check again
			// 	return allGreetings, nil
			// } else {
			// 	log.Info("Received a signal while processing ComposeGreeting activity.")
			// }
		}

		/* process latest signals
		 * Here is where we can process individual signals as they come in.
		 * It's ok to call activities here.
		 * This also validates an individual greeting:
		 * - check for duplicates
		 * - check for correct bucket
		 * Using update validation could improve this in the future
		 */
		toProcess := unprocessedGreetings
		unprocessedGreetings = nil

		for _, ug := range toProcess {
			if ug.Bucket != greetings.BucketKey {
				log.Warn("Wrong bucket, something is wrong with your signal processing. WF Bucket: [" + greetings.BucketKey + "], greeting bucket: [" + ug.Bucket + "]")
			} else if greetings.UniqueGreetingKeys[ug.GreetingKey] {
				log.Warn("Duplicate Greeting Key. Key: [" + ug.GreetingKey + "]")
			} else {
				greetings.UniqueGreetingKeys[ug.GreetingKey] = true
				greetings.GreetingsList = append(greetings.GreetingsList, ug)
			}
		}

	}

	log.Info("Accumulate workflow continuing as new", "greeting count", len(greetings.GreetingsList))
	return "Continued As New.", workflow.NewContinueAsNewError(ctx, AccumulateSignalsWorkflow, greetings)
}
