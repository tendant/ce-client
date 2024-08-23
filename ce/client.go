package ce

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	cloudevents "github.com/cloudevents/sdk-go/v2"
)

type EventClient struct {
	ctx    context.Context
	client cloudevents.Client
	wg     *sync.WaitGroup
}

func NewEventClient(ctx context.Context, wg *sync.WaitGroup, target string) (*EventClient, error) {
	c, err := cloudevents.NewClientHTTP()
	if err != nil {
		return nil, err
	}

	// Set a target.
	// target := "http://localhost:14000/events"
	ctx = cloudevents.ContextWithTarget(ctx, target)

	return &EventClient{
		ctx:    ctx,
		client: c,
		wg:     wg,
	}, nil
}

type EventGeneric struct {
	Source  string
	Type    string
	Subject string
	Data    map[string]any
}

func (c *EventClient) SendEvent(e EventGeneric) error {
	if c.client == nil {
		return errors.New("failed sending event, no cloudevents client")
	}
	event := cloudevents.NewEvent()
	event.SetSource(e.Source)
	event.SetType(e.Type)
	event.SetSubject(e.Subject)
	event.SetData(cloudevents.ApplicationJSON, e.Data)

	// Send that Event.
	if result := c.client.Send(c.ctx, event); cloudevents.IsUndelivered(result) {
		return result
	}

	return nil
}

func (c *EventClient) SendEventAsync(e EventGeneric) error {
	if c.wg == nil {
		slog.Info("Sending Event without WaitGroup")
		c.SendEvent(e)
		slog.Info("Event Sent without WaitGroup")
		return errors.New("Warning: SendEventAsync without WaitGroup, event might not be sent!")

	} else {
		c.wg.Add(1)
		go func(e EventGeneric) {
			slog.Info("Sending Event with WaitGroup")
			defer c.wg.Done()
			c.SendEvent(e)
			slog.Info("Event Sent with WaitGroup")
		}(e)
		return nil

	}
}

/*
var wg sync.WaitGroup
eventClient := esce.NewEventClient(context.Background(), &wg, w.SinkURL)
eventClient.SendEventAsync(esce.EventGeneric{
	Source: "webhook-notify",
	Type:   "category.event",
	Data: map[string]string{
		"token":              commandoTxn.GetToken(),
	},
})
wg.Wait()
*/
