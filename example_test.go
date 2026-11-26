package stomp_test

import (
	"context"
	"errors"
	"log"

	"github.com/jacoelho/stomp"
	"github.com/jacoelho/stomp/stomptext"
)

func Example() {
	ctx := context.Background()
	c, err := stomp.Open(ctx, stomp.TCP("localhost:61613"), stomp.Login("app", "secret"), stomptext.MaxFrameSize(8<<20))
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close(context.Background())

	err = c.Send(ctx, stomp.Outgoing{Destination: "/queue/orders", Body: []byte(`{"id":17}`)})
	if errors.Is(err, stomp.ErrOutcomeUnknown) {
		log.Print("the broker may or may not have the message; resend only if processing it twice is safe")
	} else if err != nil {
		log.Fatal(err)
	}
}

func ExampleSubscription_ReadMessage() {
	ctx := context.Background()
	c, err := stomp.Open(ctx, stomp.TCP("localhost:61613"))
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close(context.Background())

	sub, err := c.Subscribe(ctx, stomp.SubscribeRequest{
		ID:          "billing",
		Destination: "/queue/orders",
		Ack:         stomp.AckClientIndividual,
		Headers:     stomp.Headers{{Name: "consumer-window-size", Value: "65536"}},
	})
	if err != nil {
		log.Fatal(err)
	}
	var m stomp.Message
	for {
		if err := sub.ReadMessage(ctx, &m); err != nil {
			log.Fatal(err)
		}
		log.Printf("order %s", m.Body)
		if err := sub.Ack(ctx, m); err != nil && !errors.Is(err, stomp.ErrStaleAck) {
			log.Fatal(err)
		}
	}
}
