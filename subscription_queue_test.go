package stomp

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/jacoelho/stomp/internal/session"
)

func newQueueForTest(capacity int) *Subscription {
	return newSubscription(nil, SubscribeRequest{ID: "q", Capacity: capacity, CapacityBytes: 1 << 20, EndOnOverflow: true}, session.ClientIndividual)
}

func TestQueueKeepsOrderAcrossCompaction(t *testing.T) {
	const capacity = 5
	s := newQueueForTest(capacity)
	pushed, next := 0, 0
	for round := range 200 {
		for range 1 + round%capacity {
			if !s.push(Message{ID: strconv.Itoa(pushed)}, nil) {
				break
			}
			pushed++
		}
		for range 1 + (round*7)%capacity {
			if next == pushed {
				break
			}
			m, err := readNew(context.Background(), s)
			if err != nil || m.ID != strconv.Itoa(next) {
				t.Fatalf("round %d: ReadMessage = %q, %v; want %d", round, m.ID, err, next)
			}
			next++
		}
		if c := cap(s.queue); c > 4*capacity {
			t.Fatalf("round %d: queue capacity %d exceeds 4×Capacity with %d queued", round, c, pushed-next)
		}
	}
}

func TestQueueSteadyStateDoesNotAllocate(t *testing.T) {
	for _, backlog := range []int{0, 3} {
		s := newQueueForTest(256)
		for range backlog {
			s.push(Message{}, nil)
		}
		ctx := context.Background()
		if n := testing.AllocsPerRun(1000, func() {
			s.push(Message{}, nil)
			readNew(ctx, s)
		}); n != 0 {
			t.Errorf("push then ReadMessage with a backlog of %d allocates %v times; want 0", backlog, n)
		}
	}
}

func TestQueueReleasesABurstBeyondCapacityOnceDrained(t *testing.T) {
	s := newSubscription(nil, SubscribeRequest{ID: "q", Capacity: 4, CapacityBytes: 1 << 20}, session.ClientIndividual)
	for i := range 1000 {
		if !s.push(Message{ID: strconv.Itoa(i)}, nil) {
			t.Fatalf("push %d refused without EndOnOverflow (SUB-8)", i)
		}
	}
	for i := range 1000 {
		m, err := readNew(context.Background(), s)
		if err != nil || m.ID != strconv.Itoa(i) {
			t.Fatalf("ReadMessage = %q, %v; want %d", m.ID, err, i)
		}
	}
	if c := cap(s.queue); c > 4*4 {
		t.Errorf("drained queue keeps capacity %d; want at most 4×Capacity", c)
	}
}

func TestQueueShrinksABurstWhileABacklogRemains(t *testing.T) {
	s := newSubscription(nil, SubscribeRequest{ID: "q", Capacity: 4, CapacityBytes: 1 << 20}, session.ClientIndividual)
	pushed, next := 0, 0
	for range 1000 {
		s.push(Message{ID: strconv.Itoa(pushed)}, nil)
		pushed++
	}
	for range 5000 {
		for next < pushed-2 {
			m, err := readNew(context.Background(), s)
			if err != nil || m.ID != strconv.Itoa(next) {
				t.Fatalf("ReadMessage = %q, %v; want %d", m.ID, err, next)
			}
			next++
		}
		s.push(Message{ID: strconv.Itoa(pushed)}, nil)
		pushed++
	}
	if c := cap(s.queue); c > 4*4 {
		t.Errorf("queue with a backlog of 2 keeps capacity %d after a burst of 1000; want at most 4×Capacity", c)
	}
}

func bodyFor(i int) []byte { return bytes.Repeat([]byte{byte('a' + i%26)}, 1+i*7%97) }

func newArenaQueueForTest() *Subscription {
	return newSubscription(nil, SubscribeRequest{ID: "q", Capacity: 8, CapacityBytes: 1 << 30}, session.ClientIndividual)
}

func TestReadMessageBodiesSurviveCompactionAndDrain(t *testing.T) {
	s := newArenaQueueForTest()
	ctx := context.Background()
	pushed, read := 0, 0
	var m Message
	readOne := func() {
		t.Helper()
		if err := s.ReadMessage(ctx, &m); err != nil {
			t.Fatal(err)
		}
		if want := bodyFor(read); !bytes.Equal(m.Body, want) {
			t.Fatalf("message %d: body %q, want %q", read, m.Body, want)
		}
		read++
	}
	for round := range 300 {
		for range 1 + round%7 {
			s.push(Message{}, bodyFor(pushed))
			pushed++
		}
		for range round % 9 {
			if read < pushed {
				readOne()
			}
		}
	}
	for read < pushed {
		readOne()
	}
}

func TestReadMessageIntoAReusedMessageAllocatesNoBody(t *testing.T) {
	s := newArenaQueueForTest()
	ctx := context.Background()
	body := bytes.Repeat([]byte("x"), 4096)
	var m Message
	s.push(Message{}, body)
	if err := s.ReadMessage(ctx, &m); err != nil {
		t.Fatal(err)
	}
	storage := &m.Body[0]
	allocs := testing.AllocsPerRun(100, func() {
		s.push(Message{}, body[:100])
		if err := s.ReadMessage(ctx, &m); err != nil {
			t.Fatal(err)
		}
	})
	if allocs != 0 {
		t.Errorf("push and ReadMessage allocated %v times per message, want 0", allocs)
	}
	if &m.Body[0] != storage || !bytes.Equal(m.Body, body[:100]) {
		t.Errorf("ReadMessage did not reuse m.Body's storage for a 100-octet body")
	}
}

func TestMessageReadIntoANewMessageIsNotModifiedByLaterReads(t *testing.T) {
	s := newArenaQueueForTest()
	ctx := context.Background()
	s.push(Message{}, []byte("first"))
	s.push(Message{}, []byte("second"))
	kept, err := readNew(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	m := Message{Body: make([]byte, 0, 64)}
	if err := s.ReadMessage(ctx, &m); err != nil {
		t.Fatal(err)
	}
	s.push(Message{}, []byte("third!"))
	if err := s.ReadMessage(ctx, &m); err != nil {
		t.Fatal(err)
	}
	if string(kept.Body) != "first" {
		t.Errorf("body returned by ReadMessage = %q after later reads, want %q", kept.Body, "first")
	}
}

func TestReadMessageLeavesTheMessageUnchangedOnError(t *testing.T) {
	s := newArenaQueueForTest()
	s.end(ErrUnsubscribed)
	m := Message{ID: "kept", Body: []byte("body")}
	if err := s.ReadMessage(context.Background(), &m); !errors.Is(err, ErrUnsubscribed) {
		t.Fatalf("ReadMessage = %v, want ErrUnsubscribed", err)
	}
	if m.ID != "kept" || string(m.Body) != "body" {
		t.Errorf("message after error = %+v, want it unchanged", m)
	}
}

func TestArenaStaysBoundedWhileTheQueueNeverEmpties(t *testing.T) {
	s := newArenaQueueForTest()
	ctx := context.Background()
	body := bytes.Repeat([]byte("x"), 1000)
	s.push(Message{}, body)
	for range 10000 {
		s.push(Message{}, body)
		if _, err := readNew(ctx, s); err != nil {
			t.Fatal(err)
		}
		if live := s.bytes; len(s.arena) > 2*live+len(body) {
			t.Fatalf("arena holds %d octets for %d waiting", len(s.arena), live)
		}
	}
}

func TestArenaReleasesABurstWhileABacklogRemains(t *testing.T) {
	s := newArenaQueueForTest()
	ctx := context.Background()
	body := make([]byte, 4096)
	for range 4000 {
		s.push(Message{}, body)
	}
	for range 3999 {
		if _, err := readNew(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	for range 2 * arenaWindow * cap(s.arena) / len(body) {
		s.push(Message{}, body)
		if _, err := readNew(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	if c, limit := cap(s.arena), 4*minArena; c > limit {
		t.Errorf("arena keeps %d octets for one waiting 4 KiB body, want at most %d", c, limit)
	}
}

func TestArenaStaysNearCapacityBytesWithEndOnOverflow(t *testing.T) {
	const capacityBytes = 1 << 20
	s := newSubscription(nil, SubscribeRequest{ID: "q", Capacity: 1 << 20, CapacityBytes: capacityBytes, EndOnOverflow: true}, session.ClientIndividual)
	ctx := context.Background()
	body := make([]byte, 1000)
	peak := 0
	for i := range 20000 {
		for s.push(Message{}, body) {
			peak = max(peak, cap(s.arena))
		}
		for range 1 + i%50 {
			if _, err := readNew(ctx, s); err != nil {
				t.Fatal(err)
			}
		}
	}
	if limit := capacityBytes + capacityBytes/4; peak > limit {
		t.Errorf("arena peaked at %d octets with CapacityBytes %d, want at most %d", peak, capacityBytes, limit)
	}
}

func TestArenaKeepsStorageARecentBacklogUsed(t *testing.T) {
	ctx := context.Background()
	drain := func(t *testing.T, s *Subscription, n, size int) {
		t.Helper()
		for range n {
			s.push(Message{}, make([]byte, size))
		}
		for range n {
			if _, err := readNew(ctx, s); err != nil {
				t.Fatal(err)
			}
		}
	}
	t.Run("backlogs of varying depth keep the storage", func(t *testing.T) {
		s := newArenaQueueForTest()
		drain(t, s, 64, 4096)
		c := cap(s.arena)
		for i := range 200 {
			drain(t, s, 1+i%64, 4096)
			if cap(s.arena) != c {
				t.Fatalf("after backlog %d the arena holds %d octets, want the %d the first backlog grew", i, cap(s.arena), c)
			}
		}
	})
	t.Run("a burst is released once its size has passed in small backlogs", func(t *testing.T) {
		s := newArenaQueueForTest()
		drain(t, s, 1000, 4096)
		burst := cap(s.arena)
		for range 2 * arenaWindow * burst / 4096 {
			drain(t, s, 1, 4096)
		}
		if c := cap(s.arena); c > minArena {
			t.Errorf("arena keeps %d octets after a %d-octet burst and single 4 KiB bodies, want at most %d", c, burst, minArena)
		}
	})
	t.Run("one large body at a time", func(t *testing.T) {
		s := newArenaQueueForTest()
		for range 10 {
			drain(t, s, 1, 1<<20)
		}
		if c := cap(s.arena); c < 1<<20 {
			t.Errorf("drained arena keeps %d octets, want room for the 1 MiB body", c)
		}
	})
}

func readNew(ctx context.Context, s *Subscription) (Message, error) {
	var m Message
	err := s.ReadMessage(ctx, &m)
	return m, err
}
