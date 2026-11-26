package stomp_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/jacoelho/stomp"
)

// The simulation runs a Client, a simBroker and a simNet in one synctest
// bubble. Fault decisions come from a PCG seeded by the test seed; goroutine
// scheduling and the client's backoff jitter are not seeded, so a failing seed
// may not reproduce on every run.
//
//	STOMP_SIM_SEED=n       run seed n alone with its fault log
//	STOMP_SIM_SEEDS=n      run seeds 1..n

type simConfig struct {
	seed       uint64
	intensity  uint8
	faultPhase time.Duration
	verbose    bool
}

type simFaultKind int

const (
	simPartitionOne simFaultKind = iota
	simPartitionAll
	simLatencySpike
	simReset
	simResetMidFrame
	simRestart
	simDialRefuse
	simDialSlow
	simSlowBroker
	simFaultKinds
)

func (k simFaultKind) String() string {
	return [...]string{"partition-one", "partition-all", "latency-spike", "reset", "reset-mid-frame",
		"broker-restart", "dial-refuse", "dial-slow", "slow-broker"}[k]
}

type simFault struct {
	at, dur  time.Duration
	kind     simFaultKind
	target   uint64
	param    time.Duration
	offset   int
	toBroker bool
}

func (f simFault) String() string {
	s := fmt.Sprintf("+%v %s", f.at.Round(time.Millisecond), f.kind)
	switch f.kind {
	case simPartitionOne, simPartitionAll, simDialRefuse:
		s += fmt.Sprintf(" for %v", f.dur.Round(time.Millisecond))
	case simLatencySpike, simDialSlow, simSlowBroker:
		s += fmt.Sprintf(" %v for %v", f.param.Round(time.Millisecond), f.dur.Round(time.Millisecond))
	case simRestart:
		s += fmt.Sprintf(" down %v", f.param.Round(time.Millisecond))
	case simResetMidFrame:
		s += fmt.Sprintf(" after %d octets toBroker=%v", f.offset, f.toBroker)
	}
	return s
}

func simDur(r *rand.Rand, lo, hi time.Duration) time.Duration {
	return lo + time.Duration(r.Int64N(int64(hi-lo)))
}

func simSchedule(r *rand.Rand, phase time.Duration, intensity uint8) []simFault {
	meanGap := time.Duration(float64(6*time.Second) * 128 / float64(int(intensity)+1))
	var fs []simFault
	for at := simDur(r, 0, 2*meanGap); at < phase; at += simDur(r, 0, 2*meanGap) {
		f := simFault{at: at, kind: simFaultKind(r.IntN(int(simFaultKinds))), target: r.Uint64()}
		switch f.kind {
		case simPartitionOne, simPartitionAll:
			f.dur = simDur(r, 500*time.Millisecond, 8*time.Second)
		case simLatencySpike:
			f.dur, f.param = simDur(r, time.Second, 10*time.Second), simDur(r, 50*time.Millisecond, 1500*time.Millisecond)
		case simResetMidFrame:
			f.offset, f.toBroker = 1+r.IntN(400), r.IntN(2) == 0
		case simRestart:
			f.param = simDur(r, time.Millisecond, 3*time.Second)
		case simDialRefuse:
			f.dur = simDur(r, time.Second, 6*time.Second)
		case simDialSlow:
			f.dur, f.param = simDur(r, time.Second, 10*time.Second), simDur(r, 500*time.Millisecond, 4*time.Second)
		case simSlowBroker:
			f.dur, f.param = simDur(r, time.Second, 10*time.Second), simDur(r, 50*time.Millisecond, 800*time.Millisecond)
		}
		fs = append(fs, f)
	}
	return fs
}

type simSendRec struct {
	key string
	err error
}

type simTxRec struct {
	id    string
	sends []simSendRec
	abort bool
	err   error
}

type simApp struct {
	mu         sync.Mutex
	returned   map[string]bool      // message keys ReadMessage returned
	deliveries map[string]time.Time // x-delivery headers ReadMessage returned, and when
	sends      []simSendRec
	txs        []*simTxRec
	active     map[int]string // consumer slot to its subscription id
	violations []simViolation
	nexts      int
	overflows  int
	staleAcks  int
	txAcks     int // acknowledgements confirmed by a Commit
}

func (a *simApp) violate(prop, spec, format string, args ...any) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.violations = append(a.violations, simViolation{prop, spec, fmt.Sprintf(format, args...)})
}

func (a *simApp) check(call, spec string, err error, allowed ...error) {
	if err == nil || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return
	}
	for _, e := range allowed {
		if errors.Is(err, e) {
			return
		}
	}
	var serr *stomp.ServerError
	if errors.As(err, &serr) && slices.Contains(allowed, error(errServerAllowed)) {
		return
	}
	a.violate("g", spec, "%s returned undocumented error %v", call, err)
}

// errServerAllowed marks *stomp.ServerError as allowed in simApp.check.
var errServerAllowed = errors.New("server error allowed")

func (a *simApp) deliveryReturned(id string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	_, ok := a.deliveries[id]
	return ok
}

func (a *simApp) recordReturned(m stomp.Message) {
	d, _ := m.Headers.Get("x-delivery")
	a.mu.Lock()
	defer a.mu.Unlock()
	a.returned[simKey(m.Body)] = true
	if _, ok := a.deliveries[d]; !ok {
		a.deliveries[d] = time.Now()
	}
	a.nexts++
}

func (a *simApp) setActive(slot int, id string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if id == "" {
		delete(a.active, slot)
	} else {
		a.active[slot] = id
	}
}

func (a *simApp) activeIDs() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	ids := make([]string, 0, len(a.active))
	for _, id := range a.active {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

type simWorld struct {
	cfg    simConfig
	start  time.Time
	net    *simNet
	broker *simBroker
	client *stomp.Client
	app    *simApp

	logMu sync.Mutex
	log   []string
}

func (w *simWorld) logf(format string, args ...any) {
	line := fmt.Sprintf("[%9.3fs] ", time.Since(w.start).Seconds()) + fmt.Sprintf(format, args...)
	w.logMu.Lock()
	defer w.logMu.Unlock()
	w.log = append(w.log, line)
}

func simBody(r *rand.Rand, key string) []byte {
	if r.IntN(30) == 0 {
		return []byte(key + "|" + strings.Repeat("x", 4<<10+r.IntN(16<<10)))
	}
	return []byte(key)
}

func simSleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func (w *simWorld) produce(ctx context.Context, id int, r *rand.Rand) {
	c, a := w.client, w.app
	for n := 1; simSleep(ctx, simDur(r, 20*time.Millisecond, 300*time.Millisecond)); n++ {
		callCtx, cancel := context.WithTimeout(context.Background(), simDur(r, 500*time.Millisecond, 10*time.Second))
		if r.IntN(5) == 0 {
			w.produceTx(callCtx, id, n, r)
			cancel()
			continue
		}
		key := fmt.Sprintf("p%d-%d", id, n)
		err := c.Send(callCtx, stomp.Outgoing{Destination: simQueue, Body: simBody(r, key)})
		cancel()
		a.check("Send", "SND-5/SND-6/SND-8", err, stomp.ErrOutcomeUnknown, stomp.ErrClosed, errServerAllowed)
		a.mu.Lock()
		a.sends = append(a.sends, simSendRec{key, err})
		a.mu.Unlock()
	}
}

func (w *simWorld) produceTx(ctx context.Context, id, n int, r *rand.Rand) {
	c, a := w.client, w.app
	tx, err := c.Begin(ctx)
	a.check("Begin", "TX-1", err, stomp.ErrOutcomeUnknown, stomp.ErrClosed, errServerAllowed)
	if err != nil {
		return
	}
	rec := &simTxRec{id: tx.ID(), abort: r.IntN(4) == 0}
	for i := range 1 + r.IntN(3) {
		key := fmt.Sprintf("p%d-%d.%d", id, n, i)
		err := tx.Send(ctx, stomp.Outgoing{Destination: simQueue, Body: simBody(r, key)})
		a.check("Tx.Send", "TX-2/TX-5", err, stomp.ErrTxAborted, stomp.ErrClosed)
		rec.sends = append(rec.sends, simSendRec{key, err})
		if r.IntN(3) == 0 {
			simSleep(ctx, simDur(r, time.Millisecond, 200*time.Millisecond))
		}
	}
	if rec.abort {
		rec.err = tx.Abort(ctx)
		a.check("Abort", "TX-4/TX-5", rec.err, stomp.ErrOutcomeUnknown, stomp.ErrTxAborted, stomp.ErrClosed, errServerAllowed)
	} else {
		rec.err = tx.Commit(ctx)
		a.check("Commit", "TX-4/TX-5/TX-6", rec.err, stomp.ErrOutcomeUnknown, stomp.ErrTxAborted, stomp.ErrClosed, errServerAllowed)
	}
	a.mu.Lock()
	a.txs = append(a.txs, rec)
	a.mu.Unlock()
}

type simConsumer struct {
	slot     int
	mode     stomp.AckMode
	capacity int
	// endOnOverflow sets SubscribeRequest.EndOnOverflow; without it ErrOverflow
	// is a violation (SUB-8).
	endOnOverflow bool
	// id is a stable subscription id reused after Unsubscribe; empty lets the
	// client generate one.
	id          string
	nextTimeout bool
}

// consume runs one consumer slot until ctx ends. Before healed is closed it
// occasionally unsubscribes and subscribes again; after, it resubscribes only
// when the subscription ended, so a missing replay is not repaired.
func (w *simWorld) consume(ctx context.Context, cc simConsumer, r *rand.Rand, healed <-chan struct{}) {
	a := w.app
	var sub *stomp.Subscription
	for ctx.Err() == nil {
		if sub == nil {
			if sub = w.subscribe(ctx, cc); sub == nil {
				simSleep(ctx, 100*time.Millisecond)
				continue
			}
		}
		nctx, cancel := ctx, context.CancelFunc(func() {})
		if cc.nextTimeout {
			nctx, cancel = context.WithTimeout(ctx, simDur(r, 20*time.Millisecond, 2*time.Second))
		}
		m, err := readNew(nctx, sub)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if errors.Is(err, context.DeadlineExceeded) {
				continue
			}
			if cc.endOnOverflow {
				a.check("ReadMessage", "SUB-45/SUB-14", err, stomp.ErrOverflow, stomp.ErrClosed, errServerAllowed)
			} else {
				a.check("ReadMessage", "SUB-8/SUB-14", err, stomp.ErrClosed, errServerAllowed)
			}
			if errors.Is(err, stomp.ErrOverflow) {
				a.mu.Lock()
				a.overflows++
				a.mu.Unlock()
			}
			a.setActive(cc.slot, "")
			sub = nil
			continue
		}
		a.recordReturned(m)
		pause := simDur(r, 5*time.Millisecond, 80*time.Millisecond)
		if r.IntN(20) == 0 {
			pause = simDur(r, 200*time.Millisecond, 3*time.Second)
		}
		simSleep(ctx, pause)
		if cc.mode != stomp.AckAuto {
			w.acknowledge(ctx, sub, m, cc.mode, r)
		}
		if isClosed(healed) || r.IntN(100) != 0 {
			continue
		}
		uctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err = sub.Unsubscribe(uctx)
		cancel()
		a.check("Unsubscribe", "SUB-15/SUB-17/SUB-26", err, stomp.ErrOutcomeUnknown, stomp.ErrClosed, errServerAllowed)
		a.setActive(cc.slot, "")
		old := sub
		sub = nil
		switch r.IntN(3) {
		case 0:
		case 1:
			w.drain(ctx, old, cc.mode)
		case 2:
			sub = w.subscribe(ctx, cc)
			w.drain(ctx, old, cc.mode)
		}
	}
}

func (w *simWorld) subscribe(ctx context.Context, cc simConsumer) *stomp.Subscription {
	a := w.app
	sctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	s, err := w.client.Subscribe(sctx, stomp.SubscribeRequest{ID: cc.id, Destination: simQueue, Ack: cc.mode, Capacity: cc.capacity, EndOnOverflow: cc.endOnOverflow})
	a.check("Subscribe", "SUB-4/SUB-5", err, stomp.ErrOutcomeUnknown, stomp.ErrDuplicateID, stomp.ErrClosed, errServerAllowed)
	if err != nil {
		return nil
	}
	a.setActive(cc.slot, s.ID())
	return s
}

// drain takes the messages an unsubscribed subscription still holds and
// acknowledges them, which must write nothing: the subscription has ended
// and the broker has taken its deliveries back.
func (w *simWorld) drain(ctx context.Context, old *stomp.Subscription, mode stomp.AckMode) {
	a := w.app
	for {
		m, err := readNew(ctx, old)
		if err != nil {
			if ctx.Err() == nil {
				a.check("ReadMessage after Unsubscribe", "SUB-14", err, stomp.ErrUnsubscribed, stomp.ErrOverflow, stomp.ErrClosed, errServerAllowed)
			}
			return
		}
		a.recordReturned(m)
		if mode == stomp.AckAuto {
			continue
		}
		actx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err = old.Ack(actx, m)
		cancel()
		a.check("Ack after Unsubscribe", "SUB-21", err, stomp.ErrStaleAck, stomp.ErrClosed)
		if err == nil {
			d, _ := m.Headers.Get("x-delivery")
			a.violate("e", "SUB-15/SUB-21", "Ack of delivery %s (%s) from unsubscribed %s returned nil: an ACK was written for a subscription that has ended", d, simKey(m.Body), old.ID())
		}
	}
}

func (w *simWorld) acknowledge(ctx context.Context, sub *stomp.Subscription, m stomp.Message, mode stomp.AckMode, r *rand.Rand) {
	a := w.app
	actx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	roll := r.IntN(100)
	if roll >= 90 && w.acknowledgeTx(actx, m, r) {
		return
	}
	var err error
	var call string
	switch {
	case roll < 10:
		call, err = "AckWait", sub.AckWait(actx, m)
	case roll < 14:
		call, err = "Nack", sub.Nack(actx, m)
	case roll < 16 && mode == stomp.AckClientIndividual:
		call, err = "NackWait", sub.NackWait(actx, m)
	default:
		call, err = "Ack", sub.Ack(actx, m)
	}
	if strings.HasSuffix(call, "Wait") {
		a.check(call, "SUB-20/SUB-21", err, stomp.ErrStaleAck, stomp.ErrOutcomeUnknown, stomp.ErrClosed, errServerAllowed)
	} else {
		a.check(call, "SUB-19/SUB-21", err, stomp.ErrStaleAck, stomp.ErrClosed)
	}
	if errors.Is(err, stomp.ErrStaleAck) {
		a.mu.Lock()
		a.staleAcks++
		a.mu.Unlock()
	}
}

func (w *simWorld) inject(ctx context.Context, faults []simFault) {
	type event struct {
		at   time.Duration
		heal bool
		i    int
	}
	var evs []event
	for i, f := range faults {
		evs = append(evs, event{at: f.at, i: i})
		if f.dur > 0 {
			evs = append(evs, event{at: f.at + f.dur, heal: true, i: i})
		}
	}
	slices.SortStableFunc(evs, func(a, b event) int { return int(a.at - b.at) })
	targets := map[int]*simLink{}
	for _, ev := range evs {
		if ev.at >= w.cfg.faultPhase {
			break
		}
		if !simSleep(ctx, time.Until(w.start.Add(ev.at))) {
			return
		}
		f := faults[ev.i]
		if ev.heal {
			w.logf("heal %s", f.kind)
			switch f.kind {
			case simPartitionOne:
				if l := targets[ev.i]; l != nil {
					w.net.healLink(l)
				}
			case simPartitionAll:
				w.net.healAll()
			case simLatencySpike:
				w.net.setSpike(0)
			case simDialRefuse, simDialSlow:
				w.net.setDialFaults(false, 0)
			case simSlowBroker:
				w.broker.slow(time.Time{}, 0)
			}
			continue
		}
		l := w.net.pick(f.target)
		w.logf("fault %v link=%v", f, l)
		switch f.kind {
		case simPartitionOne:
			if l != nil {
				targets[ev.i] = l
				w.net.partitionLink(l)
			}
		case simPartitionAll:
			w.net.partitionAll()
		case simLatencySpike:
			w.net.setSpike(f.param)
		case simReset:
			if l != nil {
				w.net.reset(l)
			}
		case simResetMidFrame:
			if l != nil {
				p := l.c2s
				if !f.toBroker {
					p = l.s2c
				}
				p.armReset(f.offset, func() { w.logf("mid-frame reset of %v fires", l); w.net.reset(l) })
			}
		case simRestart:
			w.broker.restart(f.param)
		case simDialRefuse:
			w.net.setDialFaults(true, 0)
		case simDialSlow:
			w.net.setDialFaults(false, f.param)
		case simSlowBroker:
			w.broker.slow(time.Now().Add(f.dur), f.param)
		}
	}
	simSleep(ctx, time.Until(w.start.Add(w.cfg.faultPhase)))
}

type simResult struct {
	virtual    time.Duration
	failed     bool
	violations []simViolation
}

// runSim runs one seed and passes its result to record from inside the
// bubble: synctest.Test does not return normally when the seed fails.
func runSim(t *testing.T, cfg simConfig, record func(simResult)) {
	synctest.Test(t, func(t *testing.T) { record(simulate(t, cfg)) })
}

func simulate(t *testing.T, cfg simConfig) simResult {
	root := rand.New(rand.NewPCG(cfg.seed, 0x73696d))
	w := &simWorld{cfg: cfg, start: time.Now(), app: &simApp{
		returned: map[string]bool{}, deliveries: map[string]time.Time{}, active: map[int]string{},
	}}
	version := stomp.V12
	if root.IntN(2) == 0 {
		version = stomp.V11
	}
	window := 64 << 10
	if root.IntN(3) == 0 {
		window = 8 << 10
	}
	msgBeforeReceipt := root.IntN(2) == 0
	modes := []stomp.AckMode{stomp.AckAuto, stomp.AckClient, stomp.AckClientIndividual}
	root.Shuffle(len(modes), func(i, j int) { modes[i], modes[j] = modes[j], modes[i] })
	modes = modes[:2+root.IntN(2)]
	fixedIDs := root.IntN(2) == 0
	consumers := make([]simConsumer, len(modes))
	for i, mode := range modes {
		consumers[i] = simConsumer{slot: i, mode: mode, capacity: 1000}
		if fixedIDs {
			consumers[i].id = fmt.Sprintf("c%d", i)
		}
		consumers[i].nextTimeout = mode == stomp.AckAuto && root.IntN(2) == 0
	}
	if root.IntN(8) == 0 {
		consumers[0].capacity = 2
		consumers[0].endOnOverflow = root.IntN(2) == 0
	}
	faults := simSchedule(root, cfg.faultPhase, cfg.intensity)
	netSeed, brokerSeed := root.Uint64(), root.Uint64()
	workerSeeds := make([]uint64, 2+len(modes))
	for i := range workerSeeds {
		workerSeeds[i] = root.Uint64()
	}
	var closeAt time.Duration
	if root.IntN(5) == 0 {
		closeAt = simDur(root, time.Second, cfg.faultPhase)
	}

	w.net = newSimNet(netSeed, window, w.logf)
	w.broker = newSimBroker(w.net, brokerSeed, msgBeforeReceipt, w.app.deliveryReturned, w.logf)
	w.broker.start()

	header := fmt.Sprintf("seed %d intensity %d: version %v, window %d, MESSAGE-before-RECEIPT %v, consumers %+v, %d faults, close under load at %v",
		cfg.seed, cfg.intensity, version, window, msgBeforeReceipt, consumers, len(faults), closeAt)
	w.logf("%s", header)

	openCtx, cancelOpen := context.WithTimeout(context.Background(), 30*time.Second)
	c, err := stomp.Open(openCtx, w.net.dial,
		stomp.Versions(version),
		stomp.Heartbeat(time.Second, time.Second),
		stomp.WriteTimeout(2*time.Second),
		stomp.HandshakeTimeout(2*time.Second),
		stomp.Backoff(50*time.Millisecond, 2*time.Second),
		stomp.Logger(discardLogger()),
		stomp.OnState(func(s stomp.State, err error) { w.logf("client state %v: %v", s, err) }),
	)
	cancelOpen()
	if err != nil {
		t.Fatalf("seed %d: Open: %v", cfg.seed, err)
	}
	w.client = c
	a := w.app

	healed := make(chan struct{})
	prodCtx, stopProducers := context.WithCancel(context.Background())
	consCtx, stopConsumers := context.WithCancel(context.Background())
	var producers, workers sync.WaitGroup
	for i := range 2 {
		r := rand.New(rand.NewPCG(workerSeeds[i], 1))
		producers.Go(func() { w.produce(prodCtx, i+1, r) })
	}
	for i, cc := range consumers {
		r := rand.New(rand.NewPCG(workerSeeds[2+i], 2))
		workers.Go(func() { w.consume(consCtx, cc, r, healed) })
	}

	var closer sync.WaitGroup
	if closeAt > 0 {
		closer.Go(func() {
			time.Sleep(closeAt)
			w.logf("Close under load")
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := c.Close(ctx); err != nil && !errors.Is(err, stomp.ErrOutcomeUnknown) {
				a.violate("j", "CON-28", "Close under load returned %v", err)
			}
		})
	}

	injCtx, stopInjector := context.WithCancel(context.Background())
	w.inject(injCtx, faults)
	closer.Wait()
	stopInjector()
	w.net.heal()
	w.broker.slow(time.Time{}, 0)
	close(healed)
	healAt := time.Now()
	w.logf("heal: all faults stopped")
	stopProducers()
	if !simWait(&producers, time.Minute) {
		a.violate("j", "SND-13", "producers still running a minute after the heal")
	}

	if closeAt == 0 {
		w.checkLiveness(healAt)
	}

	stopConsumers()
	if !simWait(&workers, time.Minute) {
		a.violate("j", "CON-21", "consumers still running a minute after their context ended")
	}
	closeCtx, cancelClose := context.WithTimeout(context.Background(), 10*time.Second)
	if err := c.Close(closeCtx); err != nil && !errors.Is(err, stomp.ErrOutcomeUnknown) {
		a.violate("j", "CON-28", "Close returned %v", err)
	}
	cancelClose()
	w.broker.stop()
	w.net.stop()
	w.broker.wg.Wait()

	res := simResult{virtual: time.Since(w.start)}
	res.violations = w.verify()
	res.failed = len(res.violations) > 0
	w.report(t, header, faults, res)
	return res
}

func simWait(wg *sync.WaitGroup, d time.Duration) bool {
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-done:
		return true
	case <-t.C:
		return false
	}
}

func (w *simWorld) acceptedKeys() []string {
	b := w.broker
	b.mu.Lock()
	defer b.mu.Unlock()
	keys := make([]string, 0, len(b.accepted))
	for k := range b.accepted {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

// unsettled lists deliveries ReadMessage returned longer than age ago that the broker
// still holds unacknowledged on a live connection.
func (w *simWorld) unsettled(age time.Duration) []string {
	var out []string
	for _, p := range w.broker.pendingLive() {
		w.app.mu.Lock()
		at, ok := w.app.deliveries[p.deliveryID]
		w.app.mu.Unlock()
		if ok && time.Since(at) > age {
			out = append(out, fmt.Sprintf("%s(%s on connection %d subscription %s, returned at %v)",
				p.deliveryID, p.key, p.conn, p.sub, at.Sub(w.start).Round(time.Millisecond)))
		}
	}
	return out
}

func (a *simApp) missing(accepted []string) []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	var out []string
	for _, k := range accepted {
		if !a.returned[k] {
			out = append(out, k)
		}
	}
	return out
}

func (w *simWorld) verify() []simViolation {
	b, a := w.broker, w.app
	b.mu.Lock()
	defer b.mu.Unlock()
	a.mu.Lock()
	defer a.mu.Unlock()
	vs := slices.Clone(b.violations)
	add := func(prop, spec, format string, args ...any) {
		vs = append(vs, simViolation{prop, spec, fmt.Sprintf(format, args...)})
	}
	notBuffered := func(err error) bool {
		return err != nil && !errors.Is(err, stomp.ErrOutcomeUnknown) &&
			(errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) || errors.Is(err, stomp.ErrClosed))
	}
	for _, s := range a.sends {
		_, ok := b.accepted[s.key]
		switch {
		case s.err == nil && !ok:
			add("d", "SND-5", "Send %s returned nil but the broker never accepted it", s.key)
		case notBuffered(s.err) && ok:
			add("d", "SND-7", "Send %s returned %v, which means nothing was written, but the broker accepted it", s.key, s.err)
		}
	}
	for _, tx := range a.txs {
		var in, out []string
		for _, s := range tx.sends {
			if _, ok := b.accepted[s.key]; ok {
				in = append(in, s.key)
			} else {
				out = append(out, s.key)
			}
			if s.err != nil && slices.Contains(in, s.key) {
				add("f", "TX-2/TX-5", "%s: Tx.Send %s returned %v but the broker accepted it", tx.id, s.key, s.err)
			}
		}
		okSends := 0
		for _, s := range tx.sends {
			if s.err == nil {
				okSends++
			}
		}
		switch {
		case tx.abort && len(in) > 0:
			add("f", "TX-4", "%s: Abort returned %v but the broker accepted %v", tx.id, tx.err, in)
		case !tx.abort && tx.err == nil && len(in) != okSends:
			add("f", "TX-4", "%s: Commit returned nil but the broker accepted %v and not %v", tx.id, in, out)
		case !tx.abort && errors.Is(tx.err, stomp.ErrTxAborted) && len(in) > 0:
			add("f", "TX-5", "%s: Commit returned ErrTxAborted but the broker accepted %v", tx.id, in)
		case !tx.abort && tx.err != nil && len(in) > 0 && len(in) != okSends:
			add("f", "TX-6", "%s: Commit returned %v and the broker accepted only part of it: %v, not %v", tx.id, tx.err, in, out)
		}
	}
	return append(vs, a.violations...)
}

func (w *simWorld) report(t *testing.T, header string, faults []simFault, res simResult) {
	b, a := w.broker, w.app
	b.mu.Lock()
	st, accepted := b.stats, len(b.accepted)
	b.mu.Unlock()
	a.mu.Lock()
	summary := fmt.Sprintf("%v virtual; %d connections; %d sends, %d transactions, %d accepted; %d deliveries (%d redelivered), %d acked, %d nacked, %d truncated client frames; ReadMessage returned %d, %d stale acks, %d overflows, %d transactional acks",
		res.virtual.Round(time.Second), st.conns, len(a.sends), len(a.txs), accepted, st.deliveries, st.redeliveries, st.acks, st.nacks, st.truncated, a.nexts, a.staleAcks, a.overflows, a.txAcks)
	a.mu.Unlock()
	if !res.failed && !w.cfg.verbose {
		return
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "%s\n%s\nfault schedule:\n", header, summary)
	for _, f := range faults {
		fmt.Fprintf(&sb, "  %v\n", f)
	}
	w.logMu.Lock()
	lines := w.log
	if !w.cfg.verbose && len(lines) > 300 {
		lines = lines[len(lines)-300:]
		fmt.Fprintf(&sb, "event log (last 300 of %d):\n", len(w.log))
	} else {
		sb.WriteString("event log:\n")
	}
	for _, l := range lines {
		sb.WriteString("  " + l + "\n")
	}
	w.logMu.Unlock()
	t.Log(sb.String())
	if !res.failed {
		return
	}
	byProp := map[string]int{}
	for _, v := range res.violations {
		byProp[v.prop]++
		if byProp[v.prop] <= 5 {
			t.Errorf("seed %d: property (%s) %s violated: %s", w.cfg.seed, v.prop, v.spec, v.evidence)
		}
	}
	t.Errorf("seed %d failed: %v; rerun with STOMP_SIM_SEED=%d go test -run 'TestSimulation$' -race -count=5", w.cfg.seed, byProp, w.cfg.seed)
}

func simSeeds(t testing.TB) (seeds []uint64, verbose bool) {
	if s := os.Getenv("STOMP_SIM_SEED"); s != "" {
		n, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			t.Fatalf("STOMP_SIM_SEED: %v", err)
		}
		return []uint64{n}, true
	}
	count := uint64(100)
	if s := os.Getenv("STOMP_SIM_SEEDS"); s != "" {
		n, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			t.Fatalf("STOMP_SIM_SEEDS: %v", err)
		}
		count = n
	}
	for i := range count {
		seeds = append(seeds, i+1)
	}
	return seeds, false
}

func TestSimulation(t *testing.T) {
	seeds, verbose := simSeeds(t)
	var virtual atomic.Int64
	var failed atomic.Int64
	began := time.Now()
	t.Run("seed", func(t *testing.T) {
		for _, seed := range seeds {
			t.Run(strconv.FormatUint(seed, 10), func(t *testing.T) {
				t.Parallel()
				cfg := simConfig{seed: seed, intensity: 128, faultPhase: 2 * time.Minute, verbose: verbose}
				runSim(t, cfg, func(res simResult) {
					virtual.Add(int64(res.virtual))
					if res.failed {
						failed.Add(1)
					}
				})
			})
		}
	})
	t.Logf("%d seeds, %d failed, %v virtual time simulated in %v wall time",
		len(seeds), failed.Load(), time.Duration(virtual.Load()).Round(time.Second), time.Since(began).Round(time.Millisecond))
}

func FuzzSimulation(f *testing.F) {
	f.Add(uint64(1), uint8(128))
	f.Add(uint64(2), uint8(255))
	f.Add(uint64(3), uint8(30))
	f.Fuzz(func(t *testing.T, seed uint64, intensity uint8) {
		cfg := simConfig{seed: seed, intensity: intensity, faultPhase: time.Minute}
		runSim(t, cfg, func(simResult) {})
	})
}

// checkLiveness waits, after the heal, for the client to reconnect and for
// every accepted message to reach the application (properties h, i and k).
func (w *simWorld) checkLiveness(healAt time.Time) {
	c, a := w.client, w.app
	for c.State() != stomp.Connected {
		if time.Since(healAt) > 30*time.Second {
			a.violate("h", "RCN-1", "client state %v 30s after the heal", c.State())
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if time.Since(healAt) <= 30*time.Second {
		w.logf("connected %v after the heal", time.Since(healAt).Round(time.Millisecond))
	}

	deadline := healAt.Add(10 * time.Minute)
	lastAccepted, stableSince := -1, time.Now()
	for {
		accepted := w.acceptedKeys()
		missing := a.missing(accepted)
		if len(accepted) != lastAccepted {
			lastAccepted, stableSince = len(accepted), time.Now()
		}
		if len(missing) == 0 && time.Since(stableSince) >= 5*time.Second {
			break
		}
		if time.Now().After(deadline) {
			a.violate("i", "SUB-9/SUB-30/RCN-2", "%d of %d accepted messages never returned by ReadMessage 10 minutes after the heal, e.g. %v; broker: %s",
				len(missing), len(accepted), missing[:min(len(missing), 10)], w.broker.describe())
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	for start := time.Now(); ; {
		brokerSubs, clientSubs := w.broker.liveSubscriptions(), a.activeIDs()
		if slices.Equal(brokerSubs, clientSubs) {
			break
		}
		if time.Since(start) > 30*time.Second {
			a.violate("i", "RCN-2", "after the heal the broker has subscriptions %v, the client %v", brokerSubs, clientSubs)
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	for start := time.Now(); ; {
		stuck := w.unsettled(5 * time.Second)
		if len(stuck) == 0 {
			break
		}
		if time.Since(start) > 15*time.Second {
			a.violate("k", "SUB-19/SUB-28", "%d deliveries returned by ReadMessage were never acknowledged on their live connection, e.g. %v", len(stuck), stuck[:min(len(stuck), 5)])
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// acknowledgeTx acknowledges m in a transaction, reporting whether a Commit
// confirmed it. After an Abort or an unconfirmed Commit the caller still owes
// the broker an acknowledgement, as an application does.
func (w *simWorld) acknowledgeTx(ctx context.Context, m stomp.Message, r *rand.Rand) bool {
	a := w.app
	tx, err := w.client.Begin(ctx)
	a.check("Begin", "TX-1", err, stomp.ErrOutcomeUnknown, stomp.ErrClosed, errServerAllowed)
	if err != nil {
		return false
	}
	call, ack := "Tx.Ack", tx.Ack
	if r.IntN(4) == 0 {
		call, ack = "Tx.Nack", tx.Nack
	}
	err = ack(ctx, m)
	a.check(call, "TX-2/TX-5/SUB-21", err, stomp.ErrTxAborted, stomp.ErrStaleAck, stomp.ErrClosed)
	if r.IntN(4) == 0 {
		a.check("Abort", "TX-4/TX-5", tx.Abort(ctx), stomp.ErrOutcomeUnknown, stomp.ErrTxAborted, stomp.ErrClosed, errServerAllowed)
		return false
	}
	commit := tx.Commit(ctx)
	a.check("Commit", "TX-4/TX-5/TX-6", commit, stomp.ErrOutcomeUnknown, stomp.ErrTxAborted, stomp.ErrClosed, errServerAllowed)
	if err != nil || commit != nil {
		return false
	}
	a.mu.Lock()
	a.txAcks++
	a.mu.Unlock()
	return true
}
