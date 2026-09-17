package session

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/telegramdesktop/tproxy-server/internal/config"
	"github.com/telegramdesktop/tproxy-server/internal/frame"
)

// A client keeps several uplink batches in flight, and the relay answers each
// when it arrives rather than when it is applied. So a batch can be acknowledged
// while the batches before it are still being parsed: the client is entitled to
// dispatch the next one then, with the applied watermark behind it.
func TestAcceptedButUnappliedBatchesDoNotEndTheSession(t *testing.T) {
	manager, token, value := testSession(t)
	defer manager.Shutdown()

	window := value.limits.MaxPipelinedUpBatches
	keepAlive := frame.Encode(frame.Pong, 0, []byte("x"))

	// The later batches are parsed first, as happens when several arrive
	// together, so they park with the watermark where it started.
	for sequence := 2; sequence <= window; sequence++ {
		if _, err := value.ProcessUp(uint64(sequence), keepAlive); err != nil {
			t.Fatalf("batch %d was refused: %v", sequence, err)
		}
	}
	// Those arrivals were answered, so the client dispatches the next batch.
	if _, err := value.ProcessUp(uint64(window)+1, keepAlive); err != nil {
		t.Fatalf("a batch within the client's in-flight window was refused: %v", err)
	}
	if _, err := manager.Get(token); err != nil {
		t.Fatalf("the session was closed: %v", err)
	}
}

// The sequence bound has to stay finite for a client that sends cheap frames, so
// the reorder buffer is capped - and a client that reaches the cap is asked to
// slow down rather than having its session killed.
func TestTooManyParkedBatchesAreBackpressureNotAFault(t *testing.T) {
	manager, token, value := testSession(t)
	defer manager.Shutdown()

	window := value.limits.MaxPipelinedUpBatches
	keepAlive := frame.Encode(frame.Pong, 0, []byte("x"))

	var err error
	for sequence := 2; sequence <= 2*window+2; sequence++ {
		_, err = value.ProcessUp(uint64(sequence), keepAlive)
		if err != nil {
			break
		}
	}
	if !errors.Is(err, ErrBackpressure) {
		t.Fatalf("filling the reorder buffer gave %v, want ErrBackpressure", err)
	}
	if _, err := manager.Get(token); err != nil {
		t.Fatalf("filling the reorder buffer closed the session: %v", err)
	}
}

func TestSequenceRetryAndMismatch(t *testing.T) {
	manager, token, value := testSession(t)
	defer manager.Shutdown()

	first := frame.Encode(frame.Open, 1, nil)
	if ack, err := value.ProcessUp(1, first); err != nil || ack != 1 {
		t.Fatalf("first sequence failed: ack=%d error=%v", ack, err)
	}
	if ack, err := value.ProcessUp(1, append([]byte(nil), first...)); err != nil || ack != 1 {
		t.Fatalf("identical retry failed: ack=%d error=%v", ack, err)
	}
	if _, err := value.ProcessUp(1, frame.Encode(frame.Close, 1, nil)); !errors.Is(err, ErrProtocol) {
		t.Fatalf("changed retry was accepted: %v", err)
	}
	if _, err := manager.Get(token); err == nil {
		deadline := time.Now().Add(time.Second)
		for err == nil && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
			_, err = manager.Get(token)
		}
		if err == nil {
			t.Fatal("protocol-failed session token remained active")
		}
	}
}

func TestLaneSequencesAndReplayAreIndependent(t *testing.T) {
	configuration := testConfig("127.0.0.1:1")
	configuration.Profiles[0].CarrierMode = config.CarrierHTTPSLanes
	manager := NewManager(configuration, [32]byte{1})
	defer manager.Shutdown()
	bootstrap, err := manager.IssueBootstrap(
		&configuration.Profiles[0],
		"198.51.100.9")
	if err != nil {
		t.Fatal(err)
	}
	created, err := manager.Create(
		bootstrap,
		"198.51.100.9",
		frame.Encode(frame.Hello, 0, []byte{1}))
	if err != nil {
		t.Fatal(err)
	}
	first := frame.Encode(frame.Open, 51, nil)
	second := frame.Encode(frame.Open, 52, nil)
	if ack, err := created.Session.ProcessUpLane(51, 1, first); err != nil || ack != 1 {
		t.Fatalf("first lane failed: ack=%d error=%v", ack, err)
	}
	if ack, err := created.Session.ProcessUpLane(52, 1, second); err != nil || ack != 1 {
		t.Fatalf("second lane did not have an independent sequence: ack=%d error=%v", ack, err)
	}
	if ack, err := created.Session.ProcessUpLane(51, 1, append([]byte(nil), first...)); err != nil || ack != 1 {
		t.Fatalf("lane retry failed: ack=%d error=%v", ack, err)
	}
}

func TestLaneRejectsCrossStreamFrames(t *testing.T) {
	configuration := testConfig("127.0.0.1:1")
	configuration.Profiles[0].CarrierMode = config.CarrierHTTPSLanes
	manager := NewManager(configuration, [32]byte{1})
	defer manager.Shutdown()
	bootstrap, err := manager.IssueBootstrap(
		&configuration.Profiles[0],
		"198.51.100.9")
	if err != nil {
		t.Fatal(err)
	}
	created, err := manager.Create(
		bootstrap,
		"198.51.100.9",
		frame.Encode(frame.Hello, 0, []byte{1}))
	if err != nil {
		t.Fatal(err)
	}
	body := append(frame.Encode(frame.Open, 61, nil), frame.Encode(frame.Open, 62, nil)...)
	if _, err := created.Session.ProcessUpLane(61, 1, body); !errors.Is(err, ErrProtocol) {
		t.Fatalf("cross-stream lane body was accepted: %v", err)
	}
}

func TestClosedLaneReplaysDownlinkAndFinalUplink(t *testing.T) {
	configuration := testConfig("127.0.0.1:1")
	configuration.Profiles[0].CarrierMode = config.CarrierHTTPSLanes
	manager := NewManager(configuration, [32]byte{1})
	defer manager.Shutdown()
	bootstrap, err := manager.IssueBootstrap(
		&configuration.Profiles[0],
		"198.51.100.9")
	if err != nil {
		t.Fatal(err)
	}
	created, err := manager.Create(
		bootstrap,
		"198.51.100.9",
		frame.Encode(frame.Hello, 0, []byte{1}))
	if err != nil {
		t.Fatal(err)
	}
	laneID := uint32(71)
	opened := frame.Encode(frame.Open, laneID, nil)
	if _, err := created.Session.ProcessUpLane(laneID, 1, opened); err != nil {
		t.Fatal(err)
	}
	body, cursor, closed, err := created.Session.PollLane(
		context.Background(),
		laneID,
		0)
	if err != nil || closed || cursor != 1 {
		t.Fatalf("failed to receive lane close: cursor=%d closed=%v error=%v", cursor, closed, err)
	}
	frames, err := frame.ParseAll(body, frame.MaxPayload)
	if err != nil || len(frames) != 1 || frames[0].Type != frame.Close {
		t.Fatalf("unexpected lane close body: frames=%v error=%v", frames, err)
	}
	replayed, replayCursor, closed, err := created.Session.PollLane(
		context.Background(),
		laneID,
		0)
	if err != nil || closed || replayCursor != cursor || !bytes.Equal(replayed, body) {
		t.Fatal("lost lane downlink was not replayed byte-for-byte")
	}
	empty, finalCursor, closed, err := created.Session.PollLane(
		context.Background(),
		laneID,
		cursor)
	if err != nil || !closed || len(empty) != 0 || finalCursor != cursor {
		t.Fatalf("closed lane did not acknowledge its final cursor: cursor=%d closed=%v error=%v", finalCursor, closed, err)
	}
	if ack, err := created.Session.ProcessUpLane(laneID, 1, opened); err != nil || ack != 1 {
		t.Fatalf("closed lane could not replay its final uplink: ack=%d error=%v", ack, err)
	}
}

func TestRejectsStreamReuseAndWindowOverrun(t *testing.T) {
	manager, _, value := testSession(t)
	defer manager.Shutdown()

	batch := append(frame.Encode(frame.Open, 7, nil), frame.Encode(frame.Close, 7, nil)...)
	if _, err := value.ProcessUp(1, batch); err != nil {
		t.Fatal(err)
	}
	if _, err := value.ProcessUp(2, frame.Encode(frame.Open, 7, nil)); !errors.Is(err, ErrProtocol) {
		t.Fatalf("closed stream id was reused: %v", err)
	}

	manager2, _, value2 := testSession(t)
	defer manager2.Shutdown()
	tooLarge := bytes.Repeat([]byte{1}, frame.InitialStreamWindow+1)
	overrun := append(frame.Encode(frame.Open, 8, nil), frame.Encode(frame.Data, 8, tooLarge)...)
	if _, err := value2.ProcessUp(1, overrun); !errors.Is(err, ErrProtocol) {
		t.Fatalf("receive window overrun was accepted: %v", err)
	}
}

func TestPendingByteLimitIncludesBackendWrites(t *testing.T) {
	manager, token, value := testSession(t)
	defer manager.Shutdown()
	value.limits.MaxPendingPerSession = 4
	batch := append(frame.Encode(frame.Open, 9, nil), frame.Encode(frame.Data, 9, []byte("12345"))...)
	if _, err := value.ProcessUp(1, batch); !errors.Is(err, ErrBackpressure) {
		t.Fatalf("backend write exceeded the pending-byte limit: %v", err)
	}
	if value.lastUpSequence != 0 {
		t.Fatal("backpressured uplink committed its sequence")
	}
	if _, err := manager.Get(token); err != nil {
		t.Fatal("backpressured uplink closed its session")
	}
}

func TestProcessPendingByteLimitIncludesBackendWrites(t *testing.T) {
	manager, _, value := testSession(t)
	defer manager.Shutdown()
	manager.config.Limits.MaxPendingGlobal = 4
	batch := append(frame.Encode(frame.Open, 10, nil), frame.Encode(frame.Data, 10, []byte("12345"))...)
	if _, err := value.ProcessUp(1, batch); !errors.Is(err, ErrBackpressure) {
		t.Fatalf("backend write exceeded the process pending-byte limit: %v", err)
	}
}

func TestProcessPendingItemLimitIncludesBackendWrites(t *testing.T) {
	manager, _, value := testSession(t)
	defer manager.Shutdown()
	manager.config.Limits.MaxPendingItemsGlobal = 1
	batch := append(frame.Encode(frame.Open, 15, nil), frame.Encode(frame.Data, 15, []byte{1})...)
	batch = append(batch, frame.Encode(frame.Open, 16, nil)...)
	batch = append(batch, frame.Encode(frame.Data, 16, []byte{2})...)
	if _, err := value.ProcessUp(1, batch); !errors.Is(err, ErrBackpressure) {
		t.Fatalf("backend writes exceeded the process pending-item limit: %v", err)
	}
}

func TestPendingItemLimitIncludesTinyWrites(t *testing.T) {
	manager, _, value := testSession(t)
	defer manager.Shutdown()
	value.limits.MaxPendingItemsPerSession = 1
	batch := append(frame.Encode(frame.Open, 11, nil), frame.Encode(frame.Data, 11, []byte{1})...)
	batch = append(batch, frame.Encode(frame.Open, 12, nil)...)
	batch = append(batch, frame.Encode(frame.Data, 12, []byte{2})...)
	if _, err := value.ProcessUp(1, batch); !errors.Is(err, ErrBackpressure) {
		t.Fatalf("tiny writes exceeded the pending-item limit: %v", err)
	}
}

func TestControlFramesUseReservedQueueHeadroom(t *testing.T) {
	configuration := testConfig("127.0.0.1:1")
	configuration.Limits.MaxStreamsPerSession = 1
	configuration.Limits.MaxBodyBytes = 1024
	configuration.Limits.MaxPendingPerSession = 64 * 1024
	configuration.Limits.MaxPendingItemsPerSession = 256
	configuration.Limits.MaxPendingGlobal = configuration.Limits.MaxPendingPerSession
	configuration.Limits.MaxPendingItemsGlobal = configuration.Limits.MaxPendingItemsPerSession
	configuration.Limits.MaxSessionsGlobal = 1
	manager := NewManager(configuration, [32]byte{1})
	defer manager.Shutdown()
	value := newSession(sessionOptions{
		profile:  &configuration.Profiles[0],
		limits:   configuration.Limits,
		timeouts: configuration.Timeouts,
		budget:   manager.changePendingBudget,
	})
	defer value.Close()

	value.mu.Lock()
	costLimit, itemLimit := value.downlinkPendingLimits()
	if !value.reservePendingLocked(
		costLimit,
		itemLimit,
		pendingDownlink) {
		value.mu.Unlock()
		t.Fatal("could not fill the data portion of the queue")
	}
	if value.queueFrameLocked(frame.Data, 1, []byte{1}) {
		value.mu.Unlock()
		t.Fatal("DATA consumed reserved control headroom")
	}
	if !value.queueFrameLocked(frame.Window, 1, frame.WindowPayload(1)) ||
		!value.queueFrameLocked(frame.Close, 1, nil) {
		value.mu.Unlock()
		t.Fatal("control frame could not use reserved headroom")
	}
	closed := value.closed
	value.mu.Unlock()
	if closed {
		t.Fatal("control headroom exhaustion closed the session")
	}
}

func TestDownlinkBudgetPreservesOneUplinkBatch(t *testing.T) {
	configuration := testConfig("127.0.0.1:1")
	configuration.Limits.MaxStreamsPerSession = 1
	configuration.Limits.MaxBodyBytes = 1024
	configuration.Limits.MaxPendingPerSession = 64 * 1024
	configuration.Limits.MaxPendingItemsPerSession = 256
	value := newSession(sessionOptions{
		profile:  &configuration.Profiles[0],
		limits:   configuration.Limits,
		timeouts: configuration.Timeouts,
		budget:   func(int, int, pendingClass) bool { return true },
	})
	defer func() {
		value.Close()
		value.wait()
	}()

	value.mu.Lock()
	costLimit, itemLimit := value.downlinkPendingLimits()
	if !value.reservePendingLocked(
		costLimit,
		itemLimit,
		pendingDownlink) {
		value.mu.Unlock()
		t.Fatal("could not fill the downlink DATA partition")
	}
	value.mu.Unlock()

	body := append(
		frame.Encode(frame.Open, 21, nil),
		frame.Encode(frame.Data, 21, bytes.Repeat([]byte{1}, 512))...)
	ack, err := value.ProcessUp(1, body)
	if err != nil || ack != 1 {
		t.Fatalf("reserved uplink batch was rejected: ack=%d error=%v", ack, err)
	}
}

func TestWindowFramesCoalesceAcrossOtherControlFrames(t *testing.T) {
	configuration := testConfig("127.0.0.1:1")
	value := newSession(sessionOptions{
		profile:  &configuration.Profiles[0],
		limits:   configuration.Limits,
		timeouts: configuration.Timeouts,
		budget:   func(int, int, pendingClass) bool { return true },
	})
	defer value.Close()

	value.mu.Lock()
	queued := value.queueFrameLocked(frame.Window, 1, frame.WindowPayload(2))
	queued = queued && value.queueFrameLocked(frame.Close, 2, nil)
	queued = queued && value.queueFrameLocked(frame.Window, 1, frame.WindowPayload(3))
	count := len(value.pendingFrames)
	value.mu.Unlock()
	if !queued || count != 2 {
		t.Fatalf("WINDOW frames were not coalesced: queued=%v count=%d", queued, count)
	}

	body, _, err := value.Poll(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	frames, err := frame.ParseAll(body, frame.MaxPayload)
	if err != nil || len(frames) != 2 {
		t.Fatalf("could not parse coalesced controls: frames=%v error=%v", frames, err)
	}
	amount, err := frame.WindowAmount(frames[0].Payload)
	if err != nil || amount != 5 {
		t.Fatalf("coalesced WINDOW amount is %d: %v", amount, err)
	}
}

func TestDownlinkBudgetPausesBackendReads(t *testing.T) {
	configuration := testConfig("127.0.0.1:1")
	configuration.Limits.MaxStreamsPerSession = 1
	configuration.Limits.MaxBodyBytes = 1024
	configuration.Limits.MaxPendingPerSession = 64 * 1024
	configuration.Limits.MaxPendingItemsPerSession = 256
	value := newSession(sessionOptions{
		profile:  &configuration.Profiles[0],
		limits:   configuration.Limits,
		timeouts: configuration.Timeouts,
		budget:   func(int, int, pendingClass) bool { return true },
	})
	defer value.Close()
	backend := newBackendStream(value, 19, configuration.Profiles[0].Backend)
	value.streams[19] = &streamState{
		backend:      backend,
		sendCredit:   frame.InitialStreamWindow,
		creditNotify: make(chan struct{}, 1),
		writeNotify:  make(chan struct{}, 1),
	}

	value.mu.Lock()
	costLimit, itemLimit := value.downlinkPendingLimits()
	if !value.reservePendingLocked(
		costLimit,
		itemLimit,
		pendingDownlink) {
		value.mu.Unlock()
		t.Fatal("could not fill the data portion of the queue")
	}
	value.mu.Unlock()

	result := make(chan int, 1)
	go func() {
		allowance, ok := value.nextReadAllowance(19, backend.ctx.Done())
		if !ok {
			result <- -1
			return
		}
		result <- allowance
	}()
	select {
	case allowance := <-result:
		t.Fatalf("backend read was allowed against a full queue: %d", allowance)
	case <-time.After(20 * time.Millisecond):
	}

	value.mu.Lock()
	value.releasePendingLocked(1024, 1)
	value.mu.Unlock()
	select {
	case allowance := <-result:
		if allowance <= 0 || allowance > 1024-frame.HeaderSize-queueItemCost {
			t.Fatalf("invalid resumed allowance: %d", allowance)
		}
	case <-time.After(time.Second):
		t.Fatal("backend read did not resume after queue capacity returned")
	}
}

func TestDownlinkDataLimitDoesNotCloseSession(t *testing.T) {
	configuration := testConfig("127.0.0.1:1")
	configuration.Limits.MaxStreamsPerSession = 1
	configuration.Limits.MaxBodyBytes = 1024
	configuration.Limits.MaxPendingPerSession = 64 * 1024
	configuration.Limits.MaxPendingItemsPerSession = 256
	value := newSession(sessionOptions{
		profile:  &configuration.Profiles[0],
		limits:   configuration.Limits,
		timeouts: configuration.Timeouts,
		budget:   func(int, int, pendingClass) bool { return true },
	})
	defer value.Close()
	backend := newBackendStream(value, 20, configuration.Profiles[0].Backend)
	value.streams[20] = &streamState{
		backend:      backend,
		sendCredit:   frame.InitialStreamWindow,
		creditNotify: make(chan struct{}, 1),
		writeNotify:  make(chan struct{}, 1),
	}

	value.mu.Lock()
	costLimit, itemLimit := value.downlinkPendingLimits()
	if !value.reservePendingLocked(
		costLimit,
		itemLimit,
		pendingDownlink) {
		value.mu.Unlock()
		t.Fatal("could not fill the data portion of the queue")
	}
	value.mu.Unlock()
	if value.backendData(20, []byte{1}) {
		t.Fatal("backend DATA bypassed the data queue limit")
	}
	value.mu.Lock()
	closed := value.closed
	value.mu.Unlock()
	if closed {
		t.Fatal("one backpressured backend stream closed the session")
	}
}

func TestQueuedFramesChargeOverheadAndLimitBatchCount(t *testing.T) {
	value := config.Defaults()
	session := newSession(sessionOptions{
		profile:  &config.Profile{},
		limits:   value.Limits,
		timeouts: value.Timeouts,
		budget:   func(int, int, pendingClass) bool { return true },
	})
	session.mu.Lock()
	for id := uint32(1); id <= frame.MaxBatchFrames+7; id++ {
		if !session.queueFrameLocked(frame.Close, id, nil) {
			session.mu.Unlock()
			t.Fatal("could not queue bounded test frames")
		}
	}
	if session.pendingCost != (frame.MaxBatchFrames+7)*(frame.HeaderSize+queueItemCost) || session.pendingItems != frame.MaxBatchFrames+7 {
		session.mu.Unlock()
		t.Fatal("queued frame accounting omitted item overhead")
	}
	session.mu.Unlock()

	body, cursor, err := session.Poll(context.Background(), 0)
	if err != nil || cursor != 1 {
		t.Fatalf("poll failed: cursor=%d error=%v", cursor, err)
	}
	frames, err := frame.ParseAll(body, frame.MaxPayload)
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != frame.MaxBatchFrames {
		t.Fatalf("downlink returned %d frames", len(frames))
	}
	session.Close()
}

func TestConcurrentUplinkIsRejectedAndNewestPollWins(t *testing.T) {
	manager, _, value := testSession(t)
	defer manager.Shutdown()

	type pollResult struct {
		body   []byte
		cursor uint64
		err    error
	}
	first := make(chan pollResult, 1)
	go func() {
		body, cursor, err := value.Poll(context.Background(), 0)
		first <- pollResult{body, cursor, err}
	}()
	waitFor(t, func() bool {
		value.mu.Lock()
		defer value.mu.Unlock()
		return value.downActive
	})

	second := make(chan pollResult, 1)
	go func() {
		body, cursor, err := value.Poll(context.Background(), 0)
		second <- pollResult{body, cursor, err}
	}()
	select {
	case result := <-first:
		if result.err != nil || len(result.body) != 0 || result.cursor != 0 {
			t.Fatalf("superseded poll did not return an empty batch with its cursor: %+v", result)
		}
	case <-time.After(time.Second):
		t.Fatal("superseded poll was not released by the newer poll")
	}
	value.mu.Lock()
	queued := value.queueFrameLocked(frame.Window, 1, frame.WindowPayload(2))
	value.mu.Unlock()
	if !queued {
		t.Fatal("could not queue a downlink frame")
	}
	select {
	case result := <-second:
		if result.err != nil || len(result.body) == 0 || result.cursor != 1 {
			t.Fatalf("newest poll did not receive the downlink: %+v", result)
		}
	case <-time.After(time.Second):
		t.Fatal("newest poll did not observe the queued frame")
	}
	value.mu.Lock()
	closed := value.closed
	value.mu.Unlock()
	if closed {
		t.Fatal("a superseded poll closed the session")
	}

	value.SetUplinkSaturatedForTest(true)
	if _, err := value.ProcessUp(1, frame.Encode(frame.Open, 13, nil)); !errors.Is(err, ErrConcurrent) {
		t.Fatalf("concurrent uplink was accepted: %v", err)
	}
	value.SetUplinkSaturatedForTest(false)
}

func TestSessionCloseStopsBackendGoroutines(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		connection, acceptErr := listener.Accept()
		if acceptErr == nil {
			accepted <- connection
		}
	}()

	configuration := testConfig(listener.Addr().String())
	manager := NewManager(configuration, [32]byte{1})
	bootstrap, err := manager.IssueBootstrap(&configuration.Profiles[0], "198.51.100.10")
	if err != nil {
		t.Fatal(err)
	}
	created, err := manager.Create(bootstrap, "198.51.100.10", frame.Encode(frame.Hello, 0, []byte{1}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := created.Session.ProcessUp(1, frame.Encode(frame.Open, 14, nil)); err != nil {
		t.Fatal(err)
	}
	var peer net.Conn
	select {
	case peer = <-accepted:
		defer peer.Close()
	case <-time.After(time.Second):
		t.Fatal("backend connection was not established")
	}
	created.Session.mu.Lock()
	backend := created.Session.streams[14].backend
	created.Session.mu.Unlock()

	shutdown := make(chan struct{})
	go func() {
		manager.Shutdown()
		close(shutdown)
	}()
	select {
	case <-shutdown:
	case <-time.After(time.Second):
		t.Fatal("manager shutdown stranded a backend goroutine")
	}
	select {
	case <-backend.finished:
	default:
		t.Fatal("backend lifecycle was not complete when shutdown returned")
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if manager.pendingGlobalCost != 0 || manager.pendingGlobalItems != 0 {
		t.Fatal("shutdown retained pending queue budget")
	}
}

func TestBackendEOFClosesOnlyItsStream(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		connection, acceptErr := listener.Accept()
		if acceptErr == nil {
			_ = connection.Close()
		}
	}()

	configuration := testConfig(listener.Addr().String())
	manager := NewManager(configuration, [32]byte{1})
	defer manager.Shutdown()
	bootstrap, err := manager.IssueBootstrap(&configuration.Profiles[0], "198.51.100.15")
	if err != nil {
		t.Fatal(err)
	}
	created, err := manager.Create(bootstrap, "198.51.100.15", frame.Encode(frame.Hello, 0, []byte{1}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := created.Session.ProcessUp(1, frame.Encode(frame.Open, 17, nil)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		created.Session.mu.Lock()
		defer created.Session.mu.Unlock()
		_, live := created.Session.streams[17]
		return !live
	})
	body, _, err := created.Session.Poll(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	frames, err := frame.ParseAll(body, frame.MaxPayload)
	if err != nil || len(frames) != 1 || frames[0].Type != frame.Close || frames[0].StreamID != 17 {
		t.Fatalf("backend EOF did not produce one stream close: frames=%v error=%v", frames, err)
	}
	if _, err := manager.Get(created.Token); err != nil {
		t.Fatal("backend EOF closed the parent session")
	}
}

func TestStreamCancellationWakesZeroCreditWaiter(t *testing.T) {
	configuration := testConfig("127.0.0.1:1")
	value := newSession(sessionOptions{
		profile:  &configuration.Profiles[0],
		limits:   configuration.Limits,
		timeouts: configuration.Timeouts,
		budget:   func(int, int, pendingClass) bool { return true },
	})
	backend := newBackendStream(value, 18, configuration.Profiles[0].Backend)
	value.streams[18] = &streamState{
		backend:      backend,
		creditNotify: make(chan struct{}, 1),
		writeNotify:  make(chan struct{}, 1),
	}
	result := make(chan bool, 1)
	go func() {
		_, ok := value.nextReadAllowance(18, backend.ctx.Done())
		result <- ok
	}()
	backend.close()
	select {
	case ok := <-result:
		if ok {
			t.Fatal("zero-credit waiter resumed with allowance after close")
		}
	case <-time.After(time.Second):
		t.Fatal("zero-credit waiter remained blocked after stream cancellation")
	}
	value.Close()
}

func TestBootstrapLimitsExpiryAndConsumption(t *testing.T) {
	configuration := testConfig("127.0.0.1:1")
	configuration.Limits.MaxBootstrapsPerIP = 2
	configuration.Limits.MaxBootstrapsGlobal = 3
	configuration.Limits.NewBootstrapsBurst = 10
	configuration.Limits.NewBootstrapsPerMinute = 600
	manager := NewManager(configuration, [32]byte{1})
	defer manager.Shutdown()
	profile := &configuration.Profiles[0]
	first, err := manager.IssueBootstrap(profile, "198.51.100.11")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.IssueBootstrap(profile, "198.51.100.11"); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.IssueBootstrap(profile, "198.51.100.11"); !errors.Is(err, ErrLimit) {
		t.Fatalf("per-IP bootstrap limit was not enforced: %v", err)
	}
	if _, err := manager.Create(first, "198.51.100.11", frame.Encode(frame.Hello, 0, []byte{1})); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.IssueBootstrap(profile, "198.51.100.11"); err != nil {
		t.Fatalf("using a bootstrap did not release its per-IP slot: %v", err)
	}

	expiringConfig := testConfig("127.0.0.1:1")
	expiringConfig.Limits.MaxBootstrapsPerIP = 1
	expiringConfig.Timeouts.BootstrapLifetime = config.Duration(time.Millisecond)
	expiring := NewManager(expiringConfig, [32]byte{1})
	defer expiring.Shutdown()
	expired, err := expiring.IssueBootstrap(&expiringConfig.Profiles[0], "198.51.100.12")
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(3 * time.Millisecond)
	if _, err := expiring.IssueBootstrap(&expiringConfig.Profiles[0], "198.51.100.12"); err != nil {
		t.Fatalf("expired bootstrap retained its slot: %v", err)
	}
	if _, err := expiring.Create(expired, "198.51.100.12", frame.Encode(frame.Hello, 0, []byte{1})); !errors.Is(err, ErrAuthentication) {
		t.Fatalf("expired bootstrap was accepted: %v", err)
	}
}

func TestBootstrapSurvivesChangingClientAddress(t *testing.T) {
	configuration := testConfig("127.0.0.1:1")
	configuration.Limits.MaxBootstrapsPerIP = 1
	configuration.Limits.MaxSessionsPerIP = 1
	manager := NewManager(configuration, [32]byte{1})
	defer manager.Shutdown()
	profile := &configuration.Profiles[0]
	bootstrap, err := manager.IssueBootstrap(profile, "198.51.100.31")
	if err != nil {
		t.Fatal(err)
	}
	hello := frame.Encode(frame.Hello, 0, []byte{1})
	created, err := manager.Create(bootstrap, "198.51.100.32", hello)
	if err != nil {
		t.Fatalf("rotating egress rejected the bootstrap: %v", err)
	}
	retried, err := manager.Create(bootstrap, "198.51.100.33", hello)
	if err != nil {
		t.Fatalf("rotating egress rejected an identical retry: %v", err)
	}
	if retried.Token != created.Token || retried.Session != created.Session {
		t.Fatal("rotating-egress retry created a second session")
	}
	if _, err := manager.IssueBootstrap(profile, "198.51.100.31"); err != nil {
		t.Fatalf("consumption did not release the issuing address slot: %v", err)
	}
	second, err := manager.IssueBootstrap(profile, "198.51.100.34")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Create(second, "198.51.100.32", hello); !errors.Is(err, ErrLimit) {
		t.Fatalf("session was not accounted to its creation address: %v", err)
	}
	if _, err := manager.Create(second, "198.51.100.35", hello); err != nil {
		t.Fatalf("an address change could not recover from a per-IP limit: %v", err)
	}
}

func TestBootstrapRateIsGlobal(t *testing.T) {
	configuration := testConfig("127.0.0.1:1")
	configuration.Limits.MaxBootstrapsPerIP = 10
	configuration.Limits.MaxBootstrapsGlobal = 10
	configuration.Limits.NewBootstrapsBurst = 2
	configuration.Limits.NewBootstrapsPerMinute = 1
	manager := NewManager(configuration, [32]byte{1})
	defer manager.Shutdown()
	profile := &configuration.Profiles[0]
	if _, err := manager.IssueBootstrap(profile, "198.51.100.13"); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.IssueBootstrap(profile, "198.51.100.13"); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.IssueBootstrap(profile, "198.51.100.14"); !errors.Is(err, ErrLimit) {
		t.Fatalf("global bootstrap creation rate was not enforced: %v", err)
	}
}

func TestGlobalBootstrapPoolEvictsOldestUnusedEntry(t *testing.T) {
	configuration := testConfig("127.0.0.1:1")
	configuration.Limits.MaxBootstrapsGlobal = 2
	configuration.Limits.NewBootstrapsBurst = 10
	configuration.Limits.NewBootstrapsPerMinute = 600
	manager := NewManager(configuration, [32]byte{1})
	defer manager.Shutdown()
	profile := &configuration.Profiles[0]
	oldest, err := manager.IssueBootstrap(profile, "198.51.100.13")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.IssueBootstrap(profile, "198.51.100.13"); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.IssueBootstrap(profile, "198.51.100.14"); err != nil {
		t.Fatalf("global bootstrap pool did not evict an unused entry: %v", err)
	}
	if _, err := manager.Create(oldest, "198.51.100.13", frame.Encode(frame.Hello, 0, []byte{1})); !errors.Is(err, ErrAuthentication) {
		t.Fatalf("evicted bootstrap was accepted: %v", err)
	}
}

func TestDisabledPerIPSessionLimitAllowsSharedAddress(t *testing.T) {
	configuration := testConfig("127.0.0.1:1")
	configuration.Limits.MaxSessionsPerIP = 0
	configuration.Limits.MaxSessionsGlobal = 8
	configuration.Limits.NewSessionsBurst = 8
	configuration.Limits.NewSessionsPerMinute = 600
	manager := NewManager(configuration, [32]byte{1})
	defer manager.Shutdown()
	for index := 0; index != 5; index++ {
		bootstrap, err := manager.IssueBootstrap(
			&configuration.Profiles[0],
			"198.51.100.20")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := manager.Create(
			bootstrap,
			"198.51.100.20",
			frame.Encode(frame.Hello, 0, []byte{1})); err != nil {
			t.Fatalf("shared source address was limited at session %d: %v", index, err)
		}
	}
}

func TestProfileSessionLimitDoesNotConsumeGlobalCapacity(t *testing.T) {
	configuration := testConfig("127.0.0.1:1")
	configuration.Limits.MaxSessionsGlobal = 4
	configuration.Limits.NewSessionsBurst = 10
	configuration.Limits.NewSessionsPerMinute = 600
	configuration.Profiles[0].Limits.MaxSessions = 1
	secondSecret, _ := hex.DecodeString("101112131415161718191a1b1c1d1e1f")
	configuration.Profiles = append(configuration.Profiles, config.Profile{
		Name:       "second",
		Backend:    "127.0.0.1:1",
		Capability: config.DeriveCapability("proxy.example.com", "", secondSecret),
	})
	manager := NewManager(configuration, [32]byte{1})
	defer manager.Shutdown()
	hello := frame.Encode(frame.Hello, 0, []byte{1})
	firstBootstrap, err := manager.IssueBootstrap(
		&configuration.Profiles[0],
		"198.51.100.22")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Create(firstBootstrap, "198.51.100.22", hello); err != nil {
		t.Fatal(err)
	}
	limitedBootstrap, err := manager.IssueBootstrap(
		&configuration.Profiles[0],
		"198.51.100.23")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Create(limitedBootstrap, "198.51.100.23", hello); !errors.Is(err, ErrLimit) {
		t.Fatalf("profile session ceiling was not enforced: %v", err)
	}
	secondBootstrap, err := manager.IssueBootstrap(
		&configuration.Profiles[1],
		"198.51.100.24")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Create(secondBootstrap, "198.51.100.24", hello); err != nil {
		t.Fatalf("one profile's limit blocked another profile: %v", err)
	}
}

func TestSessionCreationRateIsGlobal(t *testing.T) {
	configuration := testConfig("127.0.0.1:1")
	configuration.Limits.NewSessionsBurst = 1
	configuration.Limits.NewSessionsPerMinute = 1
	manager := NewManager(configuration, [32]byte{1})
	defer manager.Shutdown()
	hello := frame.Encode(frame.Hello, 0, []byte{1})
	for index, clientIP := range []string{"198.51.100.25", "198.51.100.26"} {
		bootstrap, err := manager.IssueBootstrap(
			&configuration.Profiles[0],
			clientIP)
		if err != nil {
			t.Fatal(err)
		}
		_, err = manager.Create(bootstrap, clientIP, hello)
		if index == 0 && err != nil {
			t.Fatal(err)
		}
		if index == 1 && !errors.Is(err, ErrLimit) {
			t.Fatalf("global session creation rate was not enforced: %v", err)
		}
	}
}

func TestStreamLimitRejectsOnlyTheNewStream(t *testing.T) {
	configuration := testConfig("127.0.0.1:1")
	configuration.Limits.MaxStreamsGlobal = 1
	configuration.Limits.MaxBackendDialsInFlight = 1
	configuration.Limits.NewStreamsBurst = 10
	configuration.Limits.NewStreamsPerMinute = 600
	manager := NewManager(configuration, [32]byte{1})
	defer manager.Shutdown()
	bootstrap, err := manager.IssueBootstrap(
		&configuration.Profiles[0],
		"198.51.100.21")
	if err != nil {
		t.Fatal(err)
	}
	created, err := manager.Create(
		bootstrap,
		"198.51.100.21",
		frame.Encode(frame.Hello, 0, []byte{1}))
	if err != nil {
		t.Fatal(err)
	}
	body := append(
		frame.Encode(frame.Open, 30, nil),
		frame.Encode(frame.Open, 31, nil)...)
	if _, err := created.Session.ProcessUp(1, body); err != nil {
		t.Fatal(err)
	}
	down, _, err := created.Session.Poll(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	frames, err := frame.ParseAll(down, frame.MaxPayload)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, value := range frames {
		if value.Type == frame.Close && value.StreamID == 31 {
			found = true
		}
	}
	if !found {
		t.Fatalf("rejected stream did not receive CLOSE: %v", frames)
	}
	if _, err := manager.Get(created.Token); err != nil {
		t.Fatal("stream admission limit closed the parent session")
	}
	metrics := manager.Metrics()
	if metrics.StreamsOpened != 1 || metrics.StreamsRejected != 1 {
		t.Fatalf("unexpected stream metrics: %#v", metrics)
	}
}

func TestBackendDialLimitReopensAfterDialCompletes(t *testing.T) {
	configuration := testConfig("127.0.0.1:1")
	configuration.Limits.MaxStreamsGlobal = 2
	configuration.Limits.MaxBackendDialsInFlight = 1
	configuration.Limits.NewStreamsBurst = 10
	configuration.Limits.NewStreamsPerMinute = 600
	manager := NewManager(configuration, [32]byte{1})
	defer manager.Shutdown()
	profile := manager.MatchCapability(configuration.Profiles[0].Capability[:])
	if profile == nil || !manager.acquireStream(profile) {
		t.Fatal("first stream was not admitted")
	}
	if manager.acquireStream(profile) {
		t.Fatal("dial-in-flight ceiling admitted a second stream")
	}
	manager.backendDialFinished(profile, false)
	if !manager.acquireStream(profile) {
		t.Fatal("completed dial did not reopen dial capacity")
	}
	manager.backendDialFinished(profile, false)
	manager.streamFinished(profile)
	manager.streamFinished(profile)
}

func testSession(t *testing.T) (*Manager, string, *Session) {
	t.Helper()
	value := testConfig("127.0.0.1:1")
	manager := NewManager(value, [32]byte{1})
	bootstrap, err := manager.IssueBootstrap(&value.Profiles[0], "198.51.100.9")
	if err != nil {
		t.Fatal(err)
	}
	created, err := manager.Create(bootstrap, "198.51.100.9", frame.Encode(frame.Hello, 0, []byte{1}))
	if err != nil {
		t.Fatal(err)
	}
	return manager, created.Token, created.Session
}

func testConfig(backend string) config.Config {
	value := config.Defaults()
	value.Timeouts.BackendDial = config.Duration(20 * time.Millisecond)
	secret, _ := hex.DecodeString("000102030405060708090a0b0c0d0e0f")
	value.Profiles = []config.Profile{{
		Name:       "default",
		Backend:    backend,
		Capability: config.DeriveCapability("proxy.example.com", "", secret),
	}}
	return value
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition was not reached")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestEvictedLaneReleasesBudgetAndIgnoresLateFrames(t *testing.T) {
	configuration := testConfig("127.0.0.1:1")
	configuration.Profiles[0].CarrierMode = config.CarrierHTTPSLanes
	configuration.Limits.MaxClosedStreamIDs = 4
	manager := NewManager(configuration, [32]byte{1})
	defer manager.Shutdown()
	bootstrap, err := manager.IssueBootstrap(
		&configuration.Profiles[0],
		"198.51.100.9")
	if err != nil {
		t.Fatal(err)
	}
	created, err := manager.Create(
		bootstrap,
		"198.51.100.9",
		frame.Encode(frame.Hello, 0, []byte{1}))
	if err != nil {
		t.Fatal(err)
	}
	value := created.Session

	// Open more lanes than the tombstone capacity without ever polling them,
	// so each lane keeps its queued CLOSE (the backend refuses to dial) and
	// its budget charge until it is evicted.
	total := uint32(configuration.Limits.MaxClosedStreamIDs + 3)
	for id := uint32(1); id <= total; id++ {
		if _, err := value.ProcessUpLane(id, 1, frame.Encode(frame.Open, id, nil)); err != nil {
			t.Fatal(err)
		}
		waitFor(t, func() bool {
			value.mu.Lock()
			defer value.mu.Unlock()
			_, live := value.streams[id]
			_, closed := value.closedStreams[id]
			return !live && (closed || value.carrierLanes[id] == nil)
		})
	}
	value.mu.Lock()
	evictedLanes := 0
	for id := uint32(1); id <= total; id++ {
		if value.carrierLanes[id] == nil {
			evictedLanes++
		}
	}
	value.mu.Unlock()
	if evictedLanes == 0 {
		t.Fatal("no lane was evicted")
	}

	// Drain every surviving lane; afterwards nothing may remain charged even
	// though the evicted lanes were never polled.
	for id := uint32(1); id <= total; id++ {
		value.mu.Lock()
		lane := value.carrierLanes[id]
		value.mu.Unlock()
		if lane == nil {
			continue
		}
		cursor := uint64(0)
		for {
			body, next, closed, err := value.PollLane(context.Background(), id, cursor)
			if err != nil {
				t.Fatal(err)
			}
			if closed {
				break
			}
			if len(body) == 0 {
				t.Fatalf("lane %d returned an empty batch before closing", id)
			}
			cursor = next
		}
	}
	value.mu.Lock()
	pendingCost, pendingItems := value.pendingCost, value.pendingItems
	value.mu.Unlock()
	if pendingCost != 0 || pendingItems != 0 {
		t.Fatalf("session budget leaked after lane eviction: cost=%d items=%d", pendingCost, pendingItems)
	}
	capacity := manager.Capacity()
	if capacity.PendingBytes != 0 || capacity.PendingItems != 0 {
		t.Fatalf("global budget leaked after lane eviction: bytes=%d items=%d", capacity.PendingBytes, capacity.PendingItems)
	}

	late := frame.Encode(frame.Data, 1, []byte("late"))
	if ack, err := value.ProcessUpLane(1, 7, late); err != nil || ack != 7 {
		t.Fatalf("late DATA for an evicted lane was not ignored: ack=%d error=%v", ack, err)
	}
	value.mu.Lock()
	closed := value.closed
	value.mu.Unlock()
	if closed {
		t.Fatal("late DATA for an evicted lane closed the session")
	}
}

// acceptBackend returns the next backend connection the session dials, so a test
// can observe what the relay actually forwarded.
func acceptBackend(t *testing.T, listener net.Listener) net.Conn {
	t.Helper()
	accepted := make(chan net.Conn, 1)
	go func() {
		connection, err := listener.Accept()
		if err == nil {
			accepted <- connection
		}
	}()
	select {
	case connection := <-accepted:
		return connection
	case <-time.After(2 * time.Second):
		t.Fatal("backend connection was not established")
		return nil
	}
}

func readBackend(t *testing.T, connection net.Conn, want string) {
	t.Helper()
	_ = connection.SetReadDeadline(time.Now().Add(2 * time.Second))
	buffer := make([]byte, len(want))
	if _, err := io.ReadFull(connection, buffer); err != nil {
		t.Fatalf("backend read: %v", err)
	}
	if string(buffer) != want {
		t.Fatalf("backend received %q, want %q", buffer, want)
	}
}

func pipelinedWatermark(t *testing.T, value *Session) (uint64, int) {
	t.Helper()
	value.mu.Lock()
	defer value.mu.Unlock()
	return value.lastUpSequence, len(value.upPendingBatches)
}

// A batch that arrives before the batch it depends on must be held, not applied
// and not fatal: DATA for a stream whose OPEN is still in flight is only valid
// once that OPEN has landed.
func TestPipelinedOutOfOrderBatchesApplyInSequence(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	configuration := testConfig(listener.Addr().String())
	manager := NewManager(configuration, [32]byte{1})
	defer manager.Shutdown()
	bootstrap, err := manager.IssueBootstrap(&configuration.Profiles[0], "198.51.100.20")
	if err != nil {
		t.Fatal(err)
	}
	created, err := manager.Create(bootstrap, "198.51.100.20", frame.Encode(frame.Hello, 0, []byte{1}))
	if err != nil {
		t.Fatal(err)
	}

	opened := frame.Encode(frame.Open, 30, nil)
	payload := frame.Encode(frame.Data, 30, []byte("pipelined"))
	if ack, err := created.Session.ProcessUp(2, payload); err != nil || ack != 2 {
		t.Fatalf("ahead batch was not accepted: ack=%d error=%v", ack, err)
	}
	if watermark, buffered := pipelinedWatermark(t, created.Session); watermark != 0 || buffered != 1 {
		t.Fatalf("ahead batch was applied or dropped: watermark=%d buffered=%d", watermark, buffered)
	}
	if ack, err := created.Session.ProcessUp(1, opened); err != nil || ack != 1 {
		t.Fatalf("contiguous batch failed: ack=%d error=%v", ack, err)
	}
	if watermark, buffered := pipelinedWatermark(t, created.Session); watermark != 2 || buffered != 0 {
		t.Fatalf("reordered batches did not drain in sequence: watermark=%d buffered=%d", watermark, buffered)
	}
	if _, err := manager.Get(created.Token); err != nil {
		t.Fatal("out-of-order arrival closed the session")
	}

	peer := acceptBackend(t, listener)
	defer peer.Close()
	readBackend(t, peer, "pipelined")

	// The bytes reaching the backend queue a flow-control frame for the client.
	// Taking it and acknowledging it is what releases the last of the budget
	// reserved for the reordered batch.
	frameBody, cursor, err := created.Session.Poll(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	flowControl, err := frame.ParseAll(frameBody, frame.MaxPayload)
	if err != nil || len(flowControl) != 1 || flowControl[0].Type != frame.Window {
		t.Fatalf("backend write did not queue one window frame: frames=%v error=%v", flowControl, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, _, err := created.Session.Poll(ctx, cursor); err == nil {
		t.Fatal("expected the empty acknowledgment poll to wait out its context")
	}
	waitFor(t, func() bool {
		manager.mu.Lock()
		defer manager.mu.Unlock()
		return manager.pendingGlobalCost == 0 && manager.pendingGlobalItems == 0
	})
}

// A hole inside the window stops the drain until it is filled, after which the
// whole run is applied in order.
func TestPipelinedGapWithinWindowBuffersUntilFilled(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	configuration := testConfig(listener.Addr().String())
	manager := NewManager(configuration, [32]byte{1})
	defer manager.Shutdown()
	bootstrap, err := manager.IssueBootstrap(&configuration.Profiles[0], "198.51.100.21")
	if err != nil {
		t.Fatal(err)
	}
	created, err := manager.Create(bootstrap, "198.51.100.21", frame.Encode(frame.Hello, 0, []byte{1}))
	if err != nil {
		t.Fatal(err)
	}

	opened := frame.Encode(frame.Open, 31, nil)
	first := frame.Encode(frame.Data, 31, []byte("x"))
	second := frame.Encode(frame.Data, 31, []byte("y"))
	if ack, err := created.Session.ProcessUp(3, second); err != nil || ack != 3 {
		t.Fatalf("batch past a hole was not accepted: ack=%d error=%v", ack, err)
	}
	if ack, err := created.Session.ProcessUp(1, opened); err != nil || ack != 1 {
		t.Fatalf("opening batch failed: ack=%d error=%v", ack, err)
	}
	if watermark, buffered := pipelinedWatermark(t, created.Session); watermark != 1 || buffered != 1 {
		t.Fatalf("drain crossed a hole: watermark=%d buffered=%d", watermark, buffered)
	}
	if ack, err := created.Session.ProcessUp(2, first); err != nil || ack != 2 {
		t.Fatalf("filling batch failed: ack=%d error=%v", ack, err)
	}
	if watermark, buffered := pipelinedWatermark(t, created.Session); watermark != 3 || buffered != 0 {
		t.Fatalf("filled run did not drain: watermark=%d buffered=%d", watermark, buffered)
	}

	peer := acceptBackend(t, listener)
	defer peer.Close()
	readBackend(t, peer, "xy")
}

func TestPipelinedSequencePastWindowFailsSession(t *testing.T) {
	manager, token, value := testSession(t)
	defer manager.Shutdown()

	configuration := testConfig("127.0.0.1:1")
	window := configuration.Limits.MaxPipelinedUpBatches
	body := frame.Encode(frame.Open, 40, nil)
	if _, err := value.ProcessUp(uint64(window)+1, body); !errors.Is(err, ErrProtocol) {
		t.Fatalf("sequence past the window was accepted: %v", err)
	}
	waitFor(t, func() bool {
		_, err := manager.Get(token)
		return err != nil
	})
}

func TestPipelinedReplayUsesRecordedDigests(t *testing.T) {
	manager, _, value := testSession(t)
	defer manager.Shutdown()

	opened := frame.Encode(frame.Open, 41, nil)
	payload := frame.Encode(frame.Data, 41, []byte("z"))
	if _, err := value.ProcessUp(1, opened); err != nil {
		t.Fatal(err)
	}
	if _, err := value.ProcessUp(2, payload); err != nil {
		t.Fatal(err)
	}
	if ack, err := value.ProcessUp(1, append([]byte(nil), opened...)); err != nil || ack != 1 {
		t.Fatalf("replay of an applied batch was rejected: ack=%d error=%v", ack, err)
	}
	if _, err := value.ProcessUp(1, append([]byte(nil), payload...)); !errors.Is(err, ErrProtocol) {
		t.Fatalf("replayed sequence with different bytes was accepted: %v", err)
	}
}

func TestPipelinedBufferedResendMustMatch(t *testing.T) {
	manager, _, value := testSession(t)
	defer manager.Shutdown()

	if _, err := value.ProcessUp(2, frame.Encode(frame.Open, 42, nil)); err != nil {
		t.Fatal(err)
	}
	if ack, err := value.ProcessUp(2, frame.Encode(frame.Open, 42, nil)); err != nil || ack != 2 {
		t.Fatalf("identical resend of a buffered batch was rejected: ack=%d error=%v", ack, err)
	}
	if _, err := value.ProcessUp(2, frame.Encode(frame.Open, 43, nil)); !errors.Is(err, ErrProtocol) {
		t.Fatalf("buffered sequence resend with different bytes was accepted: %v", err)
	}
}

// The reorder buffer is bounded by the pending budget: a batch that does not fit
// is refused so the client retries, rather than being buffered in the hope that
// budget appears.
func TestPipelinedByteCapBackpressuresInsteadOfBuffering(t *testing.T) {
	manager, token, value := testSession(t)
	defer manager.Shutdown()

	value.limits.MaxPendingPerSession = 64
	value.limits.MaxPendingItemsPerSession = 4
	manager.mu.Lock()
	manager.config.Limits.MaxPendingGlobal = 64
	manager.config.Limits.MaxPendingItemsGlobal = 4
	manager.mu.Unlock()

	heavy := frame.Encode(frame.Data, 44, bytes.Repeat([]byte("a"), 4096))
	if _, err := value.ProcessUp(1, heavy); !errors.Is(err, ErrBackpressure) {
		t.Fatalf("oversized batch was not refused: %v", err)
	}
	if watermark, buffered := pipelinedWatermark(t, value); watermark != 0 || buffered != 0 {
		t.Fatalf("refused batch was buffered or applied: watermark=%d buffered=%d", watermark, buffered)
	}
	if _, err := manager.Get(token); err != nil {
		t.Fatal("backpressure closed the session")
	}
	capacity := manager.Capacity()
	if capacity.PendingBytes != 0 || capacity.PendingItems != 0 {
		t.Fatalf("refused batch retained budget: bytes=%d items=%d", capacity.PendingBytes, capacity.PendingItems)
	}
}
