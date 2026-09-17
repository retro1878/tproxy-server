package session

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"sync"
	"time"

	"github.com/telegramdesktop/tproxy-server/internal/config"
	"github.com/telegramdesktop/tproxy-server/internal/frame"
)

const (
	queueItemCost                = 256
	controlReserveExtraItems     = 16
	controlReserveItemsPerStream = 3
)

type pendingClass byte

const (
	pendingUplink pendingClass = iota
	pendingDownlink
	pendingControl
)

type sessionOptions struct {
	profile               *config.Profile
	clientIP              string
	limits                config.Limits
	timeouts              config.Timeouts
	budget                func(int, int, pendingClass) bool
	onFinished            func(*Session)
	acquireStream         func() bool
	onBackendDialFinished func(bool)
	onStreamFinished      func()
	onStreamRejected      func()
	onUp                  func(int)
	onDown                func(int)
}

type streamState struct {
	backend           *backendStream
	receiveWindow     uint32
	sendCredit        uint64
	bytesUp           uint64
	bytesDown         uint64
	pendingWriteBytes int
	pendingWriteCost  int
	pendingWriteItems int
	creditNotify      chan struct{}
	writes            [][]byte
	writeNotify       chan struct{}
}

type streamSnapshot struct {
	receiveWindow uint32
	sendCredit    uint64
}

type queuedFrame struct {
	encoded  []byte
	typeCode frame.Type
	streamID uint32
	cost     int
}

type downBatch struct {
	body  []byte
	cost  int
	items int
}

type pendingUpBatch struct {
	frames        []frame.Frame
	size          int
	digest        [sha256.Size]byte
	reservedCost  int
	reservedItems int
}

type carrierLane struct {
	lastUpSequence  uint64
	lastUpDigest    [sha256.Size]byte
	upActive        bool
	websocketActive bool
	pendingFrames   []queuedFrame
	pendingWindows  map[uint32]int
	unacked         []byte
	unackedCost     int
	unackedItems    int
	unackedBase     uint64
	downCursor      uint64
	downActive      bool
	superseded      chan struct{}
	notify          chan struct{}
}

func newCarrierLane() *carrierLane {
	return &carrierLane{
		pendingWindows: make(map[uint32]int),
		notify:         make(chan struct{}, 1),
	}
}

type Session struct {
	profile  *config.Profile
	clientIP string
	limits   config.Limits
	timeouts config.Timeouts
	budget   func(int, int, pendingClass) bool
	carrier  config.CarrierMode

	mu                    sync.Mutex
	streams               map[uint32]*streamState
	closedStreams         map[uint32]struct{}
	closedOrder           []uint32
	closedStart           int
	pendingFrames         []queuedFrame
	pendingWindows        map[uint32]int
	pendingCost           int
	pendingItems          int
	unacked               []byte
	unackedCost           int
	unackedItems          int
	unackedBase           uint64
	downCursor            uint64
	lastUpSequence        uint64
	lastUpAccepted        uint64
	upPendingBatches      map[uint64]*pendingUpBatch
	upAppliedDigests      map[uint64][sha256.Size]byte
	upAppliedOrder        []uint64
	upAppliedStart        int
	upParsing             int
	downActive            bool
	superseded            chan struct{}
	websocketActive       bool
	closed                bool
	lastActivity          time.Time
	notify                chan struct{}
	budgetNotify          chan struct{}
	done                  chan struct{}
	carrierLanes          map[uint32]*carrierLane
	finishOnce            sync.Once
	backendWG             sync.WaitGroup
	onFinished            func(*Session)
	acquireStream         func() bool
	onBackendDialFinished func(bool)
	onStreamFinished      func()
	onStreamRejected      func()
	onUp                  func(int)
	onDown                func(int)
}

func newSession(options sessionOptions) *Session {
	carrier := options.profile.CarrierMode.WithDefault()
	result := &Session{
		profile:               options.profile,
		clientIP:              options.clientIP,
		limits:                options.limits,
		timeouts:              options.timeouts,
		budget:                options.budget,
		carrier:               carrier,
		streams:               make(map[uint32]*streamState),
		closedStreams:         make(map[uint32]struct{}),
		pendingWindows:        make(map[uint32]int),
		upPendingBatches:      make(map[uint64]*pendingUpBatch),
		upAppliedDigests:      make(map[uint64][sha256.Size]byte),
		lastActivity:          time.Now(),
		notify:                make(chan struct{}, 1),
		budgetNotify:          make(chan struct{}),
		done:                  make(chan struct{}),
		carrierLanes:          make(map[uint32]*carrierLane),
		onFinished:            options.onFinished,
		acquireStream:         options.acquireStream,
		onBackendDialFinished: options.onBackendDialFinished,
		onStreamFinished:      options.onStreamFinished,
		onStreamRejected:      options.onStreamRejected,
		onUp:                  options.onUp,
		onDown:                options.onDown,
	}
	if carrier == config.CarrierHTTPSLanes {
		result.carrierLanes[0] = newCarrierLane()
	}
	return result
}

func (s *Session) CarrierMode() config.CarrierMode {
	return s.carrier
}

func (s *Session) AcquireWebSocket() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.carrier != config.CarrierWebSocket || s.websocketActive {
		return false
	}
	s.websocketActive = true
	return true
}

func (s *Session) AcquireWebSocketLane(laneID uint32) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed ||
		s.carrier != config.CarrierWebSocketLanes ||
		laneID == 0 ||
		laneID > frame.MaxStreamID {
		return false
	}
	if _, closed := s.closedStreams[laneID]; closed {
		return false
	}
	lane := s.carrierLanes[laneID]
	if lane == nil {
		if len(s.carrierLanes) >= s.limits.MaxStreamsPerSession {
			return false
		}
		lane = newCarrierLane()
		s.carrierLanes[laneID] = lane
	}
	if lane.websocketActive {
		return false
	}
	lane.websocketActive = true
	s.lastActivity = time.Now()
	return true
}

func (s *Session) ReleaseWebSocketLane(laneID uint32) {
	var backend *backendStream
	s.mu.Lock()
	if s.carrier != config.CarrierWebSocketLanes {
		s.mu.Unlock()
		return
	}
	lane := s.carrierLanes[laneID]
	if lane == nil {
		s.mu.Unlock()
		return
	}
	lane.websocketActive = false
	if state := s.streams[laneID]; state != nil {
		s.releaseStreamWritesLocked(state)
		delete(s.streams, laneID)
		s.rememberClosedLocked(laneID)
		backend = state.backend
	}
	s.releaseLaneLocked(lane)
	delete(s.carrierLanes, laneID)
	s.lastActivity = time.Now()
	s.mu.Unlock()
	if backend != nil {
		backend.close()
	}
}

// ProcessUp applies one uplink batch. A batch may arrive ahead of the applied
// watermark, because the client may keep several POSTs in flight; such a batch
// is parked, acknowledged, and applied once the batches before it have landed.
func (s *Session) ProcessUp(sequence uint64, body []byte) (uint64, error) {
	if s.usesCarrierLanes() {
		return 0, ErrProtocol
	}
	digest := sha256.Sum256(body)
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return 0, ErrClosed
	}
	s.lastActivity = time.Now()
	// A client may keep MaxPipelinedUpBatches uplink batches in flight, and each
	// is answered when it arrives rather than when it is applied, so the highest
	// sequence a client can legitimately send is measured from the highest one
	// already accepted. Measuring it from the applied watermark instead fails a
	// client that is behaving correctly: several batches are parsed at once, so
	// a late one can be acknowledged while the earlier ones are still parsing,
	// and the client - having been told it arrived - dispatches the next batch
	// with the watermark still behind it.
	if sequence == 0 || sequence > s.lastUpAccepted+uint64(s.limits.MaxPipelinedUpBatches) {
		ahead := s.lastUpAccepted + uint64(s.limits.MaxPipelinedUpBatches)
		s.mu.Unlock()
		s.protocolFailure(fmt.Sprintf("uplink sequence %d is beyond the window ending at %d", sequence, ahead))
		return 0, ErrProtocol
	}
	if sequence <= s.lastUpSequence {
		match := s.replayUpLocked(sequence, digest)
		s.mu.Unlock()
		if !match {
			s.protocolFailure(fmt.Sprintf("uplink sequence %d was resent with different bytes", sequence))
			return 0, ErrProtocol
		}
		return sequence, nil
	}
	if parked, ok := s.upPendingBatches[sequence]; ok {
		s.mu.Unlock()
		if !bytes.Equal(digest[:], parked.digest[:]) {
			s.protocolFailure(fmt.Sprintf("parked uplink sequence %d arrived with different bytes", sequence))
			return 0, ErrProtocol
		}
		return sequence, nil
	}
	if s.upParsing >= s.limits.MaxPipelinedUpBatches {
		s.mu.Unlock()
		return 0, ErrConcurrent
	}
	s.upParsing++
	s.mu.Unlock()

	frames, err := frame.ParseAll(body, s.limits.MaxFramePayload)
	if err == nil {
		for _, value := range frames {
			if shapeErr := frame.ValidateClientShape(value, s.allowsOpenPayload()); shapeErr != nil {
				err = shapeErr
				break
			}
		}
	}

	s.mu.Lock()
	s.upParsing--
	if s.closed {
		s.mu.Unlock()
		return 0, ErrClosed
	}
	if err != nil {
		s.mu.Unlock()
		s.protocolFailure(fmt.Sprintf("uplink batch %d is malformed: %v", sequence, err))
		return 0, ErrProtocol
	}
	// The watermark can move while the body is parsed, so an arrival is
	// classified again here rather than trusting the answer from before the
	// lock was dropped.
	if sequence <= s.lastUpSequence {
		match := s.replayUpLocked(sequence, digest)
		s.mu.Unlock()
		if !match {
			s.protocolFailure(fmt.Sprintf("uplink sequence %d was resent with different bytes", sequence))
			return 0, ErrProtocol
		}
		return sequence, nil
	}
	if parked, ok := s.upPendingBatches[sequence]; ok {
		s.mu.Unlock()
		if !bytes.Equal(digest[:], parked.digest[:]) {
			s.protocolFailure(fmt.Sprintf("parked uplink sequence %d arrived with different bytes", sequence))
			return 0, ErrProtocol
		}
		return sequence, nil
	}
	reservedCost, reservedItems := pendingUpReservation(frames)
	if (reservedCost != 0 || reservedItems != 0) &&
		!s.reservePendingLocked(reservedCost, reservedItems, pendingUplink) {
		s.mu.Unlock()
		return 0, ErrBackpressure
	}
	// What the sequence bound above allows has to be finite for a client that
	// sends cheap frames, so the reorder buffer is capped here. It holds at most
	// two windows: one that the client may have in flight, and one that may have
	// been accepted and not yet applied.
	if len(s.upPendingBatches) >= 2*s.limits.MaxPipelinedUpBatches {
		s.mu.Unlock()
		return 0, ErrBackpressure
	}
	s.upPendingBatches[sequence] = &pendingUpBatch{
		frames:        frames,
		size:          len(body),
		digest:        digest,
		reservedCost:  reservedCost,
		reservedItems: reservedItems,
	}
	if sequence > s.lastUpAccepted {
		s.lastUpAccepted = sequence
	}
	opened, closed, appliedBytes, drainErr := s.drainUpLocked()
	s.mu.Unlock()

	for _, value := range closed {
		value.close()
	}
	for _, value := range opened {
		go s.runBackend(value)
	}
	if drainErr != nil {
		if errors.Is(drainErr, ErrClosed) {
			s.Close()
		} else {
			s.protocolFailure(fmt.Sprintf("applying uplink batch %d failed: %v", sequence, drainErr))
		}
		return 0, drainErr
	}
	if s.onUp != nil && appliedBytes != 0 {
		s.onUp(appliedBytes)
	}
	return sequence, nil
}

// drainUpLocked applies every parked batch that has become contiguous with the
// watermark, in sequence order, and returns the streams the caller must open or
// close once the lock is released. Budget for the writes and the buffered bytes
// was reserved when the batch arrived, so the only failures here are fatal: an
// invalid batch in sequence context, or a session that closed underneath it.
//
// The caller holds s.mu.
func (s *Session) drainUpLocked() (
	[]*backendStream,
	[]*backendStream,
	int,
	error) {
	var opened, closed []*backendStream
	appliedBytes := 0
	for {
		next := s.lastUpSequence + 1
		batch := s.upPendingBatches[next]
		if batch == nil {
			return opened, closed, appliedBytes, nil
		}
		if !s.validateBatchLocked(batch.frames) {
			return opened, closed, appliedBytes, ErrProtocol
		}
		openedHere, closedHere, unusedCost, unusedItems, applied := s.applyBatchLocked(
			batch.frames,
			batch.reservedCost,
			batch.reservedItems)
		if unusedCost != 0 || unusedItems != 0 {
			s.releasePendingLocked(unusedCost, unusedItems)
		}
		delete(s.upPendingBatches, next)
		if !applied {
			return opened, closed, appliedBytes, ErrClosed
		}
		s.backendWG.Add(len(openedHere))
		opened = append(opened, openedHere...)
		closed = append(closed, closedHere...)
		s.lastUpSequence = next
		s.rememberUpDigestLocked(next, batch.digest)
		appliedBytes += batch.size
	}
}

// replayUpLocked reports whether an already-applied sequence carries the body it
// was applied with. A retry of a lost response is a no-op, but a resend of an
// applied sequence with different bytes is a protocol violation. The caller
// holds s.mu.
func (s *Session) replayUpLocked(sequence uint64, digest [sha256.Size]byte) bool {
	known, ok := s.upAppliedDigests[sequence]
	return ok && bytes.Equal(digest[:], known[:])
}

// rememberUpDigestLocked records the digest of an applied batch. Acks from
// pipelined batches can interleave, so the last batch alone is not enough: a
// retry may name any sequence still inside the window the client is allowed to
// resend.
func (s *Session) rememberUpDigestLocked(sequence uint64, digest [sha256.Size]byte) {
	s.upAppliedDigests[sequence] = digest
	s.upAppliedOrder = append(s.upAppliedOrder, sequence)
	for len(s.upAppliedOrder)-s.upAppliedStart > s.limits.MaxPipelinedUpBatches+1 {
		delete(s.upAppliedDigests, s.upAppliedOrder[s.upAppliedStart])
		s.upAppliedStart++
	}
	if s.upAppliedStart > 4096 && s.upAppliedStart*2 >= len(s.upAppliedOrder) {
		s.upAppliedOrder = append([]uint64(nil), s.upAppliedOrder[s.upAppliedStart:]...)
		s.upAppliedStart = 0
	}
}

// pendingUpReservation bounds the pending budget a batch can consume once it is
// applied. The exact cost depends on which streams are live at that point, so it
// is reserved when the batch arrives: over-reserving for a stream that has since
// closed costs some headroom, while under-reserving would leave an already
// acknowledged batch unable to land.
func pendingUpReservation(frames []frame.Frame) (int, int) {
	cost := 0
	items := 0
	for _, value := range frames {
		if value.StreamID != 0 && value.Type == frame.Data {
			cost += len(value.Payload) + queueItemCost
			items++
		}
	}
	return cost, items
}

func (s *Session) ProcessUpLane(laneID uint32, sequence uint64, body []byte) (uint64, error) {
	if !s.usesCarrierLanes() || laneID > frame.MaxStreamID {
		return 0, ErrProtocol
	}
	digest := sha256.Sum256(body)
	frames, err := frame.ParseAll(body, s.limits.MaxFramePayload)
	if err == nil {
		for _, value := range frames {
			if shapeErr := frame.ValidateClientShape(value, s.allowsOpenPayload()); shapeErr != nil || value.StreamID != laneID {
				err = ErrProtocol
				break
			}
		}
	}
	if err != nil {
		s.laneProtocolFailure(laneID, fmt.Sprintf("batch %d is malformed or belongs to another lane", sequence))
		return 0, ErrProtocol
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return 0, ErrClosed
	}
	lane := s.carrierLanes[laneID]
	if lane == nil {
		if laneID != 0 && len(frames) != 0 && frames[0].Type != frame.Open &&
			s.onlyLateFrames(frames) {
			// The lane's tombstone has already been evicted (or the client
			// remembers a close the relay no longer does): well-formed late
			// DATA, WINDOW or CLOSE frames are ignored and acknowledged, the
			// same as for a remembered tombstone, instead of killing the
			// session.
			s.lastActivity = time.Now()
			s.mu.Unlock()
			return sequence, nil
		}
		if laneID == 0 || len(frames) == 0 || frames[0].Type != frame.Open {
			s.mu.Unlock()
			s.laneProtocolFailure(laneID, "a lane must begin with OPEN")
			return 0, ErrProtocol
		}
		lane = newCarrierLane()
		s.carrierLanes[laneID] = lane
	}
	s.lastActivity = time.Now()
	if sequence == lane.lastUpSequence && sequence != 0 {
		match := bytes.Equal(digest[:], lane.lastUpDigest[:])
		s.mu.Unlock()
		if !match {
			s.laneProtocolFailure(laneID, fmt.Sprintf("sequence %d was resent with different bytes", sequence))
			return 0, ErrProtocol
		}
		return sequence, nil
	}
	if sequence != lane.lastUpSequence+1 || sequence == 0 {
		wanted := lane.lastUpSequence + 1
		s.mu.Unlock()
		s.laneProtocolFailure(laneID, fmt.Sprintf("sequence %d does not follow %d", sequence, wanted))
		return 0, ErrProtocol
	}
	if lane.upActive {
		s.mu.Unlock()
		return 0, ErrConcurrent
	}
	lane.upActive = true
	if !s.validateBatchLocked(frames) {
		lane.upActive = false
		s.mu.Unlock()
		s.laneProtocolFailure(laneID, fmt.Sprintf("batch %d is invalid for this lane's streams", sequence))
		return 0, ErrProtocol
	}
	reservedCost, reservedItems := s.backendWriteReservationLocked(frames)
	if (reservedCost != 0 || reservedItems != 0) &&
		!s.reservePendingLocked(reservedCost, reservedItems, pendingUplink) {
		lane.upActive = false
		s.mu.Unlock()
		return 0, ErrBackpressure
	}
	opened, closed, unusedCost, unusedItems, applied := s.applyBatchLocked(
		frames,
		reservedCost,
		reservedItems)
	if unusedCost != 0 || unusedItems != 0 {
		s.releasePendingLocked(unusedCost, unusedItems)
	}
	s.backendWG.Add(len(opened))
	lane.upActive = false
	if applied {
		lane.lastUpSequence = sequence
		lane.lastUpDigest = digest
	}
	s.mu.Unlock()

	for _, value := range closed {
		value.close()
	}
	for _, value := range opened {
		go s.runBackend(value)
	}
	if !applied {
		s.Close()
		return 0, ErrClosed
	}
	if s.onUp != nil {
		s.onUp(len(body))
	}
	return sequence, nil
}

func (s *Session) Poll(ctx context.Context, cursor uint64) ([]byte, uint64, error) {
	if s.usesCarrierLanes() {
		return nil, cursor, ErrProtocol
	}
	deadline := time.NewTimer(s.timeouts.LongPoll.Value())
	defer deadline.Stop()

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, cursor, ErrClosed
	}
	s.lastActivity = time.Now()
	if len(s.unacked) != 0 {
		if cursor == s.unackedBase {
			result := s.unacked
			next := s.downCursor
			s.mu.Unlock()
			return result, next, nil
		}
		if cursor != s.downCursor {
			wanted := s.downCursor
			s.mu.Unlock()
			s.protocolFailure(fmt.Sprintf("downlink cursor %d does not follow %d while a batch is outstanding", cursor, wanted))
			return nil, cursor, ErrProtocol
		}
		s.releasePendingLocked(s.unackedCost, s.unackedItems)
		s.unacked = nil
		s.unackedCost = 0
		s.unackedItems = 0
	} else if cursor != s.downCursor {
		wanted := s.downCursor
		s.mu.Unlock()
		s.protocolFailure(fmt.Sprintf("downlink cursor %d does not follow %d", cursor, wanted))
		return nil, cursor, ErrProtocol
	}
	// Newest poll wins: a poll arriving while another one is parked (its
	// connection most likely died silently) takes over, and the older waiter
	// returns an empty batch with its own cursor — harmless if that
	// connection is still alive, unobserved if it is dead — instead of the
	// newer poll being refused.
	if s.downActive && s.superseded != nil {
		close(s.superseded)
	}
	mine := make(chan struct{})
	s.superseded = mine
	s.downActive = true
	for {
		if s.superseded != mine {
			s.mu.Unlock()
			return nil, cursor, nil
		}
		if len(s.pendingFrames) != 0 {
			batch := s.takeDownBatchLocked()
			s.downCursor++
			s.unackedBase = cursor
			s.unacked = batch.body
			s.unackedCost = batch.cost
			s.unackedItems = batch.items
			s.downActive = false
			s.superseded = nil
			next := s.downCursor
			s.mu.Unlock()
			if s.onDown != nil {
				s.onDown(len(batch.body))
			}
			return batch.body, next, nil
		}
		if s.closed {
			s.downActive = false
			s.superseded = nil
			s.mu.Unlock()
			return nil, cursor, ErrClosed
		}
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			s.mu.Lock()
			if s.superseded == mine {
				s.downActive = false
				s.superseded = nil
			}
			s.mu.Unlock()
			return nil, cursor, ctx.Err()
		case <-mine:
			s.mu.Lock()
			signal(s.notify)
			s.mu.Unlock()
			return nil, cursor, nil
		case <-deadline.C:
			s.mu.Lock()
			if s.superseded != mine {
				signal(s.notify)
				s.mu.Unlock()
				return nil, cursor, nil
			}
			if len(s.pendingFrames) != 0 {
				continue
			}
			s.downActive = false
			s.superseded = nil
			s.lastActivity = time.Now()
			s.mu.Unlock()
			return nil, cursor, nil
		case <-s.notify:
			s.mu.Lock()
			if s.superseded != mine {
				// This wake-up belonged to the poll that took over.
				signal(s.notify)
			}
		}
	}
}

func (s *Session) PollLane(ctx context.Context, laneID uint32, cursor uint64) ([]byte, uint64, bool, error) {
	if !s.usesCarrierLanes() || laneID > frame.MaxStreamID {
		return nil, cursor, false, ErrProtocol
	}
	deadline := time.NewTimer(s.timeouts.LongPoll.Value())
	defer deadline.Stop()

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, cursor, false, ErrClosed
	}
	lane := s.carrierLanes[laneID]
	if lane == nil {
		s.mu.Unlock()
		return nil, cursor, false, ErrProtocol
	}
	s.lastActivity = time.Now()
	if len(lane.unacked) != 0 {
		if cursor == lane.unackedBase {
			result := lane.unacked
			next := lane.downCursor
			s.mu.Unlock()
			return result, next, false, nil
		}
		if cursor != lane.downCursor {
			wanted := lane.downCursor
			s.mu.Unlock()
			s.laneProtocolFailure(laneID, fmt.Sprintf("downlink cursor %d does not follow %d while a batch is outstanding", cursor, wanted))
			return nil, cursor, false, ErrProtocol
		}
		s.releasePendingLocked(lane.unackedCost, lane.unackedItems)
		lane.unacked = nil
		lane.unackedCost = 0
		lane.unackedItems = 0
	} else if cursor != lane.downCursor {
		wanted := lane.downCursor
		s.mu.Unlock()
		s.laneProtocolFailure(laneID, fmt.Sprintf("downlink cursor %d does not follow %d", cursor, wanted))
		return nil, cursor, false, ErrProtocol
	}
	if lane.downActive && lane.superseded != nil {
		close(lane.superseded)
	}
	mine := make(chan struct{})
	lane.superseded = mine
	lane.downActive = true
	for {
		if lane.superseded != mine {
			s.mu.Unlock()
			return nil, cursor, false, nil
		}
		if len(lane.pendingFrames) != 0 {
			batch := s.takeLaneDownBatchLocked(lane)
			lane.downCursor++
			lane.unackedBase = cursor
			lane.unacked = batch.body
			lane.unackedCost = batch.cost
			lane.unackedItems = batch.items
			lane.downActive = false
			lane.superseded = nil
			next := lane.downCursor
			s.mu.Unlock()
			if s.onDown != nil {
				s.onDown(len(batch.body))
			}
			return batch.body, next, false, nil
		}
		if s.closed {
			lane.downActive = false
			lane.superseded = nil
			s.mu.Unlock()
			return nil, cursor, false, ErrClosed
		}
		if laneID != 0 {
			_, live := s.streams[laneID]
			_, closed := s.closedStreams[laneID]
			if !live && closed {
				lane.downActive = false
				lane.superseded = nil
				s.mu.Unlock()
				return nil, cursor, true, nil
			}
		}
		if s.carrierLanes[laneID] != lane {
			lane.downActive = false
			lane.superseded = nil
			s.mu.Unlock()
			return nil, cursor, true, nil
		}
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			s.mu.Lock()
			if lane.superseded == mine {
				lane.downActive = false
				lane.superseded = nil
			}
			s.mu.Unlock()
			return nil, cursor, false, ctx.Err()
		case <-mine:
			s.mu.Lock()
			signal(lane.notify)
			s.mu.Unlock()
			return nil, cursor, false, nil
		case <-s.done:
			s.mu.Lock()
			if lane.superseded == mine {
				lane.downActive = false
				lane.superseded = nil
			}
			s.mu.Unlock()
			return nil, cursor, false, ErrClosed
		case <-deadline.C:
			s.mu.Lock()
			if lane.superseded != mine {
				signal(lane.notify)
				s.mu.Unlock()
				return nil, cursor, false, nil
			}
			if len(lane.pendingFrames) != 0 {
				continue
			}
			lane.downActive = false
			lane.superseded = nil
			s.lastActivity = time.Now()
			s.mu.Unlock()
			return nil, cursor, false, nil
		case <-lane.notify:
			s.mu.Lock()
			if lane.superseded != mine {
				signal(lane.notify)
			}
		}
	}
}

func (s *Session) LastActivity() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastActivity
}

func (s *Session) Close() {
	s.mu.Lock()
	s.closeLocked()
	s.mu.Unlock()
}

func (s *Session) wait() {
	s.backendWG.Wait()
}

func (s *Session) onlyLateFrames(values []frame.Frame) bool {
	for _, value := range values {
		switch value.Type {
		case frame.Data, frame.Window, frame.Close:
		default:
			return false
		}
	}
	return len(values) != 0
}

func (s *Session) validateBatchLocked(values []frame.Frame) bool {
	live := make(map[uint32]streamSnapshot, len(s.streams))
	for id, value := range s.streams {
		live[id] = streamSnapshot{
			receiveWindow: value.receiveWindow,
			sendCredit:    value.sendCredit,
		}
	}
	closed := make(map[uint32]struct{})
	for _, value := range values {
		if value.StreamID == 0 {
			if value.Type != frame.Pong {
				return false
			}
			continue
		}
		state, exists := live[value.StreamID]
		_, wasClosedBefore := s.closedStreams[value.StreamID]
		_, closedInBatch := closed[value.StreamID]
		wasClosed := wasClosedBefore || closedInBatch
		switch value.Type {
		case frame.Open:
			if exists || wasClosed {
				return false
			}
			live[value.StreamID] = streamSnapshot{
				receiveWindow: frame.InitialStreamWindow,
				sendCredit:    frame.InitialStreamWindow,
			}
		case frame.Data:
			if wasClosed {
				continue
			}
			if !exists || uint32(len(value.Payload)) > state.receiveWindow {
				return false
			}
			state.receiveWindow -= uint32(len(value.Payload))
			live[value.StreamID] = state
		case frame.Window:
			if wasClosed {
				continue
			}
			if !exists {
				return false
			}
			amount, _ := frame.WindowAmount(value.Payload)
			state.sendCredit += uint64(amount)
			if state.sendCredit > math.MaxUint32 {
				state.sendCredit = math.MaxUint32
			}
			live[value.StreamID] = state
		case frame.Close:
			if wasClosed {
				continue
			}
			if !exists {
				return false
			}
			delete(live, value.StreamID)
			closed[value.StreamID] = struct{}{}
		default:
			return false
		}
	}
	return true
}

func (s *Session) backendWriteReservationLocked(
	values []frame.Frame) (int, int) {
	live := make(map[uint32]bool, len(s.streams))
	for id := range s.streams {
		live[id] = true
	}
	cost := 0
	items := 0
	for _, value := range values {
		if value.StreamID == 0 {
			continue
		}
		switch value.Type {
		case frame.Open:
			live[value.StreamID] = true
		case frame.Data:
			if live[value.StreamID] {
				cost += len(value.Payload) + queueItemCost
				items++
			}
		case frame.Close:
			delete(live, value.StreamID)
		}
	}
	return cost, items
}

func (s *Session) applyBatchLocked(
	values []frame.Frame,
	reservedCost int,
	reservedItems int) (
	[]*backendStream,
	[]*backendStream,
	int,
	int,
	bool) {
	opened := make([]*backendStream, 0)
	closed := make([]*backendStream, 0)
	for _, value := range values {
		if value.StreamID == 0 {
			continue
		}
		state := s.streams[value.StreamID]
		_, wasClosed := s.closedStreams[value.StreamID]
		switch value.Type {
		case frame.Open:
			if len(s.streams) >= s.limits.MaxStreamsPerSession ||
				(s.acquireStream != nil && !s.acquireStream()) {
				if !s.rejectStreamLocked(value.StreamID) {
					return opened, closed, reservedCost, reservedItems, false
				}
				continue
			}
			address := s.profile.Backend
			if s.allowsOpenPayload() {
				destination, err := parseTunnelDestination(value.Payload)
				if err != nil {
					if !s.rejectStreamLocked(value.StreamID) {
						return opened, closed, reservedCost, reservedItems, false
					}
					continue
				}
				address = destination
			}
			backend := newBackendStream(s, value.StreamID, address)
			state = &streamState{
				backend:       backend,
				receiveWindow: frame.InitialStreamWindow,
				sendCredit:    frame.InitialStreamWindow,
				creditNotify:  make(chan struct{}, 1),
				writeNotify:   make(chan struct{}, 1),
			}
			s.streams[value.StreamID] = state
			opened = append(opened, backend)
		case frame.Data:
			if wasClosed {
				continue
			}
			cost, items := s.appendBackendWriteLocked(state, value.Payload)
			reservedCost -= cost
			reservedItems -= items
			state.receiveWindow -= uint32(len(value.Payload))
			state.bytesUp += uint64(len(value.Payload))
			signal(state.writeNotify)
		case frame.Window:
			if wasClosed {
				continue
			}
			amount, _ := frame.WindowAmount(value.Payload)
			state.sendCredit += uint64(amount)
			if state.sendCredit > math.MaxUint32 {
				state.sendCredit = math.MaxUint32
			}
			signal(state.creditNotify)
		case frame.Close:
			if wasClosed {
				continue
			}
			log.Printf("stream %d closed by the client after %d bytes up and %d down",
				value.StreamID, state.bytesUp, state.bytesDown)
			s.releaseStreamWritesLocked(state)
			delete(s.streams, value.StreamID)
			s.rememberClosedLocked(value.StreamID)
			closed = append(closed, state.backend)
		}
	}
	return opened, closed, reservedCost, reservedItems, true
}

// rejectStreamLocked refuses one stream without disturbing the session: the id
// becomes a tombstone and the client is told to close it. A limit and a refused
// tunnel destination share this path. It reports whether the CLOSE frame could
// be queued.
func (s *Session) rejectStreamLocked(id uint32) bool {
	s.rememberClosedLocked(id)
	if s.onStreamRejected != nil {
		s.onStreamRejected()
	}
	return s.queueFrameLocked(frame.Close, id, nil)
}

func (s *Session) appendBackendWriteLocked(
	state *streamState,
	payload []byte) (int, int) {
	cost := len(payload) + queueItemCost
	items := 1
	coalesce := len(state.writes) != 0 && len(state.writes[len(state.writes)-1])+len(payload) <= frame.DataChunk
	if coalesce {
		cost = len(payload)
		items = 0
	}
	if coalesce {
		last := len(state.writes) - 1
		state.writes[last] = append(state.writes[last], payload...)
	} else {
		state.writes = append(state.writes, append([]byte(nil), payload...))
	}
	state.pendingWriteBytes += len(payload)
	state.pendingWriteCost += cost
	state.pendingWriteItems += items
	return cost, items
}

func (s *Session) nextWrite(id uint32, done <-chan struct{}) ([]byte, bool) {
	for {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return nil, false
		}
		state := s.streams[id]
		if state == nil {
			s.mu.Unlock()
			return nil, false
		}
		if len(state.writes) != 0 {
			result := state.writes[0]
			state.writes[0] = nil
			state.writes = state.writes[1:]
			s.mu.Unlock()
			return result, true
		}
		notify := state.writeNotify
		s.mu.Unlock()
		select {
		case <-notify:
		case <-s.done:
			return nil, false
		case <-done:
			return nil, false
		}
	}
}

func (s *Session) backendDrained(id uint32, amount int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.streams[id]
	if s.closed || state == nil || amount <= 0 || amount > state.pendingWriteBytes || amount > state.pendingWriteCost || uint64(state.receiveWindow)+uint64(amount) > frame.InitialStreamWindow {
		return false
	}
	state.pendingWriteBytes -= amount
	state.pendingWriteCost -= amount
	s.releasePendingLocked(amount, 0)
	state.receiveWindow += uint32(amount)
	if !s.queueFrameLocked(frame.Window, id, frame.WindowPayload(uint32(amount))) {
		return false
	}
	return true
}

func (s *Session) backendWriteFinished(id uint32) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.streams[id]
	if s.closed || state == nil || state.pendingWriteItems == 0 || state.pendingWriteCost < queueItemCost {
		return false
	}
	state.pendingWriteItems--
	state.pendingWriteCost -= queueItemCost
	s.releasePendingLocked(queueItemCost, 1)
	return true
}

func (s *Session) nextReadAllowance(id uint32, done <-chan struct{}) (int, bool) {
	for {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return 0, false
		}
		state := s.streams[id]
		if state == nil {
			s.mu.Unlock()
			return 0, false
		}
		if state.sendCredit != 0 {
			result := int(state.sendCredit)
			if result > frame.DataChunk {
				result = frame.DataChunk
			}
			result = s.dataFrameAllowanceLocked(result)
			if result != 0 {
				s.mu.Unlock()
				return result, true
			}
		}
		notify := state.creditNotify
		budgetNotify := s.budgetNotify
		s.mu.Unlock()
		select {
		case <-notify:
		case <-budgetNotify:
		case <-s.done:
			return 0, false
		case <-done:
			return 0, false
		}
	}
}

func (s *Session) backendData(id uint32, data []byte) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.streams[id]
	if s.closed || state == nil || len(data) == 0 || uint64(len(data)) > state.sendCredit {
		return false
	}
	state.sendCredit -= uint64(len(data))
	state.bytesDown += uint64(len(data))
	if !s.queueFrameLocked(frame.Data, id, data) {
		return false
	}
	return true
}

func (s *Session) backendClosed(id uint32, backend *backendStream) {
	var up, down uint64
	s.mu.Lock()
	state := s.streams[id]
	if !s.closed && state != nil && state.backend == backend {
		up, down = state.bytesUp, state.bytesDown
		s.releaseStreamWritesLocked(state)
		delete(s.streams, id)
		s.rememberClosedLocked(id)
		if !s.queueFrameLocked(frame.Close, id, nil) {
			s.closeLocked()
		}
	}
	s.mu.Unlock()
	// A stream the client closed is already gone from the table, so reaching
	// here with a reason means the far end ended it. That is otherwise entirely
	// invisible: the client is told only that its stream closed.
	if backend.endReason != "" {
		log.Printf("stream %d closed after %d bytes up and %d down: %s",
			id, up, down, backend.endReason)
	}
	backend.close()
}

func (s *Session) queueFrameLocked(t frame.Type, id uint32, payload []byte) bool {
	if s.usesCarrierLanes() {
		return s.queueLaneFrameLocked(t, id, payload)
	}
	if t == frame.Window {
		if index, exists := s.pendingWindows[id]; exists {
			queued := &s.pendingFrames[index]
			previous, _ := frame.WindowAmount(
				queued.encoded[frame.HeaderSize:])
			amount, _ := frame.WindowAmount(payload)
			total := uint64(previous) + uint64(amount)
			if total <= math.MaxUint32 {
				binary.BigEndian.PutUint32(
					queued.encoded[frame.HeaderSize:],
					uint32(total))
				signal(s.notify)
				return true
			}
		}
	}
	if len(s.pendingFrames) != 0 {
		last := &s.pendingFrames[len(s.pendingFrames)-1]
		if last.typeCode == frame.Data && t == frame.Data && last.streamID == id && len(last.encoded)-frame.HeaderSize+len(payload) <= s.limits.MaxFramePayload {
			if !s.reservePendingLocked(
				len(payload),
				0,
				pendingDownlink) {
				return false
			}
			last.encoded = append(last.encoded, payload...)
			last.cost += len(payload)
			binary.BigEndian.PutUint32(last.encoded[4:8], uint32(len(last.encoded)-frame.HeaderSize))
			signal(s.notify)
			return true
		}
	}
	encoded := frame.Encode(t, id, payload)
	cost := len(encoded) + queueItemCost
	class := pendingControl
	if t == frame.Data {
		class = pendingDownlink
	}
	if !s.reservePendingLocked(cost, 1, class) {
		return false
	}
	s.pendingFrames = append(s.pendingFrames, queuedFrame{
		encoded:  encoded,
		typeCode: t,
		streamID: id,
		cost:     cost,
	})
	if t == frame.Window {
		s.pendingWindows[id] = len(s.pendingFrames) - 1
	}
	signal(s.notify)
	return true
}

func (s *Session) usesCarrierLanes() bool {
	return s.carrier == config.CarrierHTTPSLanes ||
		s.carrier == config.CarrierWebSocketLanes
}

func (s *Session) queueLaneFrameLocked(t frame.Type, id uint32, payload []byte) bool {
	lane := s.carrierLanes[id]
	if lane == nil {
		return false
	}
	if t == frame.Window {
		if index, exists := lane.pendingWindows[id]; exists {
			queued := &lane.pendingFrames[index]
			previous, _ := frame.WindowAmount(
				queued.encoded[frame.HeaderSize:])
			amount, _ := frame.WindowAmount(payload)
			total := uint64(previous) + uint64(amount)
			if total <= math.MaxUint32 {
				binary.BigEndian.PutUint32(
					queued.encoded[frame.HeaderSize:],
					uint32(total))
				signal(lane.notify)
				return true
			}
		}
	}
	if len(lane.pendingFrames) != 0 {
		last := &lane.pendingFrames[len(lane.pendingFrames)-1]
		if last.typeCode == frame.Data && t == frame.Data && last.streamID == id && len(last.encoded)-frame.HeaderSize+len(payload) <= s.limits.MaxFramePayload {
			if !s.reservePendingLocked(
				len(payload),
				0,
				pendingDownlink) {
				return false
			}
			last.encoded = append(last.encoded, payload...)
			last.cost += len(payload)
			binary.BigEndian.PutUint32(last.encoded[4:8], uint32(len(last.encoded)-frame.HeaderSize))
			signal(lane.notify)
			return true
		}
	}
	encoded := frame.Encode(t, id, payload)
	cost := len(encoded) + queueItemCost
	class := pendingControl
	if t == frame.Data {
		class = pendingDownlink
	}
	if !s.reservePendingLocked(cost, 1, class) {
		return false
	}
	lane.pendingFrames = append(lane.pendingFrames, queuedFrame{
		encoded:  encoded,
		typeCode: t,
		streamID: id,
		cost:     cost,
	})
	if t == frame.Window {
		lane.pendingWindows[id] = len(lane.pendingFrames) - 1
	}
	signal(lane.notify)
	return true
}

// ValidateBudget rejects a configuration whose per-session control reserve,
// multiplied across every session, would leave the global data budget at zero
// (bytes or items) — a silent footgun that would starve every data frame.
func ValidateBudget(cfg config.Config) error {
	reserveCost, reserveItems := pendingControlReserve(cfg.Limits)
	sessions := cfg.Limits.MaxSessionsGlobal
	if reserveCost > cfg.Limits.MaxPendingGlobal/sessions {
		return errors.New("per-session control reserve times max_sessions_global exhausts max_pending_global")
	}
	if reserveItems > cfg.Limits.MaxPendingItemsGlobal/sessions {
		return errors.New("per-session control reserve times max_sessions_global exhausts max_pending_items_global")
	}
	return nil
}

func pendingControlReserve(limits config.Limits) (int, int) {
	items := controlReserveExtraItems
	if limits.MaxStreamsPerSession > (math.MaxInt-items)/controlReserveItemsPerStream {
		return limits.MaxPendingPerSession, limits.MaxPendingItemsPerSession
	}
	items += limits.MaxStreamsPerSession * controlReserveItemsPerStream
	costPerItem := queueItemCost + frame.HeaderSize + 4
	if items > math.MaxInt/costPerItem {
		return limits.MaxPendingPerSession, limits.MaxPendingItemsPerSession
	}
	return items * costPerItem, items
}

func pendingUplinkReserve(limits config.Limits) (int, int) {
	items := limits.MaxBodyBytes / frame.HeaderSize
	if items > frame.MaxBatchFrames {
		items = frame.MaxBatchFrames
	}
	if items > (math.MaxInt-limits.MaxBodyBytes)/queueItemCost {
		return limits.MaxPendingPerSession, limits.MaxPendingItemsPerSession
	}
	return limits.MaxBodyBytes + items*queueItemCost, items
}

func subtractPendingReserve(
	cost, items int,
	reserveCost, reserveItems int) (int, int) {
	if reserveCost >= cost {
		cost = 0
	} else {
		cost -= reserveCost
	}
	if reserveItems >= items {
		items = 0
	} else {
		items -= reserveItems
	}
	return cost, items
}

func (s *Session) uplinkPendingLimits() (int, int) {
	reserveCost, reserveItems := pendingControlReserve(s.limits)
	return subtractPendingReserve(
		s.limits.MaxPendingPerSession,
		s.limits.MaxPendingItemsPerSession,
		reserveCost,
		reserveItems)
}

func (s *Session) downlinkPendingLimits() (int, int) {
	cost, items := s.uplinkPendingLimits()
	reserveCost, reserveItems := pendingUplinkReserve(s.limits)
	return subtractPendingReserve(
		cost,
		items,
		reserveCost,
		reserveItems)
}

func (s *Session) dataFrameAllowanceLocked(limit int) int {
	costLimit, itemLimit := s.downlinkPendingLimits()
	if s.pendingItems >= itemLimit {
		return 0
	}
	available := costLimit - s.pendingCost - queueItemCost - frame.HeaderSize
	if available <= 0 {
		return 0
	}
	if limit > available {
		limit = available
	}
	return limit
}

func (s *Session) reservePendingLocked(
	cost, items int,
	class pendingClass) bool {
	costLimit := s.limits.MaxPendingPerSession
	itemLimit := s.limits.MaxPendingItemsPerSession
	if class == pendingUplink {
		costLimit, itemLimit = s.uplinkPendingLimits()
	} else if class == pendingDownlink {
		costLimit, itemLimit = s.downlinkPendingLimits()
	}
	if cost <= 0 || items < 0 || cost > costLimit || items > itemLimit || s.pendingCost > costLimit-cost || s.pendingItems > itemLimit-items {
		return false
	}
	if s.budget != nil && !s.budget(cost, items, class) {
		return false
	}
	s.pendingCost += cost
	s.pendingItems += items
	return true
}

func (s *Session) takeDownBatchLocked() downBatch {
	size := 0
	cost := 0
	count := 0
	for count < len(s.pendingFrames) && count < frame.MaxBatchFrames {
		next := len(s.pendingFrames[count].encoded)
		if count != 0 && size+next > s.limits.CarrierBatchBytes {
			break
		}
		size += next
		cost += s.pendingFrames[count].cost
		count++
	}
	result := make([]byte, 0, size)
	for index := 0; index != count; index++ {
		if s.pendingFrames[index].typeCode == frame.Window &&
			s.pendingWindows[s.pendingFrames[index].streamID] == index {
			delete(s.pendingWindows, s.pendingFrames[index].streamID)
		}
		result = append(result, s.pendingFrames[index].encoded...)
		s.pendingFrames[index] = queuedFrame{}
	}
	s.pendingFrames = s.pendingFrames[count:]
	for id, index := range s.pendingWindows {
		s.pendingWindows[id] = index - count
	}
	if len(s.pendingFrames) == 0 {
		s.pendingFrames = nil
	}
	return downBatch{body: result, cost: cost, items: count}
}

func (s *Session) takeLaneDownBatchLocked(lane *carrierLane) downBatch {
	size := 0
	cost := 0
	count := 0
	for count < len(lane.pendingFrames) && count < frame.MaxBatchFrames {
		next := len(lane.pendingFrames[count].encoded)
		if count != 0 && size+next > s.limits.CarrierBatchBytes {
			break
		}
		size += next
		cost += lane.pendingFrames[count].cost
		count++
	}
	result := make([]byte, 0, size)
	for index := 0; index != count; index++ {
		if lane.pendingFrames[index].typeCode == frame.Window &&
			lane.pendingWindows[lane.pendingFrames[index].streamID] == index {
			delete(lane.pendingWindows, lane.pendingFrames[index].streamID)
		}
		result = append(result, lane.pendingFrames[index].encoded...)
		lane.pendingFrames[index] = queuedFrame{}
	}
	lane.pendingFrames = lane.pendingFrames[count:]
	for id, index := range lane.pendingWindows {
		lane.pendingWindows[id] = index - count
	}
	if len(lane.pendingFrames) == 0 {
		lane.pendingFrames = nil
	}
	return downBatch{body: result, cost: cost, items: count}
}

func (s *Session) rememberClosedLocked(id uint32) {
	if _, exists := s.closedStreams[id]; exists {
		return
	}
	s.closedStreams[id] = struct{}{}
	if len(s.closedOrder) < s.limits.MaxClosedStreamIDs {
		s.closedOrder = append(s.closedOrder, id)
	} else {
		old := s.closedOrder[s.closedStart]
		delete(s.closedStreams, old)
		if lane := s.carrierLanes[old]; lane != nil {
			s.releaseLaneLocked(lane)
			delete(s.carrierLanes, old)
		}
		s.closedOrder[s.closedStart] = id
		s.closedStart = (s.closedStart + 1) % len(s.closedOrder)
	}
	if lane := s.carrierLanes[id]; lane != nil {
		signal(lane.notify)
	}
}

// releaseLaneLocked returns an evicted lane's still-charged pending and
// unacked budget to the session and global pools and wakes any parked poller
// so it observes the eviction instead of blocking until its deadline. Late
// frames for the tombstoned id are then no-ops per PROTOCOL.md.
func (s *Session) releaseLaneLocked(lane *carrierLane) {
	cost := lane.unackedCost
	items := lane.unackedItems
	for i := range lane.pendingFrames {
		cost += lane.pendingFrames[i].cost
		items++
	}
	if cost != 0 || items != 0 {
		s.releasePendingLocked(cost, items)
	}
	lane.pendingFrames = nil
	lane.pendingWindows = nil
	lane.unacked = nil
	lane.unackedCost = 0
	lane.unackedItems = 0
	signal(lane.notify)
}

func (s *Session) releaseStreamWritesLocked(state *streamState) {
	if state.pendingWriteCost != 0 || state.pendingWriteItems != 0 {
		s.releasePendingLocked(state.pendingWriteCost, state.pendingWriteItems)
	}
	state.pendingWriteBytes = 0
	state.pendingWriteCost = 0
	state.pendingWriteItems = 0
	state.writes = nil
}

func (s *Session) releasePendingLocked(cost, items int) {
	if cost == 0 && items == 0 {
		return
	}
	if cost < 0 || items < 0 || cost > s.pendingCost || items > s.pendingItems {
		panic("invalid session pending budget release")
	}
	s.pendingCost -= cost
	s.pendingItems -= items
	if s.budget != nil {
		s.budget(-cost, -items, pendingUplink)
	}
	close(s.budgetNotify)
	s.budgetNotify = make(chan struct{})
}

// protocolFailure closes the session and every stream on it. It says why: a
// closed session is otherwise indistinguishable from the outside, because the
// carriers answer a protocol error with an uncacheable 404 and nothing else.
func (s *Session) protocolFailure(reason string) {
	log.Printf("session %s closed: %s", s.clientIP, reason)
	s.mu.Lock()
	s.closeLocked()
	s.mu.Unlock()
}

func (s *Session) laneProtocolFailure(laneID uint32, reason string) {
	if s.carrier == config.CarrierWebSocketLanes {
		s.ReleaseWebSocketLane(laneID)
	} else {
		s.protocolFailure(fmt.Sprintf("lane %d: %s", laneID, reason))
	}
}

func (s *Session) closeLocked() {
	if s.closed {
		return
	}
	s.closed = true
	close(s.done)
	for _, lane := range s.carrierLanes {
		lane.pendingFrames = nil
		lane.pendingWindows = nil
		lane.unacked = nil
		lane.unackedCost = 0
		lane.unackedItems = 0
		signal(lane.notify)
	}
	for _, value := range s.streams {
		value.backend.close()
	}
	s.streams = nil
	if s.pendingCost != 0 || s.pendingItems != 0 {
		if s.budget != nil {
			s.budget(-s.pendingCost, -s.pendingItems, pendingUplink)
		}
		s.pendingCost = 0
		s.pendingItems = 0
	}
	s.pendingFrames = nil
	s.pendingWindows = nil
	s.upPendingBatches = nil
	s.upAppliedDigests = nil
	s.upAppliedOrder = nil
	s.unacked = nil
	s.unackedCost = 0
	s.unackedItems = 0
	signal(s.notify)
	s.finishOnce.Do(func() {
		if s.onFinished != nil {
			go s.onFinished(s)
		}
	})
}

func (s *Session) runBackend(value *backendStream) {
	defer s.backendWG.Done()
	if s.onStreamFinished != nil {
		defer s.onStreamFinished()
	}
	value.run()
}

func (s *Session) backendDialFinished(failed bool) {
	if s.onBackendDialFinished != nil {
		s.onBackendDialFinished(failed)
	}
}

func signal(channel chan struct{}) {
	select {
	case channel <- struct{}{}:
	default:
	}
}

type backendStream struct {
	session   *Session
	id        uint32
	address   string
	ctx       context.Context
	cancel    context.CancelFunc
	connMu    sync.Mutex
	conn      net.Conn
	closeOnce sync.Once
	finished  chan struct{}
	// endReason is written by this stream's own read loop and read once that
	// loop has returned, so it needs no further protection.
	endReason string
}

func newBackendStream(session *Session, id uint32, address string) *backendStream {
	ctx, cancel := context.WithCancel(context.Background())
	return &backendStream{
		session:  session,
		id:       id,
		address:  address,
		ctx:      ctx,
		cancel:   cancel,
		finished: make(chan struct{}),
	}
}

func (s *backendStream) run() {
	defer close(s.finished)
	defer s.session.backendClosed(s.id, s)

	dialer := net.Dialer{
		Timeout: s.session.timeouts.BackendDial.Value(),
		Control: s.session.tunnelDialControl(),
	}
	connection, err := dialer.DialContext(s.ctx, "tcp", s.address)
	s.session.backendDialFinished(err != nil && s.ctx.Err() == nil)
	if err != nil {
		return
	}
	s.connMu.Lock()
	if s.ctx.Err() != nil {
		s.connMu.Unlock()
		_ = connection.Close()
		return
	}
	s.conn = connection
	s.connMu.Unlock()

	writeDone := make(chan struct{})
	go func() {
		defer close(writeDone)
		s.writeLoop(connection)
		s.close()
	}()
	s.readLoop(connection)
	s.close()
	<-writeDone
}

func (s *backendStream) writeLoop(connection net.Conn) {
	for {
		data, ok := s.session.nextWrite(s.id, s.ctx.Done())
		if !ok {
			return
		}
		for len(data) != 0 {
			written, err := connection.Write(data)
			if written > 0 {
				if !s.session.backendDrained(s.id, written) {
					return
				}
				data = data[written:]
			}
			if err != nil || written == 0 {
				return
			}
		}
		if !s.session.backendWriteFinished(s.id) {
			return
		}
	}
}

func (s *backendStream) readLoop(connection net.Conn) {
	buffer := make([]byte, frame.DataChunk)
	for {
		allowance, ok := s.session.nextReadAllowance(s.id, s.ctx.Done())
		if !ok {
			s.endReason = "the stream was cancelled"
			return
		}
		read, err := connection.Read(buffer[:allowance])
		if read > 0 && !s.session.backendData(s.id, buffer[:read]) {
			s.endReason = "the session stopped taking bytes"
			return
		}
		if err != nil {
			switch {
			case errors.Is(err, io.EOF):
				s.endReason = "the destination closed"
			case s.ctx.Err() != nil:
				s.endReason = "the stream was cancelled"
			default:
				s.endReason = "the destination failed: " + err.Error()
			}
			return
		}
		if read == 0 {
			s.endReason = "the destination sent nothing"
			return
		}
	}
}

func (s *backendStream) close() {
	s.closeOnce.Do(func() {
		s.cancel()
		s.connMu.Lock()
		if s.conn != nil {
			_ = s.conn.Close()
			s.conn = nil
		}
		s.connMu.Unlock()
	})
}

// SetUplinkSaturatedForTest fills or empties the in-flight uplink parse slots so
// tests can exercise the concurrent-uplink path without a real race.
func (s *Session) SetUplinkSaturatedForTest(saturated bool) {
	s.mu.Lock()
	s.upParsing = 0
	if saturated {
		s.upParsing = s.limits.MaxPipelinedUpBatches
	}
	s.mu.Unlock()
}
