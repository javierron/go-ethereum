// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package p2p

import (
	"net"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common/mclock"
	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/log"
	"github.com/streadway/amqp"
)

// PeerMQ represents a connected remote node.
type PeerMQ struct {
	rw      *amqp.Connection
	running map[string]*protoRW
	log     log.Logger
	created mclock.AbsTime

	wg       sync.WaitGroup
	protoErr chan error
	closed   chan struct{}
	disc     chan DiscReason

	// events receives message send / receive events if set
	events   *event.Feed
	testPipe *MsgPipeRW // for testing
}

// RunningCap returns true if the peerMQ is actively connected using any of the
// enumerated versions of a specific protocol, meaning that at least one of the
// versions is supported by both this node and the peerMQ p.
func (p *PeerMQ) RunningCap(protocol string, versions []uint) bool {
	if proto, ok := p.running[protocol]; ok {
		for _, ver := range versions {
			if proto.Version == ver {
				return true
			}
		}
	}
	return false
}

// LocalAddr returns the local address of the network connection.
func (p *PeerMQ) LocalAddr() net.Addr {
	return p.rw.LocalAddr()
}

// Disconnect terminates the peerMQ connection with the given reason.
// It returns immediately and does not wait until the connection is closed.
func (p *PeerMQ) Disconnect(reason DiscReason) {
	if p.testPipe != nil {
		p.testPipe.Close()
	}

	select {
	case p.disc <- reason:
	case <-p.closed:
	}
}

func newPeerMQ(log log.Logger, conn *amqp.Connection, protocols []Protocol) *PeerMQ {
	// protomap := matchProtocols(protocols, conn.caps, conn)
	p := &PeerMQ{
		rw: conn,
		// running:  protomap,
		created: mclock.Now(),
		disc:    make(chan DiscReason),
		// protoErr: make(chan error, len(protomap)+1), // protocols + pingLoop
		closed: make(chan struct{}),
		log:    log.New("id", conn.LocalAddr(), "conn"),
	}
	return p
}

func (p *PeerMQ) Log() log.Logger {
	return p.log
}

func (p *PeerMQ) run() (remoteRequested bool, err error) {
	p.log.Info("Running MQ peer!")
	var (
		writeStart = make(chan struct{}, 1)
		writeErr   = make(chan error, 1)
		readErr    = make(chan error, 1)
		// reason     DiscReason // sent to the peerMQ
	)
	p.wg.Add(2)
	go p.readLoop(readErr)
	go p.pingLoop()

	// Start all protocol handlers.
	writeStart <- struct{}{}
	p.startProtocols(writeStart, writeErr)

	// Wait for an error or disconnect.
loop:
	for {
		select {
		case err = <-writeErr:
			// A write finished. Allow the next write to start if
			// there was no error.
			if err != nil {
				// reason = DiscNetworkError
				break loop
			}
			writeStart <- struct{}{}
		case err = <-readErr:
			if _, ok := err.(DiscReason); ok {
				remoteRequested = true
				// reason = r
			} else {
				// reason = DiscNetworkError
			}
			break loop
		case err = <-p.protoErr:
			// reason = discReasonForError(err)
			break loop
		case err = <-p.disc:
			// reason = discReasonForError(err)
			break loop
		}
	}

	close(p.closed)
	// p.rw.Close()
	p.wg.Wait()
	return remoteRequested, err
}

func (p *PeerMQ) pingLoop() {
	ping := time.NewTimer(pingInterval)
	defer p.wg.Done()
	defer ping.Stop()
	for {
		select {
		case <-ping.C:
			// if err := SendItems(p.rw, pingMsg); err != nil {
			// 	p.protoErr <- err
			// 	return
			// }
			// ping.Reset(pingInterval)
		case <-p.closed:
			return
		}
	}
}

func (p *PeerMQ) readLoop(errc chan<- error) {
	defer p.wg.Done()
	for {
		// msg, err := p.rw.ReadMsg()
		// if err != nil {
		// 	errc <- err
		// 	return
		// }
		// msg.ReceivedAt = time.Now()
		// if err = p.handle(msg); err != nil {
		// 	errc <- err
		// 	return
		// }
		time.Sleep(10 * time.Second)
	}
}

func (p *PeerMQ) handle(msg Msg) error {
	// switch {
	// case msg.Code == pingMsg:
	// 	msg.Discard()
	// 	go SendItems(p.rw, pongMsg)
	// case msg.Code == discMsg:
	// 	var reason [1]DiscReason
	// 	// This is the last message. We don't need to discard or
	// 	// check errors because, the connection will be closed after it.
	// 	rlp.Decode(msg.Payload, &reason)
	// 	return reason[0]
	// case msg.Code < baseProtocolLength:
	// 	// ignore other base protocol messages
	// 	return msg.Discard()
	// default:
	// 	// it's a subprotocol message
	// 	proto, err := p.getProto(msg.Code)
	// 	if err != nil {
	// 		return fmt.Errorf("msg code out of range: %v", msg.Code)
	// 	}
	// 	if metrics.Enabled {
	// 		m := fmt.Sprintf("%s/%s/%d/%#02x", ingressMeterName, proto.Name, proto.Version, msg.Code-proto.offset)
	// 		metrics.GetOrRegisterMeter(m, nil).Mark(int64(msg.meterSize))
	// 		metrics.GetOrRegisterMeter(m+"/packets", nil).Mark(1)
	// 	}
	// 	select {
	// 	case proto.in <- msg:
	// 		return nil
	// 	case <-p.closed:
	// 		return io.EOF
	// 	}
	// }
	return nil
}

// func countMatchingProtocols(protocols []Protocol, caps []Cap) int {
// 	n := 0
// 	for _, cap := range caps {
// 		for _, proto := range protocols {
// 			if proto.Name == cap.Name && proto.Version == cap.Version {
// 				n++
// 			}
// 		}
// 	}
// 	return n
// }

// matchProtocols creates structures for matching named subprotocols.
// func matchProtocols(protocols []Protocol, caps []Cap, rw MsgReadWriter) map[string]*protoRW {
// 	sort.Sort(capsByNameAndVersion(caps))
// 	offset := baseProtocolLength
// 	result := make(map[string]*protoRW)

// outer:
// 	for _, cap := range caps {
// 		for _, proto := range protocols {
// 			if proto.Name == cap.Name && proto.Version == cap.Version {
// 				// If an old protocol version matched, revert it
// 				if old := result[cap.Name]; old != nil {
// 					offset -= old.Length
// 				}
// 				// Assign the new match
// 				result[cap.Name] = &protoRW{Protocol: proto, offset: offset, in: make(chan Msg), w: rw}
// 				offset += proto.Length

// 				continue outer
// 			}
// 		}
// 	}
// 	return result
// }

func (p *PeerMQ) startProtocols(writeStart <-chan struct{}, writeErr chan<- error) {
	p.wg.Add(len(p.running))
	// for _, proto := range p.running {
	// 	proto := proto
	// 	proto.closed = p.closed
	// 	proto.wstart = writeStart
	// 	proto.werr = writeErr
	// 	var rw MsgReadWriter = proto
	// 	// if p.events != nil {
	// 	// 	rw = newMsgEventer(rw, p.events, p.ID(), proto.Name, p.Info().Network.RemoteAddress, p.Info().Network.LocalAddress)
	// 	// }
	// 	p.log.Trace(fmt.Sprintf("Starting protocol %s/%d", proto.Name, proto.Version))
	// 	go func() {
	// 		defer p.wg.Done()
	// 		err := proto.Run(p, rw)
	// 		if err == nil {
	// 			p.log.Trace(fmt.Sprintf("Protocol %s/%d returned", proto.Name, proto.Version))
	// 			err = errProtocolReturned
	// 		} else if err != io.EOF {
	// 			p.log.Trace(fmt.Sprintf("Protocol %s/%d failed", proto.Name, proto.Version), "err", err)
	// 		}
	// 		p.protoErr <- err
	// 	}()
	// }
}

// getProto finds the protocol responsible for handling
// the given message code.
// func (p *PeerMQ) getProto(code uint64) (*protoRW, error) {
// for _, proto := range p.running {
// 	if code >= proto.offset && code < proto.offset+proto.Length {
// 		return proto, nil
// 	}
// }
// return nil, newPeerMQError(errInvalidMsgCode, "%d", code)
// }

// type protoRW struct {
// 	Protocol
// 	in     chan Msg        // receives read messages
// 	closed <-chan struct{} // receives when peerMQ is shutting down
// 	wstart <-chan struct{} // receives when write may start
// 	werr   chan<- error    // for write results
// 	offset uint64
// 	w      MsgWriter
// }

// func (rw *protoRW) WriteMsg(msg Msg) (err error) {
// 	if msg.Code >= rw.Length {
// 		return newPeerMQError(errInvalidMsgCode, "not handled")
// 	}
// 	msg.meterCap = rw.cap()
// 	msg.meterCode = msg.Code

// 	msg.Code += rw.offset

// 	select {
// 	case <-rw.wstart:
// 		err = rw.w.WriteMsg(msg)
// 		// Report write status back to PeerMQ.run. It will initiate
// 		// shutdown if the error is non-nil and unblock the next write
// 		// otherwise. The calling protocol code should exit for errors
// 		// as well but we don't want to rely on that.
// 		rw.werr <- err
// 	case <-rw.closed:
// 		err = ErrShuttingDown
// 	}
// 	return err
// }

// func (rw *protoRW) ReadMsg() (Msg, error) {
// 	select {
// 	case msg := <-rw.in:
// 		msg.Code -= rw.offset
// 		return msg, nil
// 	case <-rw.closed:
// 		return Msg{}, io.EOF
// 	}
// }

// PeerMQInfo represents a short summary of the information known about a connected
// peerMQ. Sub-protocol independent fields are contained and initialized here, with
// protocol specifics delegated to all connected sub-protocols.
// type PeerMQInfo struct {
// 	ENR     string   `json:"enr,omitempty"` // Ethereum Node Record
// 	Enode   string   `json:"enode"`         // Node URL
// 	ID      string   `json:"id"`            // Unique node identifier
// 	Name    string   `json:"name"`          // Name of the node, including client type, version, OS, custom data
// 	Caps    []string `json:"caps"`          // Protocols advertised by this peerMQ
// 	Network struct {
// 		LocalAddress  string `json:"localAddress"`  // Local endpoint of the TCP data connection
// 		RemoteAddress string `json:"remoteAddress"` // Remote endpoint of the TCP data connection
// 		Inbound       bool   `json:"inbound"`
// 		Trusted       bool   `json:"trusted"`
// 		Static        bool   `json:"static"`
// 	} `json:"network"`
// 	Protocols map[string]interface{} `json:"protocols"` // Sub-protocol specific metadata fields
// }

// Info gathers and returns a collection of metadata known about a peerMQ.
// func (p *PeerMQ) Info() *PeerMQInfo {
// 	// Gather the protocol capabilities
// 	var caps []string
// 	for _, cap := range p.Caps() {
// 		caps = append(caps, cap.String())
// 	}
// 	// Assemble the generic peerMQ metadata
// 	info := &PeerMQInfo{
// 		Enode:     p.Node().URLv4(),
// 		ID:        p.ID().String(),
// 		Name:      p.Fullname(),
// 		Caps:      caps,
// 		Protocols: make(map[string]interface{}),
// 	}
// 	if p.Node().Seq() > 0 {
// 		info.ENR = p.Node().String()
// 	}
// 	info.Network.LocalAddress = p.LocalAddr().String()
// 	info.Network.RemoteAddress = p.RemoteAddr().String()
// 	info.Network.Inbound = p.rw.is(inboundConn)
// 	info.Network.Trusted = p.rw.is(trustedConn)
// 	info.Network.Static = p.rw.is(staticDialedConn)

// 	// Gather all the running protocol infos
// 	for _, proto := range p.running {
// 		protoInfo := interface{}("unknown")
// 		if query := proto.Protocol.PeerMQInfo; query != nil {
// 			if metadata := query(p.ID()); metadata != nil {
// 				protoInfo = metadata
// 			} else {
// 				protoInfo = "handshake"
// 			}
// 		}
// 		info.Protocols[proto.Name] = protoInfo
// 	}
// 	return info
// }
