//Copied from https://github.com/jellybean4/thrift-sasl/blob/master/sasl_transport.go and heavily modified

package impalathing

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/freddierice/go-sasl"
)

const (
	negotiationStatusStart    = byte(0x01)
	negotiationStatusOk       = byte(0x02)
	negotiationStatusBad      = byte(0x03)
	negotiationStatusError    = byte(0x04)
	negotiationStatusComplete = byte(0x05)

	statusBytes        = 1
	payloadLengthBytes = 4
)

// TSaslTransport extends base TTransport with sasl
type TSaslTransport struct {
	thrift.TTransport
	client    sasl.Client
	msgHeader []byte
	wrap      bool
	buffer    *bytes.Buffer
}

// NewTSaslTransport create a new sasl supported transport
func NewTSaslTransport(transport thrift.TTransport, client sasl.Client) (thrift.TTransport, error) {
	s := &TSaslTransport{}
	s.TTransport = transport
	s.client = client
	s.msgHeader = make([]byte, statusBytes+payloadLengthBytes)
	s.buffer = &bytes.Buffer{}
	return s, nil
}

// Open the underlying transport if it's not already open and then performs
// SASL negotiation. If a QOP is negotiated during this SASL handshake, it used
// for all communication on this transport after this call is complete.
func (s *TSaslTransport) Open() (err error) {
	defer func() {
		if err != nil && s.TTransport.IsOpen() {
			s.TTransport.Close()
		}
	}()

	if s.client != nil && s.client.IsComplete() {
		return errors.New("SASL transport already open")
	}

	if !s.TTransport.IsOpen() {
		if err := s.TTransport.Open(); err != nil {
			return err
		}
	}

	// Negotiate a SASL mechanism. The client also sends its
	// initial response, or an empty one.
	if err := s.handleStartMessage(); err != nil {
		return err
	}

	var (
		status  byte
		payload []byte
	)
	for !s.client.IsComplete() {
		status, payload, err = s.receiveSaslMessage()
		if err != nil {
			return err
		} else if status != negotiationStatusComplete && status != negotiationStatusOk {
			return fmt.Errorf("Expected COMPLETE or OK, got %d", status)
		}

		// If server indicates COMPLETE, we don't need to send back any further response
		if status == negotiationStatusComplete {
			break
		}

		if challenge, err := s.client.EvaluateChallenge(payload); err != nil {
			return err
		} else if err := s.sendSaslMessage(negotiationStatusOk, challenge); err != nil {
			return err
		}
	}

	if !s.client.IsComplete() {
		return errors.New("unexpected state")
	}

	if status == 0 || status == negotiationStatusOk {
		status, payload, err = s.receiveSaslMessage()
		if err != nil {
			return err
		} else if status != negotiationStatusComplete {
			return fmt.Errorf("Expected SASL COMPLETE, bug got %d", status)
		}
	}

	if prop, err := s.client.GetNegotiatedProperty(sasl.SaslPropertyQop); err != nil {
		return err
	} else if propStr, ok := prop.(string); !ok {
		return errors.New("prop type not string")
	} else if strings.ToLower(propStr) != "auth" {
		s.wrap = true
	}
	return nil
}

// IsOpen test if the transport is opened
func (s *TSaslTransport) IsOpen() bool {
	return s.TTransport.IsOpen() && s.client != nil && s.client.IsComplete()
}

// Close the underlying transport and disposes of the SASL implementation
// underlying this transport.
func (s *TSaslTransport) Close() error {
	err := s.TTransport.Close()
	s.client.Dispose()
	if err != nil {
		return err
	}
	return nil
}

// Flush to the underlying transport. Wraps the contents if a QOP was
// negotiated during the SASL handshake.
func (s *TSaslTransport) Flush() error {
	payload := make([]byte, s.buffer.Len())
	copy(payload, s.buffer.Bytes())
	s.buffer.Reset()

	dataLength := len(payload)
	if s.wrap {
		wrapped, err := s.client.Wrap(payload, 0, len(payload))
		if err != nil {
			return err
		}
		dataLength = len(wrapped)
		payload = wrapped
	}

	if err := s.writeLength(dataLength); err != nil {
		return err
	} else if _, err := s.TTransport.Write(payload); err != nil {
		return err
	} else if err := s.TTransport.Flush(); err != nil {
		return err
	}
	return nil
}

func (s *TSaslTransport) Write(data []byte) (int, error) {
	if !s.IsOpen() {
		return 0, errors.New("SASL authentication not complete")
	}
	return s.buffer.Write(data)
}

func (s *TSaslTransport) Read(data []byte) (int, error) {
	if !s.IsOpen() {
		return 0, errors.New("SASL authentication not complete")
	}

	readCount, _ := s.buffer.Read(data)
	if readCount > 0 {
		return readCount, nil
	}

	s.buffer.Reset()
	if err := s.readFrame(); err != nil {
		return 0, err
	} else if readCount, err := s.buffer.Read(data); err != nil {
		return 0, err
	} else {
		return readCount, nil
	}
}

func (s *TSaslTransport) readFrame() error {
	frameLength, err := s.readLength()
	if err != nil {
		return err
	} else if frameLength < 0 {
		return fmt.Errorf("Read a negative frame size (%d)", frameLength)
	}

	buff := make([]byte, frameLength, frameLength)
	if err := s.readAll(buff); err != nil {
		return err
	}
	if !s.wrap {
		_, err := s.buffer.Write(buff)
		return err
	}

	if unwrapped, err := s.client.Unwrap(buff, 0, len(buff)); err != nil {
		return err
	} else if _, err := s.buffer.Write(unwrapped); err != nil {
		return err
	}
	return nil
}

func (s *TSaslTransport) readLength() (int, error) {
	buf := make([]byte, 4, 4)
	if err := s.readAll(buf); err != nil {
		return 0, err
	} else {
		return int(binary.BigEndian.Uint32(buf)), nil
	}
}

func (s *TSaslTransport) writeLength(length int) error {
	buf := make([]byte, 4, 4)
	binary.BigEndian.PutUint32(buf, uint32(length))
	_, err := s.TTransport.Write(buf)
	return err
}

func (s *TSaslTransport) handleStartMessage() error {
	resp := make([]byte, 0, 0)
	if s.client.HasInitialResponse() {
		if msg, err := s.client.EvaluateChallenge(resp); err != nil {
			return err
		} else {
			resp = msg
		}
	}

	mechanism := []byte(s.client.GetMechanismName())
	if err := s.sendSaslMessage(negotiationStatusStart, mechanism); err != nil {
		return err
	}
	status := byte(0)
	if s.client.IsComplete() {
		status = negotiationStatusComplete
	} else {
		status = negotiationStatusOk
	}
	if err := s.sendSaslMessage(status, resp); err != nil {
		return err
	}
	return s.TTransport.Flush()
}

func (s *TSaslTransport) sendSaslMessage(status byte, payload []byte) error {
	if payload == nil {
		payload = make([]byte, 0, 0)
	}

	s.msgHeader[0] = status
	binary.BigEndian.PutUint32(s.msgHeader[statusBytes:], uint32(len(payload)))

	if _, err := s.TTransport.Write(s.msgHeader); err != nil {
		return err
	} else if _, err := s.TTransport.Write(payload); err != nil {
		return err
	}
	return s.TTransport.Flush()
}

func (s *TSaslTransport) receiveSaslMessage() (byte, []byte, error) {
	if err := s.readAll(s.msgHeader); err != nil {
		return 0, nil, err
	}
	status := s.msgHeader[0]
	if !s.validStatus(status) {
		return 0, nil, fmt.Errorf("Invalid status %d", status)
	}

	payloadBytes := int(binary.BigEndian.Uint32(s.msgHeader[statusBytes:]))
	if payloadBytes < 0 || payloadBytes > 104857600 {
		return 0, nil, fmt.Errorf("Invalid payload header length: %d", payloadBytes)
	}

	payload := make([]byte, payloadBytes, payloadBytes)
	if err := s.readAll(payload); err != nil {
		return 0, nil, err
	}

	if status == negotiationStatusBad || status == negotiationStatusError {
		msg := string(payload)
		return 0, nil, fmt.Errorf("Peer indicated failre: %s", msg)
	}
	return status, payload, nil
}

func (s *TSaslTransport) validStatus(status byte) bool {
	switch status {
	case negotiationStatusStart, negotiationStatusOk:
		fallthrough
	case negotiationStatusError, negotiationStatusComplete:
		fallthrough
	case negotiationStatusBad:
		return true
	default:
		return false
	}
}

func (s *TSaslTransport) readAll(buffer []byte) error {
	count := 0
	bufferLen := len(buffer)
	if count >= bufferLen {
		return nil
	}

	for true {
		n, err := s.TTransport.Read(buffer[count:])
		if n > 0 {
			count += n
		}
		if count == bufferLen {
			return nil
		}

		if err != nil {
			return err
		}
	}
	return errors.New("unexpected state")
}
