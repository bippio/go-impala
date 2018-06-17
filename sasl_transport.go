//Copied from https://github.com/jellybean4/thrift-sasl/blob/master/sasl_transport.go
// and heavily modified based on https://github.com/cloudera/thrift_sasl/blob/master/thrift_sasl/__init__.py

package impalathing

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

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
	msgHeaderBytes     = statusBytes + payloadLengthBytes
)

// TSaslTransport extends base TTransport with sasl
type TSaslTransport struct {
	thrift.TTransport
	client *sasl.Client
	wrap   bool
	buffer *bytes.Buffer

	handshakeComplete bool
	mechList          []string
}

// NewTSaslTransport create a new sasl supported transport
func NewTSaslTransport(transport thrift.TTransport, client *sasl.Client, mechList []string) (thrift.TTransport, error) {
	if client == nil {
		panic("sasl client must not be nil")
	}

	s := &TSaslTransport{}
	s.TTransport = transport
	s.client = client
	s.buffer = &bytes.Buffer{}
	s.mechList = mechList
	return s, nil
}

// Open the underlying transport if it's not already open and then performs
// SASL negotiation. If a QOP is negotiated during this SASL handshake, it used
// for all communication on this transport after this call is complete.
func (s *TSaslTransport) Open() (err error) {
	//defer func() {
	//	if err != nil && s.TTransport.IsOpen() {
	//		s.TTransport.Close()
	//	}
	//}()

	if s.handshakeComplete {
		return errors.New("SASL transport already open")
	}

	if !s.TTransport.IsOpen() {
		if err := s.TTransport.Open(); err != nil {
			return err
		}
	}

	// Negotiate a SASL mechanism. The client also sends its
	// initial response, or an empty one.
	_, err = s.handleStartMessage()
	if err != nil {
		return err
	}

	//if done {
	//	s.handshakeComplete = true
	//	fmt.Println("done")
	//
	//	return nil
	//}

	for {
		status, payload, err := s.receiveSaslMessage()
		if err != nil {
			return err
		}

		// If server indicates COMPLETE, we don't need to send back any further response
		if status == negotiationStatusComplete {
			fmt.Println("negotiationStatusComplete")
			break
		}

		if status != negotiationStatusOk {
			return fmt.Errorf("Expected COMPLETE or OK, got %d", status)
		}

		response, done, err := s.client.Step(payload)

		if err != nil {
			return err
		}

		if done {
			return fmt.Errorf("Sasl client does not want to sent messages, while server still wants to receive them")
		}

		if err := s.sendSaslMessage(negotiationStatusOk, response); err != nil {
			return err
		}
	}

	ssf, err := s.client.GetSSF()
	if err != nil{
		return fmt.Errorf("Could not get SSF, this should not happen: %v", err)
	}
	s.wrap = (ssf != 0)

	fmt.Println("wrap", s.wrap)
	s.handshakeComplete = true
	return nil
}

// IsOpen test if the transport is opened
func (s *TSaslTransport) IsOpen() bool {
	return s.TTransport.IsOpen() && s.handshakeComplete
}

// Close the underlying transport and disposes of the SASL implementation
// underlying this transport.
func (s *TSaslTransport) Close() error {
	err := s.TTransport.Close()
	s.client.Free()
	if err != nil {
		return err
	}
	return nil
}

// Flush to the underlying transport. Wraps the contents if a QOP was
// negotiated during the SASL handshake.
func (s *TSaslTransport) Flush() error {
	fmt.Println("Flush")
	payload := make([]byte, s.buffer.Len())
	copy(payload, s.buffer.Bytes())
	s.buffer.Reset()

	dataLength := len(payload)
	if s.wrap {
		wrapped, err := s.client.Encode(payload)
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
	fmt.Println("Write", len(data))
	if !s.IsOpen() {
		return 0, errors.New("SASL authentication not complete")
	}
	return s.buffer.Write(data)
}

func (s *TSaslTransport) Read(data []byte) (int, error) {
	fmt.Println("Read")
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

	buff := make([]byte, frameLength)
	if err := s.readAll(buff); err != nil {
		return err
	}
	if !s.wrap {
		_, err := s.buffer.Write(buff)
		return err
	}

	if unwrapped, err := s.client.Decode(buff); err != nil {
		return err
	} else if _, err := s.buffer.Write(unwrapped); err != nil {
		return err
	}
	return nil
}

func (s *TSaslTransport) readLength() (int, error) {
	buf := make([]byte, 4)
	if err := s.readAll(buf); err != nil {
		return 0, err
	} else {
		return int(binary.BigEndian.Uint32(buf)), nil
	}
}

func (s *TSaslTransport) writeLength(length int) error {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(length))
	_, err := s.TTransport.Write(buf)
	return err
}

func (s *TSaslTransport) handleStartMessage() (bool, error) {

	fmt.Println("Start msg")

	mechanism, resp, done, err := s.client.Start(s.mechList)

	if err != nil {
		return false, err
	}

	fmt.Println("Mechanism", mechanism, "done", done)

	if err := s.sendSaslMessage(negotiationStatusStart, []byte(mechanism)); err != nil {
		return done, err
	}

	status := byte(0)
	//if done {
	//	status = negotiationStatusComplete
	//} else {
		status = negotiationStatusOk
	//}
	if err := s.sendSaslMessage(status, resp); err != nil {
		return done, err
	}
	fmt.Println("Start msg end")
	return done, s.TTransport.Flush()
}

func (s *TSaslTransport) sendSaslMessage(status byte, payload []byte) error {
	msgHeader := make([]byte, msgHeaderBytes)

	msgHeader[0] = status
	binary.BigEndian.PutUint32(msgHeader[statusBytes:], uint32(len(payload)))

	fmt.Println("sendSasl", status, msgHeader, payload, fmt.Sprintf("%q",payload))

	if _, err := s.TTransport.Write(msgHeader); err != nil {
		return err
	} else if _, err := s.TTransport.Write(payload); err != nil {
		return err
	}
	return s.TTransport.Flush()
}

func (s *TSaslTransport) receiveSaslMessage() (byte, []byte, error) {
	fmt.Println("receiveSasl")
	msgHeader := make([]byte, msgHeaderBytes)

	if err := s.readAll(msgHeader); err != nil {
		return 0, nil, err
	}
	fmt.Println("receiveSasl header", msgHeader)
	status := msgHeader[0]
	if !s.validStatus(status) {
		return 0, nil, fmt.Errorf("Invalid status %d", status)
	}

	payloadBytes := int(binary.BigEndian.Uint32(msgHeader[statusBytes:]))
	if payloadBytes < 0 || payloadBytes > 104857600 {
		return 0, nil, fmt.Errorf("Invalid payload header length: %d", payloadBytes)
	}

	payload := make([]byte, payloadBytes)
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
	fmt.Println("ReadAll", len(buffer))
	_, err := io.ReadFull(s.TTransport, buffer)
	return err
}
