// Command yamuxpeer is the Go end of the TypeScript yamux compatibility tests
// (ts/test/compat.test.ts). It dials a WebSocket and runs a yamux-insecure
// client session over it through the very code vtunnel uses — the ws
// transport and internal/session — then plays one scenario against whatever
// the TypeScript side is doing.
//
// It lives under testdata so that go build ./... and go vet ./... leave it be.
// Exit status 0 means the Go side saw what it expected; anything it has to say
// goes to stderr.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/vivid-money/vtunnel/internal/session"
	"github.com/vivid-money/vtunnel/internal/transport/ws"
)

func main() {
	url := flag.String("url", "", "ws:// URL of the TypeScript end")
	scenario := flag.String("case", "", "scenario to run")
	flag.Parse()
	log.SetFlags(0)
	// A scenario that hangs is a failure, and must not outlive the test run.
	time.AfterFunc(45*time.Second, func() { log.Fatalf("%s: timed out", *scenario) })

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	conn, err := ws.Dial(ctx, *url, nil, 10*time.Second)
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	sess, err := session.Dial(session.KindYamuxInsecure, conn, session.Config{Handshake: 10 * time.Second})
	if err != nil {
		log.Fatalf("session: %v", err)
	}

	switch *scenario {
	case "echo":
		err = echo(sess)
	case "serveropen":
		err = serveEcho(sess)
	case "bulk":
		err = echoOne(sess, 16<<20)
		if err == nil {
			err = sess.Close()
		}
	case "hold":
		err = holdOpen(sess, 300)
	case "reset":
		err = expectReset(sess)
	case "die":
		err = dieMidStream(sess)
	default:
		err = fmt.Errorf("unknown case %q", *scenario)
	}
	if err != nil {
		log.Fatalf("%s: %v", *scenario, err)
	}
}

// echo opens many streams at once — more than yamux's accept backlog of 256,
// so opens stall unless ACKs come back; a few bigger than any window either
// end grants; one empty — sends a payload on each, half-closes, and expects
// the same bytes back before the TypeScript end's FIN.
func echo(sess session.Session) error {
	const n = 300
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := range n {
		size := 1 + i*397%100_000
		switch {
		case i == 0:
			size = 0
		case i%40 == 1:
			size = 4<<20 + i
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := echoOne(sess, size); err != nil {
				errs <- fmt.Errorf("stream %d (%d bytes): %w", i, size, err)
			}
		}()
	}
	wg.Wait()
	close(errs)
	if err := <-errs; err != nil {
		return err
	}
	return sess.Close()
}

func echoOne(sess session.Session, size int) error {
	stream, err := sess.Open()
	if err != nil {
		return err
	}
	defer stream.Close()
	payload := make([]byte, size)
	rand.Read(payload)

	writeErr := make(chan error, 1)
	go func() {
		if _, err := stream.Write(payload); err != nil {
			writeErr <- err
			return
		}
		writeErr <- closeWrite(stream)
	}()
	got, err := io.ReadAll(stream)
	if err != nil {
		return fmt.Errorf("read: %w", err)
	}
	if err := <-writeErr; err != nil {
		return fmt.Errorf("write: %w", err)
	}
	if sha256.Sum256(got) != sha256.Sum256(payload) {
		return fmt.Errorf("echo differs: sent %d bytes, got %d", len(payload), len(got))
	}
	return nil
}

// holdOpen keeps n streams open at once, more than yamux's accept backlog, and
// closes none until every one has been answered. Closing a stream gives its
// open credit back, so only streams held open show whether ACKs arrive: without
// them the open past the backlog waits forever.
func holdOpen(sess session.Session, n int) error {
	streams := make([]net.Conn, 0, n)
	for i := range n {
		stream, err := sess.Open()
		if err != nil {
			return fmt.Errorf("open %d: %w", i, err)
		}
		streams = append(streams, stream)
		if _, err := stream.Write([]byte{byte(i)}); err != nil {
			return err
		}
	}
	for i, stream := range streams {
		b := make([]byte, 1)
		if _, err := io.ReadFull(stream, b); err != nil || b[0] != byte(i) {
			return fmt.Errorf("stream %d: got %v, %v", i, b, err)
		}
	}
	for _, stream := range streams {
		stream.Close()
	}
	return sess.Close()
}

// serveEcho echoes every stream the TypeScript end opens until it ends the
// session.
func serveEcho(sess session.Session) error {
	for {
		stream, err := sess.Accept()
		if err != nil {
			return nil // the TypeScript end closed the session: done
		}
		go func() {
			defer stream.Close()
			io.Copy(stream, stream)
			closeWrite(stream)
		}()
	}
}

// expectReset opens a stream the TypeScript end resets on sight.
func expectReset(sess session.Session) error {
	stream, err := sess.Open()
	if err != nil {
		return err
	}
	if _, err := stream.Write([]byte("reset me")); err != nil {
		return err
	}
	stream.SetReadDeadline(time.Now().Add(10 * time.Second))
	_, err = io.ReadAll(stream)
	if err == nil || !strings.Contains(err.Error(), "connection reset") {
		return fmt.Errorf("want connection reset, got %v", err)
	}
	return nil
}

// dieMidStream opens a stream, says something, and vanishes without closing
// anything — the controller pod killed mid-request.
func dieMidStream(sess session.Session) error {
	stream, err := sess.Open()
	if err != nil {
		return err
	}
	if _, err := stream.Write([]byte("half a request")); err != nil {
		return err
	}
	buf := make([]byte, 5)
	if _, err := io.ReadFull(stream, buf); err != nil {
		return err
	}
	if !bytes.Equal(buf, []byte("ready")) {
		return fmt.Errorf("want ready, got %q", buf)
	}
	os.Exit(0)
	return nil
}

func closeWrite(c net.Conn) error {
	cw, ok := c.(interface{ CloseWrite() error })
	if !ok {
		return errors.New("stream cannot half-close")
	}
	return cw.CloseWrite()
}
