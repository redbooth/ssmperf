package main

import (
	"bufio"
	"bytes"
	"crypto"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"os"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"time"
)

func writeHex(v int64, b []byte) {
	for i := 0; i < 16; i++ {
		b[i] = 'a' + byte(v&0xf)
		v >>= 4
	}
}

func readHex(b []byte) int64 {
	var v int64
	for i := 0; i < 16; i++ {
		v = (v << 4) | int64(b[15-i]-'a')
	}
	return v
}

type benchFunc func(*conn, *sync.WaitGroup)

func dcast(t byte, c net.Conn, numCon, messageCount, messageSize, writeBuffer int) {
	// fill message skeleton
	msg := make([]byte, 40+messageSize)
	msg[0] = t
	copy(msg[1:], []byte("CAST "))
	msg[22] = ' '
	for i := 39; i < len(msg)-1; i++ {
		msg[i] = '.'
	}
	msg[len(msg)-1] = '\n'

	// prep buffer
	n := writeBuffer / len(msg)
	buffer := make([]byte, n*len(msg))
	for i := 0; i < n; i++ {
		copy(buffer[i*len(msg):], msg)
	}

	k := 0
	for i := 0; i < messageCount; i++ {
		// update target and payload
		writeHex(rand.Int63n(int64(numCon)), buffer[k*len(msg)+6:])
		writeHex(time.Now().UnixNano(), buffer[k*len(msg)+23:])
		if k++; k == n {
			k = 0
			if w, err := c.Write(buffer); err != nil || w != len(buffer) {
				fmt.Fprintf(os.Stderr, "ucast failed %v\n", err)
				// FIXME: fix end condition to account for failed send?
			}
		}
	}
	if k != 0 {
		if w, err := c.Write(buffer[:k*len(msg)]); err != nil || w != k*len(msg) {
			fmt.Fprintf(os.Stderr, "ucast failed %v\n", err)
		}
	}
}

func makeBenchFunc(numCon, numSub int, messageType string, messageCount, messageSize, writeBuffer int) benchFunc {
	if messageType == "UCAST" {
		return func(c *conn, w *sync.WaitGroup) {
			w.Done()
			w.Wait()
			dcast('U', c.c, numCon, messageCount, messageSize, writeBuffer)
		}
	} else if messageType == "MCAST" {
		return func(c *conn, w *sync.WaitGroup) {
			// TODO: sub
			w.Done()
			w.Wait()
			//dcast('M', c.c, numCon, numSub, messageCount, messageSize, writeBuffer)
		}
	} else if messageType == "BCAST" {
		return func(c *conn, w *sync.WaitGroup) {
			// TODO: sub
			w.Done()
			w.Wait()
			//bcast(c.c, numCon, numSub, messageCount, messageSize)
		}
	}
	fmt.Fprintf(os.Stderr, "unknown message type %s\n", messageType)
	Usage()
	return nil
}

type conn struct {
	c net.Conn
	r *bufio.Reader
	i int
}

func newConnection(address string, cfg *tls.Config, user int, cred string, ws, we *sync.WaitGroup, fn benchFunc) {
	c, err := net.Dial("tcp", address)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect failed: %s\n", err.Error())
		os.Exit(2)
	}
	c.(*net.TCPConn).SetNoDelay(true)
	if cfg != nil {
		c = tls.Client(c, cfg)
	}
	cc := &conn{
		c: c,
		i: user,
		r: bufio.NewReaderSize(c, 1024),
	}
	cc.Do(cred, ws, we, fn)
}

func (c *conn) Do(cred string, ws, we *sync.WaitGroup, fn benchFunc) {
	defer c.c.Close()
	var auth string
	if len(cred) == 0 {
		auth = "open"
	} else {
		auth = "secret " + cred
	}
	var err error
	d := make([]byte, 24+len(auth))
	copy(d, []byte("LOGIN "))
	writeHex(int64(c.i), d[6:])
	d[22] = ' '
	copy(d[23:], []byte(auth))
	d[len(d)-1] = '\n'

	if _, err = c.c.Write(d); err == nil {
		if d, err = c.r.ReadSlice('\n'); err == nil {
			if len(d) < 3 || d[0] != '2' || d[1] != '0' || d[2] != '0' {
				err = fmt.Errorf("unexpected response: %s", string(d[:len(d)-1]))
			}
		}
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "login failed: %s\n", err.Error())
		os.Exit(2)
	}

	go c.readLoop(we)

	fn(c, ws)
	we.Wait()
}

var ping = []byte("000 . PING")
var pong = []byte("PONG\n")

func (c *conn) readLoop(we *sync.WaitGroup) {
	for {
		l, err := c.r.ReadSlice('\n')
		if err != nil {
			if nerr, ok := err.(*net.OpError); !ok ||
				nerr.Err.Error() != "use of closed network connection" {
				fmt.Fprintf(os.Stderr, "%s\n", err.Error())
			}
			break
		}
		l = l[:len(l)-1]
		if len(l) < 3 || (l[0] != '0' && l[0] != '2') {
			fmt.Fprintf(os.Stderr, "unexpected message: %s\n", string(l))
			os.Exit(2)
		}
		if l[0] == '2' {
			continue
		}

		if bytes.Equal(l, ping) {
			c.c.Write(pong)
			continue
		}

		// count received message
		n := atomic.AddInt64(&stats.received, 1)

		// derive end-to-end latency from payload
		payloadIdx := 27
		// BCAST doesn't have a target id
		if l[21] != 'B' {
			payloadIdx += 17
		}
		if len(l) <= payloadIdx+15 {
			fmt.Fprintf(os.Stderr, "unexpected event: %d %s\n", payloadIdx, string(l))
			os.Exit(2)
		}
		timestamp := readHex(l[payloadIdx:])
		latency := time.Since(time.Unix(0, timestamp)).Nanoseconds()

		// streaming stats computation
		us := latency / 1000
		atomic.AddInt64(&stats.total, us)
		k := 0
		for us >= 10 && k < len(stats.bucket)-1 {
			us /= 10
			k++
		}
		atomic.AddInt64(&stats.bucket[k], 1)
		for {
			min := atomic.LoadInt64(&stats.min)
			if latency >= min || atomic.CompareAndSwapInt64(&stats.min, min, latency) {
				break
			}
		}
		for {
			max := atomic.LoadInt64(&stats.max)
			if latency <= max || atomic.CompareAndSwapInt64(&stats.max, max, latency) {
				break
			}
		}

		if n == stats.expected {
			we.Done()
		}
	}
}

var stats struct {
	expected int64
	received int64

	// latency in ns
	min int64
	max int64

	// cumulative latency in us
	total int64

	// histogram
	// small bucket: <10us
	// increment: x10
	bucket [8]int64
}

func main() {
	insecure := flag.Bool("insecure", false, "disable TLS")
	certFile := flag.String("cert", "", "path to client cert")
	keyFile := flag.String("key", "", "path to client key")
	cacertFile := flag.String("cacert", "", "path to CA cert")
	secret := flag.String("secret", "", "shared secret to use for auth")
	numCon := flag.Int("conn", 100, "number of client connections")
	numSub := flag.Int("sub", 10, "number of subscribers per topic")
	messageType := flag.String("type", "UCAST", "message type: {UCAST, MCAST, BCAST}")
	messageCount := flag.Int("count", 10000, "number of messages sent per connection")
	messageSize := flag.Int("size", 100, "payload size in bytes [16, 980]")
	writeBuffer := flag.Int("writebuf", 1024, "write buffer size in bytes")
	cpuprofile := flag.String("cpuprofile", "", "write cpu profile to file")
	memprofile := flag.String("memprofile", "", "write memory profile to this file")
	flag.Parse()

	if flag.NArg() < 1 {
		Usage()
	}

	// FIXME: BCAST can actually do larger payload because it doesn't include a target
	if *messageSize < 16 || *messageSize > 980 {
		Usage()
	}
	*messageSize -= 16

	// FIXME: careful about 32bit overflow...
	total := *numCon * *messageCount

	var err error
	var tlsCfg *tls.Config
	if !*insecure {
		if tlsCfg, err = LoadTLSConfig(*keyFile, *certFile, *cacertFile); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to load cert or key: %s\n", err.Error())
			Usage()
		}
	}

	address := flag.Arg(0)
	b := makeBenchFunc(*numCon, *numSub, *messageType, *messageCount, *messageSize, *writeBuffer)

	var ws, we sync.WaitGroup
	ws.Add(*numCon)
	we.Add(1)
	stats.min = 0xfffffffffffffff
	stats.expected = int64(total)

	for i := 0; i < *numCon; i++ {
		go newConnection(address, tlsCfg, i, *secret, &ws, &we, b)
	}

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err == nil {
			pprof.StartCPUProfile(f)
			defer pprof.StopCPUProfile()
		} else {
			fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		}
	}

	fmt.Fprint(os.Stderr, "wait for start\n")
	ws.Wait()
	start := time.Now()
	fmt.Fprint(os.Stderr, "wait for reception\n")
	we.Wait()
	end := time.Now()
	elapsed := end.Sub(start)
	fmt.Fprintf(os.Stderr, "total=%d ms throughput=%d msg/s latency={min=%v us avg=%v us max=%v us}\n",
		elapsed.Nanoseconds()/1000/1000,
		(int)(float64(total)/elapsed.Seconds()),
		stats.min/1000,
		float64(stats.total)/float64(stats.received),
		stats.max/1000,
	)
	fmt.Fprintf(os.Stderr, "latency distribution:\n <  10us: %d\n < 100us: %d\n <   1ms: %d\n <  10ms: %d\n < 100ms: %d\n <   1s: %d\n <  10s: %d\n >= 10s: %d\n",
		stats.bucket[0], stats.bucket[1], stats.bucket[2], stats.bucket[3], stats.bucket[4], stats.bucket[5], stats.bucket[6], stats.bucket[7],
	)
	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		}
		pprof.WriteHeapProfile(f)
		f.Close()
	}
}

func u(f float64, _ error) float64 {
	return f
}

func Usage() {
	fmt.Fprintf(os.Stderr, "%s <address> [flags]\n", os.Args[0])
	flag.PrintDefaults()
	os.Exit(1)
}

var errInvalidCert = fmt.Errorf("invalid cert")

func certFromFile(file string) (*x509.Certificate, error) {
	d, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, err
	}
	b, _ := pem.Decode(d)
	if b == nil || b.Type != "CERTIFICATE" {
		return nil, errInvalidCert
	}
	return x509.ParseCertificate(b.Bytes)
}

func LoadTLSConfig(keyFile, certFile, cacertFile string) (*tls.Config, error) {
	cacert, err := certFromFile(cacertFile)
	if err != nil {
		return nil, err
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	x509, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return nil, err
	}
	return NewTLSConfig(cert.PrivateKey, x509, cacert), nil
}

func NewTLSConfig(key crypto.PrivateKey, cert *x509.Certificate, cacert *x509.Certificate) *tls.Config {
	roots := x509.NewCertPool()
	roots.AddCert(cacert)
	// lock down TLS config to 1.2 or higher and safest available ciphersuites
	return &tls.Config{
		MinVersion: tls.VersionTLS12,
		CipherSuites: []uint16{
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
			tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA,
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA,
		},
		RootCAs: roots,
		Certificates: []tls.Certificate{
			tls.Certificate{
				Certificate: [][]byte{
					cert.Raw,
					cacert.Raw,
				},
				PrivateKey: key,
				Leaf:       cert,
			},
		},
		ClientAuth: tls.VerifyClientCertIfGiven,
		ClientCAs:  roots,
	}
}
