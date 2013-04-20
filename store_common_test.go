package store

import (
	"bufio"
	"crypto/rand"
	"os"
	"math/big"
	"strings"
	"sync"
	"runtime"
)

type rnd struct {
	f *os.File
	max int64
	l *sync.Mutex
	buf chan string
}

type wordpair struct {
	a *WordAddress
	w string
}


func newrnd(file string) (*rnd, error) {
	r := new(rnd)
	f, e := os.Open(file)
	if e != nil {
		return nil, e
	}
	r.f = f
	max, e := r.f.Seek(0, 2)
	max = max-1
	if e != nil {
		return nil, e
	}
	r.max = max-20
	r.l = new(sync.Mutex)
	r.buf = make(chan string, runtime.GOMAXPROCS(0))
	for i:= 0; i < len(r.buf); i++ {
		go r.putbuf()
	}
	go r.putbuf()
	return r, nil
}

func (r *rnd) close() {
	r.f.Close()
}

func (r *rnd) putbuf() {
	idx, e := rand.Int(rand.Reader, big.NewInt(r.max))
	if e != nil {
		panic(e)
	}
	r.l.Lock()
	_, e = r.f.Seek(idx.Int64(), 0)
	if e != nil {
		panic(e)
	}
	b := bufio.NewReader(r.f)
	str, e := b.ReadString('\n')
	if e != nil {
		panic(e)
	}
	str, e = b.ReadString('\n')
	if e != nil {
		panic(e)
	}
	r.l.Unlock()
	r.buf <-str
}
func (r *rnd) word() string {
	go r.putbuf()
	return strings.Trim(<-r.buf, "\n")
}

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}