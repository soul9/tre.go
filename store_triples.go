package store

import (
  "io"
  "bufio"
  "fmt"
  "strings"
  "sync"
  "crypto/sha512"
)

type Triple struct {
	Source, Relation, Destination *WordAddress
	Address *WordAddress
}

func NewTriple(t string) (*Triple, error) {
	sp := strings.Split(t, ";")
	if len(sp) != 3 {
		return nil, fmt.Errorf("NewTriple: Invalid input: %s", t)
	}
	src, e := NewWordAddress(sp[0])
	if e != nil {
		return nil, reterr("NewTriple", e)
	}
	rel, e := NewWordAddress(sp[1])
	if e != nil {
		return nil, reterr("NewTriple", e)
	}
	dst, e := NewWordAddress(sp[2])
	if e != nil {
		return nil, reterr("NewTriple", e)
	}
	return &Triple{src,rel,dst,nil}, nil
}

func (t *Triple) String() string {
	return fmt.Sprintf("%s;%s;%s", t.Source, t.Relation, t.Destination)
}

func (t *Triple) FileName() string {
	h := sha512.New()
	io.WriteString(h, t.Source.String())
	return fmt.Sprintf("%x", h.Sum(nil))[:4]
}

func (s *Store) writeTriple(t *Triple) error {
	fname := s.GetTripleFileName(t)
	if _, ok := s.locks[fname]; !ok {
		s.locks[fname] = new(sync.RWMutex)
	}
	s.locks[fname].Lock()
	defer s.locks[fname].Unlock()
	f, e := s.openOrCreate(fname)
	if e != nil {
		reterr("writeTriple", e)
	}
	defer f.Close()
	if _, e := f.Seek(0, 2); e != nil {
		return reterr("writeTriple", e)
	}
	if _, e := io.WriteString(f, fmt.Sprintf("%s\n", t.String())); e != nil {
		return reterr("writeTriple", e)
	}
	return nil
}

func (s *Store) PutStringTriple(src, rel, dst string) (*Triple, error) {
	t := &Triple{}
	var e error
	t.Source, e = s.PutStr(src)
	if e != nil {
		return nil, reterr("PutStringTriple", e)
	}
	t.Relation, e = s.PutStr(rel)
	if e != nil {
		return nil, reterr("PutStringTriple", e)
	}
	t.Destination, e = s.PutStr(dst)
	if e != nil {
		return nil, reterr("PutStringTriple", e)
	}
	tr := &Triple{}
	tr, e = s.PutTriple(t)
	if e != nil {
		return nil, reterr("PutStringTriple", e)
	}
	return tr, nil
}

func (s *Store) PutTriple(t *Triple) (*Triple, error) {
	if tr, e := s.GetTripleByString(t.String()); e == nil && tr != nil {
		return tr, nil
	}
	if e := s.writeTriple(t); e != nil {
		return nil, reterr("PutTriple", e)
	}
	tr, e := s.GetTripleByString(t.String())
	if e != nil || tr == nil {
		return nil, reterr("PutTriple", e)
	}
	return tr, nil
}

func (s *Store) GetTripleByString(t string) (*Triple, error) {
	tr, e := NewTriple(t)
	if e != nil {
		return nil, reterr("GetTripleByString", e)
	}
	fname := s.GetTripleFileName(tr)
	if _, ok := s.locks[fname]; !ok {
		s.locks[fname] = new(sync.RWMutex)
	}
	s.locks[fname].RLock()
	defer s.locks[fname].RUnlock()
	f, e := s.open(fname)
	if e != nil {
		return nil, reterr("GetTripleByString", e)
	}
	defer f.Close()
	if _, e := f.Seek(0, 0); e != nil {
		return nil, reterr("GetTripleByString", e)
	}
	b := bufio.NewReader(f)
	var idx int64
	for str, e := b.ReadString('\n'); ; str, e = b.ReadString('\n') {
		if strings.TrimRight(str, "\n") == t {
			tr.Address = &WordAddress{fname, idx}
			return tr, nil
		}
		if e != nil {
			return nil, reterr("GetTripleByString", e)
		}
		idx++
	}
	return nil, fmt.Errorf("GetTripleByString: Couldn't find triple")
}

func (s *Store) GetTripleByAddress(addr *WordAddress) (*Triple, error) {
	if _, ok := s.locks[addr.File]; !ok {
		s.locks[addr.File] = new(sync.RWMutex)
	}
	s.locks[addr.File].RLock()
	defer s.locks[addr.File].RUnlock()
	f, e := s.open(addr.File)
	if e != nil {
		return nil, reterr("GetTripleByAddress", e)
	}
	defer f.Close()
	if _, e := f.Seek(0, 0); e != nil {
		return nil, reterr("GetTripleByAddress", e)
	}
	b := bufio.NewReader(f)
	var str string
	for i:=int64(0); i<=addr.Line ; i++ {
		var e error
		str, e = b.ReadString('\n')
		if e!=nil {
			return nil, reterr("GetTriple", e)
		}
	}
	str = strings.TrimRight(str, "\n")
	tr, e := NewTriple(str)
	if e != nil {
			return nil, reterr("GetTripleByAddress", e)
	}
	tr.Address = addr
	return tr, nil
}

func (s *Store) GetTriples(t *Triple) []*Triple {
	return nil
}

func (s *Store) GetTripleFileName(t *Triple) string {
	return fmt.Sprintf("trpl/%s", t.FileName())
}

func (s *Store) IsTripleFileName(str string) bool {
	return strings.HasPrefix(str, "trpl/")
}
