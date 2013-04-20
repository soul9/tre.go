package store

import (
  "io"
  "bufio"
  "fmt"
  "strings"
  "sync"
  "crypto/sha512"
)

func (s *Store) GetStrIdx(srch string) *WordAddress {
	fname := s.GetStrFileName(srch)
	s.lock.Lock()
	if _, ok := s.locks[fname]; !ok {
		s.locks[fname] = new(sync.RWMutex)
	}
	s.lock.Unlock()
	s.locks[fname].RLock()
	defer s.locks[fname].RUnlock()
	strf, err := s.open(fname)
	if err != nil {
		return &WordAddress{"", -1}
	}
	defer strf.Close()
	if _, e := strf.Seek(0, 0); e != nil {
		return &WordAddress{"", -1}
	}
	b := bufio.NewReader(strf)
	var idx int64
	for str, e := b.ReadString('\n'); ; str, e = b.ReadString('\n') {
		if strings.Replace(strings.TrimRight(str, "\n"), "\\n", "\n", -1) == srch {
			return &WordAddress{s.GetStrFileName(str), idx}
		}
		if e != nil {
			return &WordAddress{"", -1}
		}
		idx++
	}
	return &WordAddress{"", -1}
}

func (s *Store) GetIdxStr(addr *WordAddress) (string, error) {
	s.lock.Lock()
	if _, ok := s.locks[addr.File]; !ok {
		s.locks[addr.File] = new(sync.RWMutex)
	}
	s.lock.Unlock()
	s.locks[addr.File].RLock()
	defer s.locks[addr.File].RUnlock()
	strf, e := s.open(addr.File)
	if e != nil {
		return "", reterr("GetIdxStr", e)
	}
	defer strf.Close()
	if _, e := strf.Seek(0, 0); e != nil {
		return "", reterr("GetIdxStr", e)
	}
	b := bufio.NewReader(strf)
	var i int64
	var str string
	for i=0; i<=addr.Line ; i++ {
		var e error
		str, e = b.ReadString('\n')
		if e!=nil {
			return "", reterr("GetIdxStr", e)
		}
	}
	str = strings.Replace(strings.TrimRight(str, "\n"), "\\n", "\n", -1)
	return str, nil
}

func (s *Store) writeStr(str string) error {
	fname := s.GetStrFileName(str)
	s.lock.Lock()
	if _, ok := s.locks[fname]; !ok {
		s.locks[fname] = new(sync.RWMutex)
	}
	s.lock.Unlock()
	s.locks[fname].Lock()
	defer s.locks[fname].Unlock()
	strf, err := s.openOrCreate(fname)
	if err != nil {
		return reterr("writeStr", err)
	}
	defer strf.Close()
	if _, err = strf.Seek(0, 2); err != nil {
		return reterr("writeStr", err)
	}
	if _, e := io.WriteString(strf, fmt.Sprintf("%s\n", strings.Replace(str, "\n", "\\n", -1))); e != nil {
		return reterr("writeStr", err)
	}
	return nil
}

func (s *Store) PutStr(str string) (*WordAddress, error) {
	if addr := s.GetStrIdx(str); addr.Line != -1 {
		return addr, nil
	}
	if e := s.writeStr(str); e != nil {
		return &WordAddress{}, reterr("PutStr", e)
	}
	return s.GetStrIdx(str), nil
}

func (s *Store) GetStrFileName(str string) string {
	h := sha512.New()
	io.WriteString(h, strings.TrimRight(str, "\n"))
	return fmt.Sprintf("str/%s", fmt.Sprintf("%x", h.Sum(nil))[:4])
}

func (s *Store) IsStrFileName(str string) bool {
	return strings.HasPrefix(str, "str/")
}
