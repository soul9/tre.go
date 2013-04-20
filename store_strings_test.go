package store

import "testing"

func TestStringStore(t *testing.T) {
	s, e := NewMemStore()
	if e != nil {
		panic("Couldn't create store")
	}
	if addr := s.GetStrIdx("doesntexist"); addr.Line != -1 {
		t.Errorf("Tried to get nonexistant string, but got %#v", addr)
	}
	r, e := newrnd("/tmp/quads")
	if e != nil {
		panic(e)
	}
	for j:=0; j<100; j++ {
		st := make(map[string]*WordAddress)
		ch := make(chan wordpair, 50)
		for i:=0; i<100; i++ {
			go func() {
				str := r.word()
				var r *WordAddress
				if r, e = s.PutStr(str); r.Line == -1 || e != nil {
					t.Errorf("Failed to put %s\n (err %s)", str, e)
					ch <- wordpair{}
					return
				}
				ch <- wordpair{r, str}
				return
			}()
		}
		for i:=0; i<100; i++ {
			wp := <-ch
			st[wp.w] = wp.a
		}
		for word, addr := range st {
			go func(wp wordpair) {
				if a := s.GetStrIdx(wp.w); *a != *wp.a {
					t.Errorf("Invalid address for %s should be %#v but got %#v", wp.w, wp.a, a)
				}
			}(wordpair{addr, word})
		}
		for word, addr := range st {
			go func(wp wordpair) {
				if w, e := s.GetIdxStr(wp.a); w != wp.w || e != nil {
					t.Errorf("Invalid value for %#v should be %s but got %s (err %s)", wp.a, wp.w, w, e)
				}
			}(wordpair{addr, word})
		}
	}
}
