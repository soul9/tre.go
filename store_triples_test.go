package store

import "testing"

func TestTripleStore(t *testing.T) {
	for i:=0; i<100; i++ {
		var e error
		s, e := NewMemStore()
		if e != nil {
			panic("Couldn't create store")
		}
		r, e := newrnd("/usr/share/dict/cracklib-small")
		if e != nil {
			panic(e)
		}
		str1 := r.word()
		str2 := r.word()
		str3 := r.word()
		tr := &Triple{}
		tr.Source, e = s.PutStr(str1)
		if e != nil {
			panic(e)
		}
		tr.Relation, e = s.PutStr(str2)
		if e != nil {
			panic(e)
		}
		tr.Destination, e = s.PutStr(str3)
		if e != nil {
			panic(e)
		}
		trr := &Triple{}
		trr, e = s.PutTriple(tr)
		if e != nil {
			t.Errorf("Tried to put %s, got error %s", tr.String(), e)
		}
		trrr := &Triple{}
		trrr, e = s.PutStringTriple(str1, str2, str3)
		if trr.Address.File != trrr.Address.File || trr.Address.Line != trrr.Address.Line {
			t.Errorf("got different triple from PutTripleString: %s, got %s", trr.String(), trrr.String())
		}
		if trr.String() != tr.String() {
			t.Errorf("got different triple from Put: %s, got %s", tr.String(), trr.String())
		}
		tr, e = s.GetTripleByString(trr.String())
		if e != nil || tr.String() != trr.String() {
			t.Errorf("Got something wrong from GetTripleByString: orig %s, got %s (err %s)", trr.String(), tr.String(), e)
		}
		tr, e = s.GetTripleByAddress(trr.Address)
		if e != nil || tr.String() != trr.String() {
			t.Errorf("Got something wrong from GetTripleByAddress: orig %s, got %s (err %s)", trr.String(), tr.String(), e)
		}
	}
}
