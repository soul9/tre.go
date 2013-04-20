package store

import (
  "os"
  "fmt"
  "strings"
  "sync"
  "strconv"
)

// Type to identify any type represented in the store, in this case it's just filepath:lineno
type WordAddress struct {
	File string
	Line int64
}

// A store in RootDir directory
type Store struct {
	RootDir	string
	locks map[string]*sync.RWMutex
}

// Parse a string to a WordAddress
func NewWordAddress(s string) (*WordAddress, error) {
	sp := strings.Split(s, ":")
	if len(sp) != 2 {
		return nil, fmt.Errorf("NewWordAddress: Invalid input: %s", s)
	}
	addr, e := strconv.ParseInt(sp[1], 10, 64)
	if e != nil {
		return nil, reterr("NewWordAddress", e)
	}
	return &WordAddress{sp[0], addr}, nil
}

// Format a WordAddress as it's supposed to be written to file
func (w *WordAddress) String() string {
	return fmt.Sprintf("%s:%d", w.File, w.Line)
}

// Creates a store in directory specified by root
func NewStore(root string) (*Store, error) {
	if err := mkDirOrExist(fmt.Sprintf("%s", root)); err != nil {
		return nil, reterr("NewStore", err)
	}
	if err := mkDirOrExist(fmt.Sprintf("%s/str", root)); err != nil {
		return nil, reterr("NewStore", err)
	}
	if err := mkDirOrExist(fmt.Sprintf("%s/trpl", root)); err != nil {
		return nil, reterr("NewStore", err)
	}
	return &Store{root, make(map[string]*sync.RWMutex)}, nil
}

// Creates a new store in RAM (/dev/shm/)
func NewMemStore() (*Store, error) {
	return NewStore("/dev/shm/tre")
}

// helper functions: not exported
func reterr(pref string, e error) error {
	return fmt.Errorf("%s: %s", pref, e)
}

func mkDirOrExist(dir string) error {
	mode := os.ModeDir^0700
	if err := os.Mkdir(dir, mode); err != nil {
		if fi, e := os.Stat(dir); e != nil || !fi.IsDir() || fi.Mode() != mode {
			return reterr("mkDirOrExist", err)
		}
	}
	return nil
}

func (s *Store) openOrCreate(file string) (*os.File, error) {
	fname := fmt.Sprintf("%s/%s", s.RootDir, file)
	f, e := os.OpenFile(fname, os.O_RDWR^os.O_SYNC^os.O_CREATE, 0600)
	if e != nil {
		return nil, reterr("openOrCreate", e)
	}
	return f, nil
}

func (s *Store) open(file string) (*os.File, error) {
	fname := fmt.Sprintf("%s/%s", s.RootDir, file)
	f, e := os.OpenFile(fname, os.O_RDWR^os.O_SYNC, 0600)
	if e != nil {
		return nil, reterr("open", e)
	}
	return f, nil
}
