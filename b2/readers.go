package b2

import (
	"fmt"
	"hash"
	"io"
)

type HashedPostfixedReader struct {
	R io.ReadCloser
	H hash.Hash

	finished bool
	hexRem   []byte
}

func (r *HashedPostfixedReader) Read(p []byte) (int, error) {
	if r.finished {
		rem := copy(p, r.hexRem)
		r.hexRem = r.hexRem[rem:]
		if len(r.hexRem) == 0 {
			return rem, io.EOF
		} else {
			return rem, nil
		}
	}

	n, err := r.R.Read(p)
	if n > 0 {
		r.H.Write(p[:n])
	}
	if err == io.EOF {
		r.finished = true
		r.hexRem = []byte(fmt.Sprintf("%x", r.H.Sum(nil)))
		if n < len(p) {
			rem := copy(p[n:], r.hexRem)
			r.hexRem = r.hexRem[rem:]
			n += rem
		}

		if len(r.hexRem) > 0 {
			err = nil
		}
	}

	return n, err
}

func (r *HashedPostfixedReader) Close() error {
	return r.R.Close()
}
