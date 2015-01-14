package main

// I implemented this for fun, don't hate.  I know about math.Big.

import (
	"fmt"
)

type bitmap struct {
	bits	[]uint64
	numBits	int
}

func newBitmap(bits int) bitmap {
	return bitmap{
		bits: make([]uint64, (bits + 63) / 64),
		numBits: bits,
	}
}

func (b *bitmap) set(bit int) {
	if bit < 0 {
		panic(fmt.Sprintf("%d < 0", bit))
	} else if bit >= b.numBits {
		panic(fmt.Sprintf("%d >= %d", bit, b.numBits))
	}
	i := bit / 64
	b.bits[i] |= 1 << (uint64(bit) % 64)
}

func mbaobf(b int) uint64 {
	f := uint64(1 << (uint64(b) % 64))
	if f == 1 {
		return ^uint64(0)
	}
	m := uint64(1<<63)
	if f & m > 0 {
		panic(fmt.Sprintf("%d & %d (%d) > 0", f, m, f & m))
	}
	for b := int(62); b >= 0; b-- {
		bb := uint64(1<<uint64(b))
		m |= bb
		if f & m > 0 {
			return ^m
		}
	}
	panic("not reached")
}

func (b bitmap) mustBeAllOnes() {
	for i, v := range b.bits {
		v = ^v
		if i == len(b.bits) - 1 {
			v &= mbaobf(b.numBits)
		}

		if v != 0 {
			panic(fmt.Sprintf("value %d at index %d (%d) is not all ones (numBits %d)", b.bits[i], i, v, b.numBits))
		}
	}
}

func (b *bitmap) merge(a bitmap) {
	if b.numBits != a.numBits {
		panic(fmt.Sprintf("%d != %d", b.numBits, a.numBits))
	}
	for i, v := range a.bits {
		if b.bits[i] & v > 0 {
			panic(fmt.Sprintf("%d: %d & %d > 0", i, b.bits[i], v))
		}
		b.bits[i] = b.bits[i] | v
	}
}

