package gserver

import "fmt"

type byteRange struct {
	offset int64
	length int64
}

func planByteRanges(size int64, streams int) ([]byteRange, error) {
	if size < 0 {
		return nil, fmt.Errorf("size must be >= 0")
	}
	if streams < 1 {
		streams = 1
	}
	if size == 0 {
		return nil, nil
	}
	if int64(streams) > size {
		streams = int(size)
	}
	ranges := make([]byteRange, 0, streams)
	base := size / int64(streams)
	rem := size % int64(streams)
	var offset int64
	for i := 0; i < streams; i++ {
		length := base
		if int64(i) < rem {
			length++
		}
		ranges = append(ranges, byteRange{offset: offset, length: length})
		offset += length
	}
	return ranges, nil
}
