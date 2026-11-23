package batch

import (
	"encoding/base64"
	"encoding/binary"
	"strings"
)

// videoTokensPerSecond is what Gemini bills for a second of video at the
// default resolution: a frame per second plus its audio.
const videoTokensPerSecond = 300

// videoTokens estimates the tokens of an inline video from its size, which
// covers streams down to 240 kbps, and from the duration an MP4 or QuickTime
// header declares, which covers slower streams.
func videoTokens(base64Data string) int {
	decodedSize := len(base64Data) * 3 / 4
	tokens := decodedSize/videoBytesPerToken + 1
	if seconds := mp4DurationSeconds(base64Data); seconds > 0 {
		tokens = max(tokens, (seconds+1)*videoTokensPerSecond)
	}
	return tokens
}

// mp4DurationSeconds reads the movie duration from the mvhd box of an MP4 or
// QuickTime file, or returns 0.
func mp4DurationSeconds(base64Data string) int {
	data, err := base64.StdEncoding.DecodeString(strings.TrimRight(base64Data, "\n"))
	if err != nil {
		return 0
	}
	moov := findBox(data, "moov")
	if moov == nil {
		return 0
	}
	mvhd := findBox(moov, "mvhd")
	if len(mvhd) < 20 {
		return 0
	}
	var timescale, duration uint64
	if mvhd[0] == 1 {
		if len(mvhd) < 32 {
			return 0
		}
		timescale = uint64(binary.BigEndian.Uint32(mvhd[20:24]))
		duration = binary.BigEndian.Uint64(mvhd[24:32])
	} else {
		timescale = uint64(binary.BigEndian.Uint32(mvhd[12:16]))
		duration = uint64(binary.BigEndian.Uint32(mvhd[16:20]))
	}
	if timescale == 0 {
		return 0
	}
	seconds := duration / timescale
	if seconds > 24*3600 {
		return 24 * 3600
	}
	return int(seconds)
}

// findBox returns the payload of the first box of the given type among the
// boxes in data.
func findBox(data []byte, boxType string) []byte {
	for len(data) >= 8 {
		size := uint64(binary.BigEndian.Uint32(data[:4]))
		header := uint64(8)
		switch size {
		case 0:
			size = uint64(len(data))
		case 1:
			if len(data) < 16 {
				return nil
			}
			size = binary.BigEndian.Uint64(data[8:16])
			header = 16
		}
		if size < header || size > uint64(len(data)) {
			return nil
		}
		if string(data[4:8]) == boxType {
			return data[header:size]
		}
		data = data[size:]
	}
	return nil
}
