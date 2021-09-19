package hw02unpackstring

import (
	"errors"
	"strconv"
	"strings"
	"unicode"
)

var ErrInvalidString = errors.New("invalid string")

func Unpack(srcStr string) (string, error) {
	var result strings.Builder
	tmp := make([]rune, len(srcStr))
	for index, str := range srcStr {
		tmp[index] = str
	}
	for i := 0; i < len(tmp)-1; i++ {
		if !unicode.IsDigit(tmp[i]) {
			t, err := strconv.Atoi(string(tmp[i+1]))
			if err == nil {
				result.WriteString(strings.Repeat(string(tmp[i]), t))
			} else {
				result.WriteRune(tmp[i])
			}
		} else if _, err := strconv.Atoi(string(tmp[i+1])); err == nil || i == 0 {
			return "", ErrInvalidString
		}
	}
	// check the last
	if len(tmp) >= 1 {
		if unicode.IsDigit(tmp[0]) {
			return "", ErrInvalidString
		}
		if !unicode.IsDigit(tmp[len(tmp)-1]) {
			result.WriteRune(tmp[len(tmp)-1])
		}
	}

	return result.String(), nil
}
