package crkv

import (
	"errors"
	"reflect"

	"github.com/gogo/protobuf/proto"
)

// i2Bytes is a helper function to convert variables to bytes
func i2Bytes(i interface{}) ([]byte, error) {
	b := []byte{}
	switch value := i.(type) {
	case proto.Message:
		err := error(nil)
		b, err = proto.Marshal(value)
		if err != nil {
			return nil, err
		}
	case string:
		b = []byte(value)
	case []byte:
		b = value
	case Value:
		b = value.RawBytes
	case Key:
		b = value.RawBytes
	default:
		typ := reflect.TypeOf(value).Name()
		return nil, errors.New("unsupported value type " + typ)
	}
	return b, nil
}
