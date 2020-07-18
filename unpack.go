package erlpack

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"github.com/jakemakesstuff/structs"
	"reflect"
	"unsafe"
)

var errorInterface = reflect.TypeOf((*error)(nil)).Elem()

var uncastedResultType = reflect.TypeOf((*UncastedResult)(nil))

// Atom is used to define an atom within the codebase.
type Atom string

// RawData is used to define data which was within an Erlpack array but has not been parsed yet.
// This is different to UncastedResult since it has not been processed yet.
type RawData []byte

// Cast is used to cast the result to a pointer.
func (r RawData) Cast(Ptr interface{}) error {
	v := &pointerSetter{ptr: reflect.ValueOf(Ptr)}
	if v.ptr.Kind() != reflect.Ptr {
		return errors.New("invalid pointer")
	}
	return processItem(v, bytes.NewReader(r))
}

// UncastedResult is used to define a result which has not been casted yet.
// You can call Cast on this to cast the item after the initial unpacking.
type UncastedResult struct {
	item interface{}
}

// Cast is used to cast the result to a pointer.
func (u *UncastedResult) Cast(Ptr interface{}) error {
	v := &pointerSetter{ptr: reflect.ValueOf(Ptr)}
	if v.ptr.Kind() != reflect.Ptr {
		return errors.New("invalid pointer")
	}
	return handleItemCasting(u.item, v)
}

// Used to cast the item.
func handleItemCasting(Item interface{}, setter *pointerSetter) error {
	// Get the base pointer.
	Ptr := setter.getBasePtr()

	// Handle a interface or uncasted result.
	switch Ptr.(type) {
	case *interface{}:
		return setter.set(reflect.ValueOf(&Item))
	case *UncastedResult:
		return setter.set(reflect.ValueOf(&UncastedResult{item: Item}))
	}

	// Handle specific type casting.
	switch x := Item.(type) {
	case Atom:
		switch Ptr.(type) {
		case *Atom:
			return setter.set(reflect.ValueOf(&x))
		}
	case int64:
		switch Ptr.(type) {
		case *int:
			p := int(x)
			return setter.set(reflect.ValueOf(&p))
		case *int64:
			return setter.set(reflect.ValueOf(&x))
		default:
			return errors.New("could not de-serialize into int")
		}
	case int32:
		switch Ptr.(type) {
		case *int:
			p := int(x)
			return setter.set(reflect.ValueOf(&p))
		case *int32:
			return setter.set(reflect.ValueOf(&x))
		default:
			return errors.New("could not de-serialize into int")
		}
	case float64:
		switch Ptr.(type) {
		case *float64:
			return setter.set(reflect.ValueOf(&x))
		default:
			return errors.New("could not de-serialize into float64")
		}
	case uint8:
		switch Ptr.(type) {
		case *uint:
			p := uint(x)
			return setter.set(reflect.ValueOf(&p))
		case *uint8:
			return setter.set(reflect.ValueOf(&x))
		case *int:
			p := int(x)
			return setter.set(reflect.ValueOf(&p))
		default:
			return errors.New("could not de-serialize into uint8")
		}
	case string:
		// Map key.
		switch Ptr.(type) {
		case *string:
			p := x
			return setter.set(reflect.ValueOf(&p))
		default:
			return errors.New("could not de-serialize into string")
		}
	case []byte:
		// We should try and string-ify this if possible.
		switch Ptr.(type) {
		case *string:
			p := string(x)
			return setter.set(reflect.ValueOf(&p))
		case *[]byte:
			return setter.set(reflect.ValueOf(&x))
		default:
			return errors.New("could not de-serialize into string")
		}
	case bool:
		// This should cast into either a string or a boolean.
		switch Ptr.(type) {
		case *Atom:
			// Set it to a string representation of the value.
			var p Atom
			if x {
				p = "true"
			} else {
				p = "false"
			}
			return setter.set(reflect.ValueOf(&p))
		case *bool:
			// Set it to the raw value.
			return setter.set(reflect.ValueOf(&x))
		}
	case nil:
		// This should zero any data types other than atoms.
		switch Ptr.(type) {
		case *Atom:
			// We should set this to "nil".
			nils := (Atom)("nil")
			return setter.set(reflect.ValueOf(&nils))
		default:
			// Set to nil.
			return setter.set(reflect.ValueOf(Ptr))
		}
	case []interface{}:
		// We should handle this array.
		switch Ptr.(type) {
		case *[]interface{}:
			// This is simple.
			return setter.set(reflect.ValueOf(&x))
		default:
			// Get the reflect value.
			r := reflect.MakeSlice(reflect.ValueOf(Ptr).Type().Elem(), len(x), len(x))

			// Set all the items.
			for i, v := range x {
				indexItem := r.Index(i)
				x := reflect.New(indexItem.Type())
				t := x.Interface()
				err := handleItemCasting(v, &pointerSetter{ptr: reflect.ValueOf(t)})
				if err != nil {
					return err
				}
				indexItem.Set(x.Elem())
			}

			// Create the pointer.
			ptr := reflect.New(reflect.PtrTo(r.Type()).Elem())
			ptr.Elem().Set(r)
			return setter.set(ptr)
		}
	case map[interface{}]interface{}:
		// Maps are complicated since they can serialize into a lot of different types.
		switch Ptr.(type) {
		case *map[interface{}]interface{}:
			// This is the first thing we check for since it is by far the best situation.
			return setter.set(reflect.ValueOf(&x))
		}

		// Check the type of the pointer.
		switch e := reflect.ValueOf(Ptr).Type().Elem(); e.Kind() {
		case reflect.Struct:
			// Make the new struct.
			i := reflect.New(e)

			// Check if the struct has a "UncastedErlpack" function. If so, call that and return any errors.
			function := i.MethodByName("UncastedErlpack")
			if function.IsValid() {
				if function.Type().NumIn() != 1 {
					return errors.New("only *UncastedResult is expected as an argument")
				}
				if function.Type().In(0) != uncastedResultType {
					return errors.New("only *UncastedResult is expected as a result")
				}
				if function.Type().NumOut() != 1 {
					return errors.New("only error is expected as a result")
				}
				if !function.Type().Out(0).Implements(errorInterface) {
					return errors.New("result is not error")
				}
				f := function.Interface().(func(*UncastedResult) error)
				return f(&UncastedResult{item: Item})
			}

			// Get the struct object.
			s := structs.New(i.Interface())
			s.TagName = "erlpack"

			// Set tag > field.
			tag2field := map[string]string{}
			for _, field := range s.Fields() {
				t := field.Tag("erlpack")
				if t != "-" {
					if t == "" {
						tag2field[field.Name()] = field.Name()
						continue
					}
					tag2field[t] = field.Name()
				}
			}

			// Iterate through the map.
			for k, v := range x {
				switch str := k.(type) {
				case string:
					fieldName, ok := tag2field[str]
					if !ok {
						continue
					}
					field, ok := s.FieldOk(fieldName)
					if !ok {
						return errors.New("failed to get field")
					}
					r := reflect.New(field.Type())
					x := r.Interface()
					err := handleItemCasting(v, &pointerSetter{ptr: reflect.ValueOf(x)})
					if err != nil {
						return err
					}
					err = field.Set(r.Elem().Interface())
					if err != nil {
						return err
					}
				default:
					return errors.New("key must be string")
				}
			}

			// Create the pointer.
			ptr := reflect.New(reflect.PtrTo(e).Elem())
			ptr.Elem().Set(i.Elem())
			return setter.set(ptr)
		case reflect.Map:
			// Make the new map.
			m := reflect.MakeMap(e)

			// Get the key type.
			keyType := m.Type().Key()

			// Get the value type.
			valueType := m.Type().Elem()

			// Iterate through the map.
			for k, v := range x {
				// Create a new version of the key with the reflect type.
				reflectKey := reflect.Zero(keyType)
				pptr := reflect.New(reflect.PtrTo(reflectKey.Type()).Elem())
				pptr.Elem().Set(reflectKey)

				// Handle the item casting for the key.
				err := handleItemCasting(k, &pointerSetter{ptr: pptr})
				if err != nil {
					return err
				}
				reflectKey = pptr.Elem()

				// Create a new version of the value with the reflect type.
				reflectValue := reflect.Zero(valueType)
				pptr = reflect.New(reflect.PtrTo(reflectValue.Type()).Elem())
				pptr.Elem().Set(reflectValue)

				// Handle the item casting for the value.
				err = handleItemCasting(v, &pointerSetter{ptr: pptr})
				if err != nil {
					return err
				}
				reflectValue = pptr.Elem()

				// Set the item.
				if !pptr.IsNil() {
					m.SetMapIndex(reflectKey, reflectValue)
				}
			}

			// Create the pointer.
			ptr := reflect.New(reflect.PtrTo(e).Elem())
			ptr.Elem().Set(m)
			return setter.set(ptr)
		}
	}

	// Return unknown type error.
	return errors.New("unable to unpack to pointer specified")
}

// Used to process an atom during unpacking.
func processAtom(Data []byte) interface{} {
	matchRest := func(d []byte) bool {
		if len(d) > len(Data)-1 {
			return false
		}
		for i := 0; i < len(d); i++ {
			if Data[i+1] != d[i] {
				return false
			}
		}
		return true
	}
	switch Data[0] {
	case 't':
		matched := matchRest([]byte("rue"))
		if !matched {
			return Atom(Data)
		}
		return true
	case 'f':
		matched := matchRest([]byte("alse"))
		if !matched {
			return Atom(Data)
		}
		return false
	case 'n':
		matched := matchRest([]byte("il"))
		if !matched {
			return Atom(Data)
		}
		return nil
	default:
		return Atom(Data)
	}
}

// Process the raw data.
func processRawData(DataType byte, setter *pointerSetter, r *bytes.Reader, jsonType bool) error {
	// Defines the byte array it'll go into.
	var bytes []byte

	// Get the right data type.
	switch DataType {
	case 's': // atom
		if r.Len() == 0 {
			// Byte slice is too small.
			return errors.New("atom information missing")
		}
		b, _ := r.ReadByte()
		Len := int(b)
		bytes = make([]byte, Len+2)
		bytes[0] = 's'
		bytes[1] = b
		for Total := 0; Total != Len; Total++ {
			b, err := r.ReadByte()
			if err != nil {
				return errors.New("atom size larger than remainder of array")
			}
			bytes[Total+2] = b
		}
	case 'j': // blank list
		bytes = []byte{'j'}
	case 'l': // list
		// Get the length of the list.
		lengthBytes := make([]byte, 4)
		_, err := r.Read(lengthBytes)
		if err != nil {
			return errors.New("not enough bytes for list length")
		}
		l := binary.BigEndian.Uint32(lengthBytes)
		bytes = make([]byte, 5, (l*3)+5)
		bytes[0] = 'l'
		for i := uint8(0); i < 4; i++ {
			bytes[i+1] = lengthBytes[i]
		}

		// Try and get each item from the list.
		for i := 0; i < int(l); i++ {
			DataType, err := r.ReadByte()
			if err != nil {
				return errors.New("not long enough to include data type")
			}
			var raw RawData
			itemSetter := &pointerSetter{ptr: reflect.ValueOf(&raw)}
			if err = processRawData(DataType, itemSetter, r, false); err != nil {
				return err
			}
			bytes = append(bytes, raw...)
		}
	case 'm': // string
		// Get the length of the string.
		lengthBytes := make([]byte, 4)
		_, err := r.Read(lengthBytes)
		if err != nil {
			return errors.New("not enough bytes for list length")
		}
		l := binary.BigEndian.Uint32(lengthBytes)

		// Create the byte array.
		Len := int(l)
		bytes = make([]byte, Len+5)
		bytes[0] = 'm'
		for i := uint8(0); i < 4; i++ {
			bytes[i+1] = lengthBytes[i]
		}

		// Write each byte.
		for Total := 0; Total != Len; Total++ {
			b, err := r.ReadByte()
			if err != nil {
				return errors.New("atom size larger than remainder of array")
			}
			bytes[Total+5] = b
		}
	case 'a': // small int
		i, err := r.ReadByte()
		if err != nil {
			return errors.New("failed to read small int")
		}
		bytes = []byte{'a', i}
	case 'b': // int32
		b := make([]byte, 4)
		_, err := r.Read(b)
		if err != nil {
			return errors.New("not enough bytes for int32")
		}
		bytes = append([]byte{'b'}, b...)
	case 'n': // int64
		// Get the number of encoded bytes.
		encodedBytes, err := r.ReadByte()
		if err != nil {
			return errors.New("unable to read int64 byte count")
		}

		// Create the byte array.
		bytes = make([]byte, encodedBytes+2)
		bytes[0] = 'n'
		bytes[1] = encodedBytes

		// Write each byte.
		for Total := uint8(0); Total != encodedBytes; Total++ {
			b, err := r.ReadByte()
			if err != nil {
				return errors.New("int size larger than remainder of array")
			}
			bytes[Total+2] = b
		}
	case 'F': // float
		// Get the next 8 bytes.
		bytes = make([]byte, 9)
		bytes[0] = 'F'
		for i := uint8(0); i < 8; i++ {
			b, err := r.ReadByte()
			if err != nil {
				return errors.New("float size larger than remainder of array")
			}
			bytes[i+1] = b
		}
	case 't': // map
		// Get the length of the map.
		lengthBytes := make([]byte, 4)
		_, err := r.Read(lengthBytes)
		if err != nil {
			return errors.New("not enough bytes for list length")
		}
		l := binary.BigEndian.Uint32(lengthBytes)
		bytes = make([]byte, 5, (l*6)+5)
		bytes[0] = 't'
		for i := uint8(0); i < 4; i++ {
			bytes[i+1] = lengthBytes[i]
		}

		// Try and get each item from the map.
		for i := 0; i < int(l); i++ {
			DataType, err := r.ReadByte()
			if err != nil {
				return errors.New("not long enough to include data type")
			}
			var raw RawData
			itemSetter := &pointerSetter{ptr: reflect.ValueOf(&raw)}
			if err = processRawData(DataType, itemSetter, r, false); err != nil {
				return err
			}
			bytes = append(bytes, raw...)
			DataType, err = r.ReadByte()
			if err != nil {
				return errors.New("not long enough to include data type")
			}
			if err = processRawData(DataType, itemSetter, r, false); err != nil {
				return err
			}
			bytes = append(bytes, raw...)
		}
	default:
		return errors.New("unknown data type")
	}

	// Handle processing the pointer.
	if jsonType {
		x := (json.RawMessage)(bytes)
		return setter.set(reflect.ValueOf(&x))
	} else {
		x := (RawData)(bytes)
		return setter.set(reflect.ValueOf(&x))
	}
}

// Processes a item.
func processItem(setter *pointerSetter, r *bytes.Reader) error {
	// Gets the type of data.
	DataType, err := r.ReadByte()
	if err != nil {
		return errors.New("not long enough to include data type")
	}

	// Check if this is meant to be raw data and process that differently if so.
	switch setter.getBasePtr().(type) {
	case *json.RawMessage:
		return processRawData(DataType, setter, r, true)
	case *RawData:
		return processRawData(DataType, setter, r, false)
	}

	// Handle the various different data types.
	var Item interface{}
	switch DataType {
	case 's': // atom
		// Get the atom information.
		if r.Len() == 0 {
			// Byte slice is too small.
			return errors.New("atom information missing")
		}
		b, _ := r.ReadByte()
		Len := int(b)
		Data := make([]byte, Len)
		for Total := 0; Total != Len; Total++ {
			b, err := r.ReadByte()
			if err != nil {
				return errors.New("atom size larger than remainder of array")
			}
			Data[Total] = b
		}
		Item = processAtom(Data)
	case 'j': // blank list
		Item = []interface{}{}
	case 'l': // list
		// Get the length of the list.
		lengthBytes := make([]byte, 4)
		_, err := r.Read(lengthBytes)
		if err != nil {
			return errors.New("not enough bytes for list length")
		}
		l := binary.BigEndian.Uint32(lengthBytes)

		// Try and get each item from the list.
		Item = make([]interface{}, l)
		for i := 0; i < int(l); i++ {
			var x interface{}
			err := processItem(&pointerSetter{ptr: reflect.ValueOf(&x)}, r)
			if err != nil {
				return err
			}
			Item.([]interface{})[i] = x
		}
	case 'm': // string
		// Get the length of the string.
		lengthBytes := make([]byte, 4)
		_, err := r.Read(lengthBytes)
		if err != nil {
			return errors.New("not enough bytes for list length")
		}
		l := binary.BigEndian.Uint32(lengthBytes)

		// Make an array of the specified length.
		Item = make([]byte, l)

		// Write into it if we can.
		_, err = r.Read(Item.([]byte))
		if err != nil {
			return errors.New("string length is longer than remainder of array")
		}
	case 'a': // small int
		i, err := r.ReadByte()
		if err != nil {
			return errors.New("failed to read small int")
		}
		Item = i
	case 'b': // int32
		b := make([]byte, 4)
		_, err := r.Read(b)
		if err != nil {
			return errors.New("not enough bytes for int32")
		}
		l := binary.BigEndian.Uint32(b)
		Item = *(*int32)(unsafe.Pointer(&l))
	case 'n': // int64
		// Get the number of encoded bytes.
		encodedBytes, err := r.ReadByte()
		if err != nil {
			return errors.New("unable to read int64 byte count")
		}

		// Get the signature.
		signatureChar, err := r.ReadByte()
		if err != nil {
			return errors.New("unable to read int64 signature")
		}
		negative := signatureChar == 1

		// Create the uint64.
		u := uint64(0)

		// Decode the int64.
		x := uint64(0)
		for i := 0; i < int(encodedBytes); i++ {
			// Read the next byte.
			b, err := r.ReadByte()
			if err != nil {
				return errors.New("int64 length greater than array")
			}

			// Add the byte.
			u += uint64(b) * x
			x <<= 8
		}

		// Turn the uint64 into a int64.
		if negative {
			Item = int64(u) * -1
		} else {
			Item = int64(u)
		}
	case 'F': // float
		// Get the next 8 bytes.
		encodedBytes := make([]byte, 8)

		// Read said encoded bytes.
		_, err := r.Read(encodedBytes)
		if err != nil {
			return errors.New("not enough bytes to decode")
		}

		// Get the item as a uint64.
		i := binary.BigEndian.Uint64(encodedBytes)

		// Turn it into a float64.
		Item = *(*float64)(unsafe.Pointer(&i))
	case 't': // map
		// Get the length.
		b := make([]byte, 4)
		_, err := r.Read(b)
		if err != nil {
			return errors.New("not enough bytes for int32")
		}
		l := binary.BigEndian.Uint32(b)

		// Create the map.
		m := make(map[interface{}]interface{}, l)

		// Get each item from the map.
		for i := uint32(0); i < l; i++ {
			// Get the key.
			var Key interface{}
			err := processItem(&pointerSetter{ptr: reflect.ValueOf(&Key)}, r)
			if err != nil {
				return err
			}
			switch x := Key.(type) {
			case []byte:
				// bytes should be stored as strings for maps
				Key = string(x)
			}

			// Get the value.
			var Value interface{}
			err = processItem(&pointerSetter{ptr: reflect.ValueOf(&Value)}, r)
			if err != nil {
				return err
			}

			// Set the key to the value specified.
			m[Key] = Value
		}

		// Set the item to the map.
		Item = m
	default: // Don't know this data type.
		return errors.New("unknown data type")
	}

	// Handle the item casting.
	return handleItemCasting(Item, setter)
}

// Unpack is used to unpack a value to a pointer.
// Note that to ensure compatibility in codebases where you have both erlpack and json, json.RawMessage is treated the same as erlpack.RawData.
func Unpack(Data []byte, Ptr interface{}) error {
	// Check if the ptr is actually a pointer.
	v := &pointerSetter{ptr: reflect.ValueOf(Ptr)}
	if v.ptr.Kind() != reflect.Ptr {
		return errors.New("invalid pointer")
	}

	// The invalid erlpack handler.
	err := func() error {
		return errors.New("invalid erlpack bytes")
	}

	// Create a bytes reader.
	r := bytes.NewReader(Data)

	// Check the length.
	l := len(Data)
	if 2 > l {
		return err()
	}

	// Check the version.
	Version, _ := r.ReadByte()
	if Version != 131 {
		return err()
	}

	// Return the data unpacking.
	return processItem(v, r)
}
