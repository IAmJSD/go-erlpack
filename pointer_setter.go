package erlpack

import (
	"errors"
	"reflect"
)

type pointerSetter struct {
	ptr reflect.Value
}

func (s *pointerSetter) getBasePtr() interface{} {
	x := s.ptr.Type()
	for x.Kind() == reflect.Ptr {
		x = x.Elem()
		if x.Kind() != reflect.Ptr {
			return reflect.NewAt(x,nil).Interface()
		}
	}
	panic("not a pointer - this is a go-erlpack bug, this should be caught in the public functions!")
}

func (s *pointerSetter) set(ptr reflect.Value) error {
	// Get the original pointer for iteration.
	x := s.ptr

	// Get the reflect value before.
	var valueBefore *reflect.Value

	// Loop until the pointer doesn't point to a pointer.
	for x.Elem().Kind() == reflect.Ptr {
		// Create a new element of the pointer type.
		n := reflect.New(x.Elem().Type().Elem())

		// Get the result of the parent to this child.
		x.Elem().Set(n)

		// Set the parent.
		xCpy := x
		valueBefore = &xCpy

		// Set x to the child.
		x = n
	}

	// Is the pointer nil?
	if ptr.IsNil() {
		// Check if the value before isn't nil.
		if valueBefore == nil {
			return errors.New("cannot nil a value pointer, this should be a pointer to a pointer")
		}

		// Set the pointer.
		valueBefore.Elem().Set(reflect.Zero(reflect.PtrTo(valueBefore.Elem().Elem().Type())))

		// Return no errors.
		return nil
	}

	// Set the element.
	x.Elem().Set(ptr.Elem())
	return nil
}
