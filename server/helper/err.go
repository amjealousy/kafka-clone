package helper

import "fmt"

type RestrictedOpError struct {
	Message string
	Wrapped error
}

func (e *RestrictedOpError) Error() string {
	return fmt.Sprintf("%s: %v", e.Message, e.Wrapped)
}
func (e *RestrictedOpError) Unwrap() error {
	return e.Wrapped
}

type FollowerModeError struct {
}

func (e *FollowerModeError) Error() string {
	return fmt.Sprintf("FollowerModeError")
}
