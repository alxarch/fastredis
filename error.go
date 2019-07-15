package redis

// Err is a constant value error
type Err string

func (e Err) Error() string {
	return string(e)
}
func (e Err) String() string {
	return string(e)
}

// TimeoutError is an error indicating blocking command timeout
type TimeoutError struct{}

func (*TimeoutError) Error() string {
	return `Timeout error`
}

// IsTimeout implements IsTimeouter interface
func (*TimeoutError) IsTimeout() bool {
	return true
}
