package redis

type Err string

func (e Err) Error() string {
	return string(e)
}
func (e Err) String() string {
	return string(e)
}

type TimeoutError struct{}

func (*TimeoutError) Error() string {
	return `Timeout error`
}
func (*TimeoutError) IsTimeout() bool {
	return true
}
