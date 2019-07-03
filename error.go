package redis

type Err string

func (e Err) Error() string {
	return string(e)
}
func (e Err) String() string {
	return string(e)
}
