**pudgedb**

pudgedb предоставляет мемкеш интерфейс к движкам, имплементирующим мемкеш протокол


По сути - это сокет сервер, слущающий мемкеш протокол - и при получении команды - передающий управление движку.

Движок может имплементировать, любую из поддерживаемых комманд и отвечать на нее.


**Поддерживаемые команды**

```
	Get(key []byte, rw *bufio.ReadWriter) (value []byte, noreply bool, err error)
	Gets(keys [][]byte, rw *bufio.ReadWriter) error
	Set(key, value []byte, flags uint32, exp int32, size int, noreply bool, rw *bufio.ReadWriter) (noreplyresp bool, err error)
	Incr(key []byte, value uint64, rw *bufio.ReadWriter) (result uint64, isFound bool, noreply bool, err error)
	Decr(key []byte, value uint64, rw *bufio.ReadWriter) (result uint64, isFound bool, noreply bool, err error)
	Delete(key []byte, rw *bufio.ReadWriter) (isFound bool, noreply bool, err error)
	Close() error
	FileSize() (int64, error)
```

Например, я хочу имплементировать get в своем движке pudge, надо зарегистрировать движок в engine и имплементировать соответсвующий метод:

```
func (en *pudgeEngine) Get(key []byte, rw *bufio.ReadWriter) ([]byte, bool, error) {
	var b []byte
	err := en.Db.Get(key, &b)
	return b, false, err
}
```

Соотвественно, когда я запускаю pudgedb c движком pudge (-e pudge) - при получении get - выполнится мой метод. Таким образом - можно строить микросервисы выполняющие любую функциональность в едином интерфейсе.