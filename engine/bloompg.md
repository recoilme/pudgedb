**BloomPg**

Сервис проверки наличие хеша в блум фильтре.

**Сборка**

```
go get github.com/recoilme/pudgedb
cd $GOPATH/github.com/recoilme/pudgedb
go get -d ./...
go build
```

**Запуск**

 ./pudgedb -params connStr=postgres://user:pass@ip/database?sslmode=disable --e bloompg --debug true -p 11213


 Прочие параметры: ./pudgedb --help (порт интерфейс и тп)

 **Как работает**

 bloompg - это движок, имплементирующий метод get и close memcache протокола

 Проверка наличия хеша по id - с помощью мемкеш клиента или по телнет:

```
telnet 127.0.0.1 11213
get check filters:9 checks:Wikipedia
VALUE check 0 12
Wikipedia:9
END
get check filters:9 checks:u
VALUE check 0 3
u:
END
close
Connection closed by foreign host.
```

**Алгоритм**

filters:9,10,11 - для данных id - сервис идет в потгрес и скачивает битмап.
Формат: https://metacpan.org/source/SMUELLER/Algorithm-BloomFilter-0.02


Первый байт - k хешфункций
Второй байт - степень двойки битмапа (размер)
Далее битмап


При последующих запросах - в бд не ходит


Проверка - берется 2 сипхеша от значения с к1,k2 = 0,0 и к1,k2 = 0,1
и с шифтом (64 - степень войки) в цикле крутится хешфункция, вычисляется инт и по нему делается BITSET


Ответ - в фотмате xxxxyyyy: uxrR45A8:11,23

Если что то пошло не так - ответ пустой (END)
Ошибки пишутся в лог
