# LoadBalanserGo

Запустить балансировщик:
```shell
go run main.go
```
Запустить тестовые сервера:
```shell
cd servers
```
```shell
go run servers.go 8081
```
```shell
go run servers.go 8082
```
```shell
go run servers.go 8083
```
Послать запросы на http://localhost:8080/
Для проверки Graceful Shutdown 
Послать запросы и завершить работу балансировщика(Ctrl+C)
Балансировщик должен завершить текущие запросы