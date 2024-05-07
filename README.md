# desegmentation-server
Симуляция транспортного уровня: сбор сегментов в сообщение.
### Запуск:
docker-compose up -d
### Запуск после внесения изменений:
docker-compose up -d --build --remove-orphans
### Остановка:
docker stop $(docker ps -a -q --filter name=desegmentation-servergit-*)