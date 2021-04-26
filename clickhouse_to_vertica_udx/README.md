**Данные из Clickhouse в Vertica**

В этой папке содержатся примеры кода для статьи  ...

В статье описано решение, позволяющее строить витрины данных в Vertica, используя данные ClickHouse. 
Представленный код дает возможность запросить данные из ClickHouse с помощью команды COPY:
"COPY vertica.destination_table SOURCE ClickHouse(query='SELECT * FROM clickhouse.source_table')"
