# clickhouse2r

Проект для подключения к ClickHouse из RStudio.

Основан на статье Александра Кучина [Выгрузка из Clickhouse не выходя из Rstudio](https://wiki.yandex-team.ru/users/alexkucin/Vygruzka-iz-Clickhouse-ne-vyxodja-iz-Rstudio/)

## Использование
  * Создаём auth.RData с логином и паролем для подключения к ssh;
  * Подключаем проект с помощью `source("/path/to/clickhouse2r.R")`;
  * Используем функцию read.clickhouse для выгрузки данных в виде Data Frame.