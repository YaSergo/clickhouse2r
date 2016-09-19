# Демонстрация работы с clickhouse в RStudio

source("./clickhouse2r.R")

#список хостов Clickhouse
hosts<- c("mtstat01-1.yandex.ru",
          "mtmega.yandex.ru",
          "mtmega01e.yandex.ru",
          "mtmega01d.yandex.ru",
          "mtmega01i.yandex.ru")

#сам  запрос, пишите стандартно, как привыкли, только кавычки должны быть простые ' а не двойные "
query<- c("select UUID, OriginalDeviceID, ADVID, max((SessionType = 0) ? toDate(StartDate) : toDate('0000-00-00')) as MaxDate
          from mobile.events_all
          where
          APIKey IN (23104, 23107) AND
          StartDate >= ('2016-07-19') AND
          StartDate <= ('2016-07-19')
          group by UUID, OriginalDeviceID, ADVID
          limit 1000")


#используем функцию для выгрузки из Clickhouse
df<- read.clickhouse(query=query, host=hosts[1], verbose=T)
