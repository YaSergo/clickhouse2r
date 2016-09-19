# Основано на статье Александра Кучина:
# https://wiki.yandex-team.ru/users/alexkucin/Vygruzka-iz-Clickhouse-ne-vyxodja-iz-Rstudio/

#список хостов Clickhouse
hosts<- c("mtstat01-1.yandex.ru",
          "mtmega.yandex.ru",
          "mtmega01e.yandex.ru",
          "mtmega01d.yandex.ru",
          "mtmega01i.yandex.ru")

auth.file.path <- "./auth.Rdata"
if (!file.exists(auth.file.path)){
  stop("Создайте ./auth.Rdata, с двумя переменными secret.user и secret.password
       с логином и паролем для подключения к ssh")
}
# в данном файле должны быть сохранены две переменные:
#   * secret.user     - str, логин для подключения к ssh
#   * secret.password - str, пароль для подключения к ssh
load(file = auth.file.path)

log.print <- function(start.time, message){
  diff.time <- as.numeric(Sys.time() - start.time, units = "secs")
  diff.time <- paste0("[\t", round(diff.time, 2), " сек] ")
  writeLines(paste0(diff.time, message))
}

#сам  запрос, пишите стандартно, как привыкли, только кавычки должны быть простые ' а не двойные "
query<- c("select UUID, OriginalDeviceID, ADVID, max((SessionType = 0) ? toDate(StartDate) : toDate('0000-00-00')) as MaxDate
          from mobile.events_all
          where
          APIKey IN (23104, 23107) AND
          StartDate >= ('2016-07-19') AND
          StartDate <= ('2016-07-19')
          group by UUID, OriginalDeviceID, ADVID
          limit 100")

#функция для обработки запроса на удаленной машинке, возвращает результат запроса в виде data frame
read_CH<- function(query, host, local_dir="~/tmp", remote_dir="tmp", header=T, fill=T, strFact=F, strip.white=T, verbose=F, check_time=T)
{
  start.time <- Sys.time()
  
  
  if (!dir.exists(local_dir)){
    dir.create(local_dir)
  }
  current_dir <- getwd()
  setwd(local_dir)
  
  #escape кавычек
  query<- gsub(x=query, pattern="'", replacement="\'\"\'\"\'")
  
  #выполняем запрос в Clickhouse, сохранение в указанной папке на удаленной машинке. Если ошибка получим сообщение
  message<- system(paste("ssh -t -t analytics1e.stat.yandex.net  'cd ",remote_dir,
                         " && clickhouse-client --host=",host,
                         " --user=", secret.user,
                         " --password=", secret.password,
                         " --format=TabSeparatedWithNames --query=\"",query,
                         "\" > Rout && exit'", sep=""), intern=T)
  
  #если ошибка на стороне Clickhouse
  if(length(message)!=0){ 
    
    #удаление временного файла
    system(paste("ssh -t -t analytics1e.stat.yandex.net  'cd ",remote_dir," && rm Rout && exit'", sep=""))
    if(verbose==T) {log.print(start.time, "99%: Временный файл удален из удаленной папки")}
    if(verbose==T) {log.print(start.time, "100%: Ошибка!")}

    #выводим ошибку и ничего не возвращаем
    log.print(start.time, message)
    return()
  }
  
  if(verbose==T) {log.print(start.time, "25%: Запрос выпонен в Clickhouse")}

  #перенос файла на локальную машинку в указанную папку
  system(paste("scp analytics1e.stat.yandex.net:",remote_dir,"/Rout ./Rout.txt", sep=""))
  if(verbose==T) {log.print(start.time, "50%: Файл с данными перенесен в локальную папку")}
  
  #чтение файла с указанными параметрами
  df<- read.table(file="Rout.txt", sep="\t", header=header, fill=fill, stringsAsFactors=strFact, strip.white=strip.white)
  if(verbose==T) {log.print(start.time, "90%: Данные загружены в память")}

  #удаление временных файлов в локальной и удаленной папках
  system("rm Rout.txt")
  if(verbose==T) {log.print(start.time, "95%: Временный файл удален из локальной папки")}
  
  system(paste("ssh -t -t analytics1e.stat.yandex.net  'cd ",remote_dir," && rm Rout && exit'", sep=""))
  if(verbose==T) {log.print(start.time, "99%: Временный файл удален из удаленной папки")}
  if(verbose==T) {log.print(start.time, "100%: Успешно!")}

  setwd(current_dir)
  #возвращаем data frame
  return(df)
}

#используем функцию для выгрузки из Clickhouse
df<- read_CH(query=query, host=hosts[1], verbose=T)