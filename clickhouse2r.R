# Основано на статье Александра Кучина:
# https://wiki.yandex-team.ru/users/alexkucin/Vygruzka-iz-Clickhouse-ne-vyxodja-iz-Rstudio/

#список хостов Clickhouse
hosts<- c("mtstat01-1.yandex.ru",
          "mtmega.yandex.ru",
          "mtmega01e.yandex.ru",
          "mtmega01d.yandex.ru",
          "mtmega01i.yandex.ru")

# в данном файле должны быть сохранены две переменные:
#   * secret.user     - логин для подключения к ssh
#   * secret.password - пароль для подключения к ssh
load(file = "./auth.Rdata")

#сам  запрос, пишите стандартно, как привыкли, только кавычки должны быть простые ' а не двойные "
query<- c("select UUID, OriginalDeviceID, ADVID, max((SessionType = 0) ? toDate(StartDate) : toDate('0000-00-00')) as MaxDate
          from mobile.events_all
          where
          APIKey IN (23104, 23107) AND
          StartDate >= ('2016-07-19') AND
          StartDate <= ('2016-07-19')
          group by UUID, OriginalDeviceID, ADVID
          ")

#функция для обработки запроса на удаленной машинке, возвращает результат запроса в виде data frame
read_CH<- function(query, host, local_dir="~/tmp", remote_dir="tmp", header=T, fill=T, strFact=F, strip.white=T, verbose=F, check_time=T)
{
  t1<- Sys.time()
  
  
  if (!dir.exists(local_dir)){
    dir.create(local_dir)
  }
  setwd(local_dir)
  
  #escape кавычек
  query<- gsub(x=query,pattern="'", replacement="\'\"\'\"\'")
  
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
    if(verbose==T) {writeLines("99%: Временный файл удален из удаленной папки")}
    
    t2<- Sys.time()
    if(verbose==T) {writeLines("100%: Ошибка!")}
    if(check_time==T | verbose==T) {writeLines(paste("Общее время выполнения: ",round(t2-t1,2), sep=""))}
    
    #выводим ошибку и ничего не возвращаем
    writeLines(message)
    return()
  }
  
  if(verbose==T) {writeLines("25%: Запрос выпонен в Clickhouse")}
  t2<- Sys.time()
  if(verbose==T) {writeLines(paste("Время выполнения: ",round(t2-t1,2), sep=""))}
  
  #перенос файла на локальную машинку в указанную папку
  system(paste("scp analytics1e.stat.yandex.net:",remote_dir,"/Rout ./Rout.txt", sep=""))
  if(verbose==T) {writeLines("50%: Файл с данными перенесен в локальную папку")}
  t3<- Sys.time()
  if(verbose==T) {writeLines(paste("Время выполнения: ",round(t3-t2,2), sep=""))}
  
  #чтение файла с указанными параметрами
  df<- read.table(file="Rout.txt", sep="\t", header=header, fill=fill, stringsAsFactors=strFact, strip.white=strip.white)
  if(verbose==T) {writeLines("90%: Данные загружены в память")}
  t2<- Sys.time()
  if(verbose==T) {writeLines(paste("Время выполнения: ",round(t2-t3,2), sep=""))}
  
  #удаление временных файлов в локальной и удаленной папках
  system("rm Rout.txt")
  if(verbose==T) {writeLines("95%: Временный файл удален из локальной папки")}
  
  system(paste("ssh -t -t analytics1e.stat.yandex.net  'cd ",remote_dir," && rm Rout && exit'", sep=""))
  if(verbose==T) {writeLines("99%: Временный файл удален из удаленной папки")}
  
  t2<- Sys.time()
  if(verbose==T) {writeLines("100%: Успешно!")}
  if(check_time==T | verbose==T) {writeLines(paste("Общее время выполнения: ",round(t2-t1,2), sep=""))}
  
  #возвращаем data frame
  return(df)
}

#используем функцию для выгрузки из Clickhouse
df<- read_CH(query=query, host=hosts[1], verbose=T)