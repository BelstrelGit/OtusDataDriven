
OtusBigProject
Выпускной проект по курсу Data Engineer

Данное приложение реализует Data-driven подход для аналитики и мониторинга показателей с фондовых рынков. 
 Данные на самом низком уровне будут поступать в Apache Kafka  из API( Mock  Files) ,
откуда сырые данные в необработанном виде через Kafka Connenct будут доставляться в уровень хранилища Stage(SQL / NoSQL / S3 / parquet / delta и тд) Опционально можно использовать инструменты для переноса данных(Apache Flume / Sqoop /Cast Tools ) . Также из Kafka данные будут забираться и обогащаться Spark Streaming , записываться в Kafka в другой топик и через Kafka Connect сохраняться в слой хранения данных. Из втрого топика обогащенные данные будут поступать в Эластик и следом в Kibana для визуализации. Из слоя хранения данные в целях построения аналитических отчетов будут периодически забираться(Apache Airflow, cron) батч обработкой Spark Batch. Также в целях обучения ML моделей данные могут бытьь предоставлены из слоя хранения , либо уже подготовленные батч обработкой и сохранённые в специальных файлах/таблицах. После обработки данные будут визуализироваться на Jupyter Notebooks . Данная схема не финальная, дополняется и изменяется в процессе разработки. Визуальная схема находиться в pdf файле.

## How-to run


```
- Run analytics
```bash
make run-appliance
```


You could also access the SparkUI for this Job at http://localhost:4040/jobs



## Known issues

- Sometimes you need to increase docker memory limit for your machine (for Mac it's 2.0GB by default).
- To debug memory usage and status of the containers, please use this command:
```bash
docker stats
```
- Sometimes docker couldn't gracefully stop the consuming applications, please use this command in case if container hangs:
```bash
docker-compose -f <name of compose file with the job>.yaml down
```
