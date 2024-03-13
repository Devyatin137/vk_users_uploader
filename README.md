# vk_users_uploader

Мини-приложение, которое читает идентификаторы пользователей 
социальной сети "ВКонтакте" из базы данных (БД). Получает информацию о пользователях по этим 
идентификаторам. Пишет в БД полученную информацию. Читает из БД, сохраняет информацию в файл xlsx.

Как запустить:
1. Создать или использовать любую существующую базу Postgres.
2. Выполнить в БД скрипт по созданию таблицы, файл: src/main/resources/Postgres_create_user_info.sql
3. Выполнить в БД скрипт записи идентификаторов ВК в БД, файл: src/main/resources/Postgres_insert_vk_ids.sql
4. Задать настройки в файле target/config.properties <br>
   DB_CONNECTION_STRING - строка подключения к БД Postgres.<br>
   DB_USER - имя пользователя БД <br>
   DB_PASSWORD - пароль <br>
   VK_ACCESS_TOKEN - access_token из "ВКонтакте" <br>
   THREAD_COUNT - количество потоков, которые будут обращаться к API ВК <br>
   REQ_USERS_COUNT - количество одновременно запрашиваемых пользователей ВК <br>
   READ_DELAY_TIME - задержка в мсек между запросами к API ВК, при слишком активном чтении ВК ругается "Too many requests per second" <br>
   XLSX_FILE_NAME - имя конечного выгружаемого файла Excel <br>
5. Запустить исполняемый файл можно из командной строки, каталог target <br>
   java -jar -Dfile.encoding=UTF-8 vk_users_uploader.jar <br>
   или файл vk_users_uploader.bat <br>
6. Результатом работы будет файл xlsx.
7. Пример выгруженного файла vk_users_20240312.xlsx

   
