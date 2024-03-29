# Проект 7-го спринта

## Описание
Репозиторий предназначен для сдачи проекта 7-го спринта

## Как работать с репозиторием
1. В вашем GitHub-аккаунте автоматически создастся репозиторий `de-project-sprint-7` после того, как вы привяжете свой GitHub-аккаунт на Платформе.
2. Скопируйте репозиторий на свой компьютер. В качестве пароля укажите ваш `Access Token`, который нужно получить на странице [Personal Access Tokens](https://github.com/settings/tokens)):
	* `git clone https://github.com/{{ username }}/de-project-sprint-7.git`
3. Перейдите в директорию с проектом: 
	* `cd de-project-sprint-7`
4. Выполните проект и сохраните получившийся код в локальном репозитории:
	* `git add .`
	* `git commit -m 'my best commit'`
5. Обновите репозиторий в вашем GutHub-аккаунте:
	* `git push origin main`

## Структура хранилища
Источники данных:
/user/master/data/geo/events
/user/dmitri1661/geo/sources/geo_cities/geo.csv
Детальный слой данных:
/user/dmitri1661/geo/storages/user_geo_profiles
Тестовые данные:
/user/dmitri1661/geo/test_samples
Данные для аналитиков:
/user/dmitri1661/geo/datamarts/geo_stats
/user/dmitri1661/geo/datamarts/friendship_recommendation
как часто обновляются данные - раз в день, задержка относительно обновления источника - 2 часа
Формат данных - json/parquet

## Структура репозитория
Вложенные файлы в репозиторий будут использоваться для проверки и предоставления обратной связи по проекту. Поэтому постарайтесь публиковать ваше решение согласно установленной структуре — так будет проще соотнести задания с решениями.

Внутри `src` расположены две папки:
- `/src/dags`;
- `/src/sql`.

## Варки данных:
### 1. MessageCityMatch - день ко дню от сырых данных
#### Параметры
	- дата
	- путь к сырым данным
	- путь к гео данным
	- путь к результату
### 2. User DataMart - каждый день - окно в прошлое от MessageCityMatch по кол-ву дней
#### Параметры
	- дата
	- путь к MessageCityMatch
	- путь к результату
### 3. City DataMart - каждый день - окно в прошлое от MessageCityMatch по кол-ву дней
#### Параметры
	- дата
	- путь к MessageCityMatch
	- путь к результату
### 4. User Recommendations - LastSuccess от User DataMart
#### Параметры
	- дата
	- путь к MessageCityMatch
	- путь к результату
