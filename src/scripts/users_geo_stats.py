
import pyspark.sql.functions as F 
from pyspark.sql.window import Window
from spark_app import SparkApp



class UserGeoStats(SparkApp):
    def __init__(self):
        super().__init__(
            "UserGeoStats",
            "UserGeoStats"
        )
    def run(self, args):
        date = args[0]
        days_count = args[1]
        home_stand_days = args[2]
        geo_events = args[3]
        user_mart = args[4]

        end_date = F.to_date(F.lit(date), "yyyy-MM-dd")
        events_users = self.geo_events_source(geo_events, end_date, days_count)

        

        user_travels = self.extract_user_travels(events_users).persist()


        user_last_travels = self.search_user_last_travels(user_travels)

        user_home_cities = self.search_home_cities(user_travels, home_stand_days)

        user_agg_stats = self.user_travel_agg_stats(user_travels)

        # 1) для каждого юзера взять самый свежий город, последнее время события
        # 2) отфильтрить города дольше 27 дней, взять самый свежий
        # 3) сгруппировать города по юзеру и посчитать count, collect_list городов
        
        # сджойнить слева 1) и 2) и 3)
        
        result = user_last_travels \
            .join(user_agg_stats, on='user_id') \
            .join(user_home_cities, on='user_id', how='left')
        

        result.write.parquet(f'{user_mart}/dt={date}')

    def extract_user_travels(self, user_geo_events):
        # взять все сообщения
        # построить лаг города
        # оставить только смены городов, первую запись выкинуть
        # построить лид дат при смене города
        # заполнить последнюю дату макс датой в выборке
        # персист
        max_date = user_geo_events.select(F.max('datetime'))
        user_story_window = Window().partitionBy('user_id').orderBy(F.asc('datetime'))
        user_travels = user_geo_events \
            .withColumn('city_lag', F.lag('zone_id').over(user_story_window)) \
            .filter(
                (~F.isnull('city_lag')) &
                (F.col('city_lag') != F.col('zone_id'))
            ) \
            .withColumn('datetime_lead', F.lead('datetime').over(user_story_window)) \
            .withColumn('datetime_lead', self._fill_datetime('datetime_lead', max_date))
        
        return user_travels
    
    def _fill_datetime(self, dt_col, fill_with):
        return F.when(F.isnull(dt_col), fill_with).otherwise(dt_col)

    def _filter_user_last_travels(self, user_travels):
        last_travel_wnd = Window().partitionBy('user_id').orderBy(F.desc('datetime'))
        return user_travels \
            .withColumn('rn', F.row_number().over(last_travel_wnd)) \
            .filter('rn = 1') \
            .drop('rn')

    def search_user_last_travels(self, user_travels):
        user_last_travels = self._filter_user_last_travels(user_travels) \
            .select(
                'user_id',
                F.col('zone_id').alias('act_city'),
                'local_time'
            )
        return user_last_travels
    
    def search_home_cities(self, user_travels, home_stand_days):
        home_cities_draft = user_travels \
            .withColumn('stay_for_days', F.datediff('datetime_lead', 'datetime')) \
            .filter(f'stay_for_days > {home_stand_days}') \
            
        return self._filter_user_last_travels(home_cities_draft) \
            .select(
                'user_id',
                F.col('zone_id').alias('home_city')
            )
    
    def user_travel_agg_stats(self, user_travels):
        user_stats = user_travels \
            .withColumn('travel_struct',
                F.struct(
                     # sort array будет соритровать список структур по первой колонке
                     F.col('datetime'),
                     F.col('zone_id')
                )
            ) \
            .groupBy('user_id') \
            .agg(
                F.count('zone_id').alias('travel_count'),
                F.sort_array(F.collect_list('travel_struct')).alias('travel_array_dt')
            ) \
            .withColumn(
                'travel_array',
                F.col('travel_array_dt').zone_id
            ) \
            .drop('travel_array_dt')

        return user_stats


    def geo_events_source(self, path, end_date, days_count):
        return self.spark.read.parquet(path) \
                .filter(F.date_sub(end_date, days_count) <= F.col("date"))

if __name__ == '__main__':
    UserGeoStats().main()
