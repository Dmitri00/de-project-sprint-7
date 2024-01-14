import pyspark.sql.functions as F 
from pyspark.sql.window import Window
from spark_app import SparkApp


class GeoStats(SparkApp):
    EVENTS = 'message subscription reaction user'.split()
    def __init__(self):
        super().__init__(
            "GeoStats",
            "GeoStats"
        )
    def run(self, args):
        date = args[0]
        days_count = args[1]
        geo_events = args[3]
        user_recommendations = args[4]

        end_date = F.to_date(F.lit(date), "yyyy-MM-dd")
        events_users = self.geo_events_source(geo_events, end_date, days_count).persist()
        # Напомним, как будет работать рекомендация друзей: 
        # если пользователи подписаны на один канал - нужна таблица подписок
        # ранее никогда не переписывались - нужен маппинг чатов
        # и расстояние между ними не превышает 1 км - нужна таблица последней гео точки по каждому юзеру
        # то им обоим будет предложено добавить другого в друзья. 
        # Образовывается парный атрибут, который обязан быть уникальным: 
        # порядок упоминания не должен создавать дубли пар.

        # Алгоритм:
        # подготовить таблицу подписок для джойна: две копии таблицы с префиксами колонки юзера
        # сджойнить таблицу подписок с собой через ид чата (будут симметричные копии)
        # выкинуть из драфта пары, где user_id left > user_id_right
        # антиджойн подписок с переписками (будет всегда user_id_left < user_id_right)
        # подкинуть координаты и города юзеров слева (колонки координат префиксануть)
        # подкинуть джойном координаты и города юзеров справа (колонки координат префиксануть)
        # посчитать расстояния между юзерами
        # выкинуть юзеров, которые дальше чем на 1 км 
        # сохранить витрину с 
        # user_left, user_right, processed_dttm (end_date), zone_id (zone_id_left)
        

        subscriptions = self.prepare_subscriptions(events_users)

        current_friends = self.prepare_current_friends(events_users)

        user_actual_geo = self.prepare_user_actual_geo(events_users)

        channel_neighbours = self.get_channel_neighbours(subscriptions)

        channel_neighbours_recom = channel_neighbours \
            .join(
                current_friends,
                on='user_left user_right'.split(),
                how='left_anti'
            )
        
        dist_col = 'user_dist_km'
        result = self.get_recom_geo_distances(
                channel_neighbours_recom,
                user_actual_geo,
                dist_col
            ) \
            .filter(f'{dist_col} < 1') \
            .select(
                'user_left',
                'user_right',
                end_date.alias('processed_dttm'),
                F.col('zone_left').alias('zone_id')
            )

        result.write.parquet(f'{user_recommendations}/dt={date}')
    
    def get_recom_geo_distances(self, user_recoms, user_geo, dist_col):
        distance_recom = user_recoms \
            .join(
                self.get_join_side_get(user_geo, 'left'),
                on='user_left'
            ) \
            .join(
                self.get_join_side_get(user_geo, 'right'),
                on='user_right'
            ) \
            .withColumn(
                dist_col,
                self.coord_dist_col(
                    'city_lat_left', 'city_lon_left',
                    'city_lat_right', 'city_lon_right'
                )
            )
        return distance_recom
            
    
    def get_join_side_get(self, geo_users, side):
        return geo_users \
                    .select(
                        F.col('user_id').alias(f'user_{side}'),
                        F.col('zone_id').alias(f'zone_{side}'),
                        F.col('city_lat').alias(f'city_lat_{side}'),
                        F.col('city_lon').alias(f'city_lon_{side}')
                    )
    
    def get_channel_neighbours(self, subscriptions):
        channel_neighbours = subscriptions \
            .withColumnRenamed('user_id', 'user_left') \
            .join(
                subscriptions \
                    .withColumnRenamed('user_id', 'user_right'),
                on='event.subscription_channel'
            ) \
            .filter('user_left < user_right')
        return channel_neighbours
    

    def prepare_subscriptions(self, events):
        # Подготовка таблицы подписок юзеров на каналы
        subs = events.where("event.subscription_channel is not null")  \
            .select(F.col("event.user").alias("user_id"), "event.subscription_channel") \
            .distinct()
        return subs

    def prepare_current_friends(self, events):
        users_pair = F.array([
                        F.col("event.message_from"),
                        F.col("event.message_to")])
        messages = events \
            .where(
             "event.message_from is not null and event.message_to is not null"
            ) \
            .select(
                # пара юзеров упорядочена по возрастанию для дедупликации пар
                F.array_min(
                    users_pair
                ).alias('user_left'),
                F.array_max(
                    users_pair
                ).alias('user_right')
            ) \
            .distinct()
        return messages
    
    def _filter_user_last_events(self, geo_events):
        last_travel_wnd = Window().partitionBy('user_id').orderBy(F.desc('datetime'))
        return geo_events \
            .withColumn('rn', F.row_number().over(last_travel_wnd)) \
            .filter('rn = 1') \
            .drop('rn')

    def prepare_user_actual_geo(self, geo_events):
        user_last_travels = self._filter_user_last_travels(geo_events) \
            .select(
                'user_id',
                F.col('zone_id').alias('act_city'),
                'local_time',
                'city_lat',
                'city_lon'
            )
        return user_last_travels
     

    def geo_events_source(self, path, end_date, days_count):
        return self.spark.read.parquet(path) \
                .filter(F.date_sub(end_date, days_count) <= F.col("datetime"))
    
    def coord_dist_col(self, a_lat, a_lon, b_lat, b_lon):
        return F.acos(
            F.sin(a_lat) * F.sin(b_lat)
            + F.cos(a_lat)
            * F.cos(b_lat)
            * F.cos(a_lon - b_lon)
                ) \
                * F.lit(6371.0*2)

if __name__ == '__main__':
    GeoStats().main()
