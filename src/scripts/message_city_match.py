
import pyspark.sql.functions as F 
from pyspark.sql.window import Window
from spark_app import SparkApp

class MessageCityMatchApp(SparkApp):
    def __init__(self):
        super().__init__(
            "MessageCityMatchApp",
            "MessageCityMatchApp"
        )
    def run(self, args):
        date = args[0]
        events_raw = args[1]
        city_geo = args[2]
        messages_matched = args[3]

        end_date = F.to_date(F.lit(date), "yyyy-MM-dd")
        events_users = self.events_source(events_raw, end_date)

        city_geo = self.city_geo_source(city_geo)
        dist_col = 'distance'
        message_with_city = events_users \
            .crossJoin(city_geo) \
            .withColumn(dist_col,
                        self.coord_dist_col(
                            'message_lat',
                            'message_lon',
                            'city_lat',
                            'city_lon')) \
            .withColumn('city_rank', self.city_rank('message_id', dist_col)) \
            .filter('city_rank = 1')
        
        message_with_city = message_with_city \
            .withColumn('local_time', F.from_utc_timestamp(F.col("datetime"),F.col('city_tz')))

        message_with_city.write.parquet(f'{messages_matched}/dt={date}')

    def coord_dist_col(self, a_lat, a_lon, b_lat, b_lon):
        return F.acos(
            F.sin(a_lat) * F.sin(b_lat)
            + F.cos(a_lat)
            * F.cos(b_lat)
            * F.cos(a_lon - b_lon)
                ) \
                * F.lit(6371.0*2)
    def city_rank(self, partition_by_col, dist_col):
        message_city_window = Window().partitionBy([partition_by_col]).orderBy(dist_col)
        return F.row_number().over(message_city_window)



    def city_geo_source(self, path):
        city_geo = self.spark.read.option("delimiter", ",") \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv(path) \
                .select(
                    F.col('id').alias('zone_id'),
                    'city',
                    'city_lat',
                    'city_lon',
                    F.col('tz').alias('city_tz')
                )
        return city_geo

    def events_source(self, path, end_date):
        return self.spark.read.parquet(path) \
                .filter(F.col("datetime")== end_date) \
                .where("event.message_from is not null and event_type = 'message'")


if __name__ == '__main__':
    MessageCityMatchApp().main()
