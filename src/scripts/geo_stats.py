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
        geo_stats_path = args[4]

        end_date = F.to_date(F.lit(date), "yyyy-MM-dd")
        events_users = self.geo_events_source(geo_events, end_date, days_count).persist()
        
        week_stats = self.pivot_agg_stats(
            events_users \
                .groupBy('week', 'month', 'zone_id', 'event_type'),
            'week'
            )
        
        month_stats = self.pivot_agg_stats(
            events_users \
                .groupBy('month', 'zone_id', 'event_type'),
            'month'
            )

        joint_stats = week_stats \
            .join(month_stats, on=['month', 'zone_id'])

        joint_stats.write.parquet(f'{geo_stats_path}/dt={date}')

    def pivot_agg_stats(self, grouped_events, prefix):
        # оличество сообщений;
        # количество реакций;
        # количество подписок;
        # количество событий user.
        agg_stats = grouped_events \
            .pivot('event_type', self.EVENTS) \
            .count('user_id') \
            .withColumnsRenamed({event:f'{prefix}_{event}' for event in self.EVENTS})
        return agg_stats

    def geo_events_source(self, path, end_date, days_count):
        return self.spark.read.parquet(path) \
                .filter(F.date_sub(end_date, days_count) <= F.col("datetime")) \
                .withColumn('week', F.weekofyear('week', 'date')) \
                .withColumn('month', F.date_trunc('month', 'date')) \
                .withColumnRenamed('event.event_type', 'event_type')

if __name__ == '__main__':
    GeoStats().main()
