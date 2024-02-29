# coding:utf8
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
import pandas as pd
from pyspark.sql.window import Window
from pyspark.sql.functions import sum as spark_sum, col

if __name__ == '__main__':
    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        getOrCreate()
    sc = spark.sparkContext

    # 测试数据1
    pdf1 = pd.DataFrame(
        {
            "peer_id": ['ABC17969(AB)','ABC17969(AB)','ABC17969(AB)','ABC17969(AB)',\
                        'ABC17969(AB)','ABC17969(AB)','AE686(AE)','AE686(AE)','AE686(AE)',\
                        'AE686(AE)','AE686(AE)','AE686(AE)'],
            "id_1": [ '1','2','3','4','5','6','7','8','9','10','11','12'],
            "id_2": [ 'ABC17969','CDC52533','DEC59161','F43874','MY06154','MY4387','AE686',\
                      'BH2740','EG999','AE0908','QA402','OM691'],
            "year": [2022,2022,2023,2022,2021,2022,2023,2021,2021,2021,2022,2022]
        }
    )

    # 测试数据2
    pdf2 = pd.DataFrame(
        {
            "peer_id": ['AE686(AE)','AE686(AE)','AE686(AE)','AE686(AE)','AE686(AE)','AE686(AE)',\
                        'AE686(AE)','AE686(AE)','AE686(AE)'],
            "id_1": ['7', '8', '9', '10', '11', '12', '12', '12', '12'],
            "id_2": ['AE686','BH2740','EG999','AE0908','QA402','OA691','OB691','OC691','OD691'],
            "year": [2022,2021,2021,2023,2022,2022,2022,2019,2017]
        }
    )


    # ！！注意，目前输入的数据1，可以查看测试结果1，若要看数据2的测试结果，请讲pf2参数传入下方,查看测试结果2和3
    df = spark.createDataFrame(pdf2)
    df1 = df.filter(df["peer_id"].contains(df["id_2"]))
    print("步骤1输出：")
    df1.show()
    df2 = df.groupBy(["peer_id","year"]).count().withColumnRenamed("count", "peer_cnt")
    df1.createTempView("contain_table")
    df2.createTempView("count_table")
    df3 = spark.sql("SELECT b.peer_id, b.year, b.peer_cnt  FROM contain_table a INNER JOIN count_table b"\
              " ON a.peer_id=b.peer_id AND a.year >= b.year ORDER BY b.peer_id,b.year DESC")
    df3.createTempView("result")
    df4 = spark.sql("SELECT peer_id,year,peer_cnt,SUM(peer_cnt) OVER(PARTITION BY peer_id ORDER BY year DESC) "\
            " AS cnt FROM result")
    print("步骤2输出：")
    df4.show()
    df4.createTempView("last_result")

    print("步骤3输出：")

    print("测试结果1输出：")
    spark.sql(""" select * from (
        (select peer_id,year from (SELECT peer_id,year,cnt,ROW_NUMBER() OVER(PARTITION BY peer_id ORDER BY
        year DESC) AS newline  from last_result ) t where t.newline=1 and t.cnt > 3) union all
        (select peer_id,year from (SELECT peer_id,year,cnt,LAG(cnt)  OVER(PARTITION BY peer_id ORDER BY
            year DESC) AS newcnt  from last_result ) t where t.cnt > 3 and t.newcnt < 3) union all
       (select  peer_id,year from last_result where cnt <=3 group by peer_id,year)) M
        """).show()

    print("测试结果2输出：")
    spark.sql(""" select * from (
        (select peer_id,year from (SELECT peer_id,year,cnt,ROW_NUMBER() OVER(PARTITION BY peer_id ORDER BY
        year DESC) AS newline  from last_result ) t where t.newline=1 and t.cnt > 5) union all
        (select peer_id,year from (SELECT peer_id,year,cnt,LAG(cnt)  OVER(PARTITION BY peer_id ORDER BY
            year DESC) AS newcnt  from last_result ) t where t.cnt > 5 and t.newcnt < 5) union all
       (select  peer_id,year from last_result where cnt <=5 group by peer_id,year)) M
    """).show()

    print("测试结果3输出：")
    spark.sql(""" select * from (
        (select peer_id,year from (SELECT peer_id,year,cnt,ROW_NUMBER() OVER(PARTITION BY peer_id ORDER BY
        year DESC) AS newline  from last_result ) t where t.newline=1 and t.cnt > 7) union all
        (select peer_id,year from (SELECT peer_id,year,cnt,LAG(cnt)  OVER(PARTITION BY peer_id ORDER BY
            year DESC) AS newcnt  from last_result ) t where t.cnt > 7 and t.newcnt < 7) union all
       (select  peer_id,year from last_result where cnt <=7 group by peer_id,year)) M
        """).show()

    # spark.sql("""select peer_id,year from (SELECT peer_id,year,cnt,LAG(cnt)  OVER(PARTITION BY peer_id ORDER BY
    #         year DESC) AS newcnt  from last_result ) t where t.cnt > 5 and t.newcnt < 5
    #         """).show()









