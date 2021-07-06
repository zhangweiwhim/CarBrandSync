# coding=utf-8
import logging
import pandas as pd
from pandas import DataFrame
from sqlalchemy import create_engine
import codecs
import datetime
import schedule

# pd.set_option('display.max_rows', 1000)


logging.basicConfig(level=logging.DEBUG,
                    filename='sync-' + str(datetime.datetime.now().date()) + '.log',
                    filemode='a',
                    format=
                    '%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s'
                    )


def job(dif):
    logging.debug(dif + "开始执行同步")
    try:
        path = 'car_base.txt'
        # path = '/usr/share/elasticsearch/plugins/analysis-hanlp/data/dictionary/custom/ShanghaiPlaceName.txt'
        # 获取字典文件中的品牌
        file_already = codecs.open(path, 'r', 'utf-8')
        lines = file_already.readlines()
        df_lines = DataFrame(lines, columns=['name']).replace("\n", '', regex=True)
        file_already.close()
        # print df_lines
        # 获取数据库中的品牌
        # engine_pro = create_engine('mysql+pymysql://car:34refwasdf@10.88.2.30:3306/car?charset=utf8', encoding='utf-8')
        # engine_uat = create_engine('mysql+pymysql://car:qxCMwdQhav@119.3.77.221:3306/car?charset=utf8',encoding='utf-8')
        engine_uat = None
        sql_query = 'select  distinct name  from car_brand;'

        df_sql = pd.read_sql_query(sql_query, engine_uat)
        # 求差集
        df_all = pd.concat([df_sql, df_lines, df_lines]).drop_duplicates(keep=False)
        # print df_all
        # append到词典文件文件中
        df_all.to_csv(path, header=False, index=False, encoding='utf-8', mode='a')
    except  Exception as e:
        logging.error(e.args)
    else:
        logging.debug("程序处理完成")


if __name__ == '__main__':
    job('初始化')
    s = '定时' + str(datetime.datetime.now())
    while True:
        schedule.run_pending()
