from config.log_config import LOG_CONFIG
from os.path import expanduser
import configparser
import csv
import logging.config
import mysql.connector

logging.config.dictConfig(LOG_CONFIG)
logger = logging.getLogger("aurora_db")

if __name__ == "__main__":
    config = configparser.RawConfigParser()

    credentials_file = '/.rds/config'
    home = expanduser("~")
    credentials_file = home + credentials_file
    config.read(credentials_file)

    profile = "prd"

    config = {
        "host": config.get(profile, 'host'),
        "user": config.get(profile, 'user'),
        "password": config.get(profile, 'password'),
        "database": config.get(profile, 'database'),
    }

    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()

    query = """
                SELECT 'appointment' AS aurora_tbl_name, COUNT(*) counts FROM (SELECT DISTINCT appointment_identifier FROM appointment) a
                UNION SELECT 'article' AS aurora_tbl_name, COUNT(*) counts FROM (SELECT DISTINCT article_number FROM article) b
                UNION SELECT 'employee' AS aurora_tbl_name, COUNT(*) counts FROM (SELECT DISTINCT employee_identifier FROM employee) c
                UNION SELECT 'sales_order' AS aurora_tbl_name, COUNT(*) counts FROM (SELECT DISTINCT sales_order_identifier FROM sales_order) d
                UNION SELECT 'sales_order_receipt' AS aurora_tbl_name, COUNT(*) counts FROM (SELECT DISTINCT sales_order_identifier FROM sales_order_receipt) e
                UNION SELECT 'site' AS aurora_tbl_name, COUNT(*) counts FROM (SELECT DISTINCT site_number FROM site) f
                UNION SELECT 'vehicle' AS aurora_tbl_name, COUNT(*) counts FROM (SELECT DISTINCT vehicle_identifier, trim_identifier, assembly_identifier FROM vehicle) g
                UNION SELECT 'vehicleinspection' AS aurora_tbl_name, COUNT(*) counts FROM (SELECT DISTINCT inspection_identifier FROM vehicle_inspection) h
                UNION SELECT 'workorder' AS aurora_tbl_name, COUNT(*) counts FROM (SELECT DISTINCT work_order_identifier FROM work_order) i;
            """

    cursor.execute(query)

    with open('table_counts_aurora.csv', mode='w') as aurora_table_counta:
        writer = csv.writer(aurora_table_counta, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(("table_name", "counts"))

        for i in cursor:
            logger.info(i)
            print(i)
            writer.writerow(i)

    cursor.close()
    cnx.close()
