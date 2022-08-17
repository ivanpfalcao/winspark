import os
import requests
import pathlib
import pyspark

from zipfile import ZipFile

class WinSpark():
    def set_pyspark_env_vars():
        pyspark_path = str(pyspark.__path__[0])
        print(f'pyspark path: {pyspark_path}')
        os.environ['SPARK_HOME']=pyspark_path
        os.environ['HADOOP_HOME']=pyspark_path

    def set_JDK4py():
        from jdk4py import JAVA_HOME
        os.environ['JAVA_HOME']=str(JAVA_HOME)

    def load_spark_winutils():
        pyspark_path = str(pyspark.__path__[0])
        win_utils_file = pyspark_path + '\\bin\\winutils.exe'
        hadoop_dll_file = pyspark_path + '\\bin\\hadoop.dll'

        print(f'Winutils path: {win_utils_file}')
        if (not os.path.isfile(win_utils_file)):
            print('Baixando winutils')
            win_utils_url = 'https://github.com/steveloughran/winutils/raw/master/hadoop-3.0.0/bin/winutils.exe'
            response = requests.get(win_utils_url)
            open(win_utils_file, "wb").write(response.content)

        print(f'Hadoop.dll path: {hadoop_dll_file}')
        if (not os.path.isfile(hadoop_dll_file)):
            print('Baixando winutils')
            win_utils_url = 'https://github.com/steveloughran/winutils/raw/master/hadoop-3.0.0/bin/hadoop.dll'
            response = requests.get(win_utils_url)
            open(hadoop_dll_file, "wb").write(response.content)            

    def load_spark_jdbc():
        pyspark_path = str(pyspark.__path__[0])
        jdbc_zip_file = os.path.join(pyspark_path, 'jars\\mysql-connector-java-8.0.28.zip')
        jdbc_zip_unziped = os.path.join(pyspark_path,'jars\\mysql-connector-java-8.0.28.jar')
        jdbc_path = os.path.join(pyspark_path, 'jars')
        if (not os.path.isfile(jdbc_zip_unziped)):
            print('Baixando jdbc')
            mysql_jdbc_url = 'https://downloads.mysql.com/archives/get/p/3/file/mysql-connector-java-8.0.28.zip'
            response = requests.get(mysql_jdbc_url)
            open(jdbc_zip_file, "wb").write(response.content)
            with ZipFile(jdbc_zip_file, 'r') as zipObject:
                list_of_files = zipObject.namelist()
                jdbc_ziped_file = list(filter(lambda fname: ('mysql-connector-java-8.0.28.jar' in fname), list_of_files))[0]
                zipObject.extract(jdbc_ziped_file, jdbc_path)
                jdbc_ziped_path = os.path.join(jdbc_path, jdbc_ziped_file)
                jdbc_jar =  os.path.join(jdbc_path, 'mysql-connector-java-8.0.28.jar')
                os.replace(jdbc_ziped_path, jdbc_jar)


