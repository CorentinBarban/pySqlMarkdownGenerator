# -*- coding: utf-8 -*-
import pymysql
import os

class DataDict(object):
    def __init__(self, connect_info):
        # Database connection configuration
        self.host_name = connect_info[0]
        self.user_name = connect_info[1]
        self.pwd = connect_info[2]
        self.db_name = connect_info[3]
        self.port = int(connect_info[4])
        self.folder_name = 'mysql_dict'

    def run(self):
        "" "script execution entry" ""
        tables = self.get_tables()
        self.generate_markdown_tables(tables)

    def get_tables(self):
        try:
            # Create a connection
            conn = pymysql.connect(self.host_name, self.user_name, self.pwd, self.db_name, self.port)
            # Create a cursor object with cursor()
            cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
            sql = "SELECT TABLE_NAME AS name FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '%s' ORDER BY name ASC ;" % (
                self.db_name,)
            cursor.execute(sql)
            tables = cursor.fetchall()
            cursor.close()
            return tables
        except Exception as e:
            print(e)
            print('Database connection failed, please check the connection information!')
            exit(1)

    def generate_markdown_tables(self, tables):
        try:
            # Create a connection
            conn = pymysql.connect(self.host_name, self.user_name, self.pwd, self.db_name, self.port)
            # Create a cursor object with cursor()
            cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
            self.deal_file(self.folder_name + os.sep + 'Documentation base de données.md')
            self.generate_markdown_header()
            for table in tables:
                # Table annotation get
                print('Start generating data dictionary for table %s' % (table,))
                sql = "show table status WHERE Name = '%s'" % (table["name"],)
                cursor.execute(sql)
                result = cursor.fetchone()
                self.generate_markdown_table(result)
                self.generate_markdown_indice(result)
                # Close connection
                print('complete data dictionary of table %s' % (table["name"],))
            cursor.close()
            conn.close()
        except Exception as e:
            print(e)
            print('Database connection failed, please check the connection information!')
            exit(1)

    def generate_markdown_header(self):
        file_path = self.folder_name + os.sep + 'Documentation base de données.md'
        # Open file, ready to write
        dict_file = open(file_path, 'a', encoding='UTF-8')

        dict_file.write('# Documentation de la base de donnée de %s\n\n' % (self.db_name.upper(),))

    def generate_markdown_table(self, table):
        try:
            # Create a connection
            conn = pymysql.connect(self.host_name, self.user_name, self.pwd, self.db_name, self.port)
            # Create a cursor object with cursor()
            cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
            # Folder and file processing
            file_path = self.folder_name + os.sep + 'Documentation base de données.md'
            # Open file, ready to write
            dict_file = open(file_path, 'a', encoding='UTF-8')

            # Generate table header
            dict_file.write('## Table: `' + table["Name"] + '`\n\n')
            dict_file.write('### Description: \n\n')

            table_comment = table['Comment'].replace("\r", "")
            table_comment = table_comment.replace("\n", "").capitalize()
            dict_file.write(table_comment + '\n\n')
            dict_file.write(
                '| Colonne | Type de données | Attributs | Default | Description |\n| --- | --- | --- | --- | ---  |\n')

            # Table structure query
            field_str = "COLUMN_NAME,COLUMN_TYPE,COLUMN_DEFAULT,COLUMN_COMMENT,COLUMN_KEY"
            sql = "select %s from information_schema.COLUMNS where table_schema='%s' and table_name='%s'" % (
                field_str, self.db_name, table["Name"])
            cursor.execute(sql)
            fields = cursor.fetchall()
            for field in fields:
                column_name = field['COLUMN_NAME']
                column_type = field['COLUMN_TYPE']
                column_key = field['COLUMN_KEY']
                column_default = str(field['COLUMN_DEFAULT'])
                column_comment = (field['COLUMN_COMMENT']).replace("\r", "")
                column_comment = column_comment.replace("\n", "").capitalize()
                if column_key == "PRI":
                    column_key = 'PRIMARY'
                elif column_key == "UNI":
                    column_key = 'UNIQUE'
                else:
                    column_key = ''
                info = ' | `' + column_name + '` | ' + column_type + ' | ' + column_key + ' | `' + column_default + '` | ' + column_comment + ' | '
                dict_file.write(info)
                dict_file.write('\n')
            dict_file.close()
            cursor.close()
            conn.close()

        except Exception as e:
            print(e)
            print('Database connection failed, please check the connection information!')
            exit(1)

    def generate_markdown_indice(self, table):
        try:
            # Create a connection
            conn = pymysql.connect(self.host_name, self.user_name, self.pwd, self.db_name, self.port)
            # Create a cursor object with cursor()
            cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
            # Table structure query
            field_str = "Key_name,Column_name,Index_type,Index_comment"
            sql = "SHOW INDEX FROM %s;" % (table["Name"],)
            cursor.execute(sql)
            fields = cursor.fetchall()
            if len(fields) > 0:
                # Folder and file processing
                file_path = self.folder_name + os.sep + 'Documentation base de données.md'
                # Open file, ready to write
                dict_file = open(file_path, 'a', encoding='UTF-8')

                # Generate table header
                dict_file.write('### Index: \n\n')
                dict_file.write('| Nom | Colonne | Type | Description |\n| --- | --- | --- | --- |\n')


                for field in fields:
                    key_name = field['Key_name']
                    column_name = field['Column_name']
                    index_type = field['Index_type']
                    index_comment = (field['Index_comment']).replace("\r", "")
                    index_comment = index_comment.replace("\n", "").capitalize()

                    info = ' | `' + key_name + '` | ' + column_name + ' | ' + index_type + ' | ' + index_comment + ' | '
                    dict_file.write(info)
                    dict_file.write('\n')
                dict_file.close()
            cursor.close()
            conn.close()
        except Exception as e:
            print(e)
            print('Database connection failed, please check the connection information!')
            exit(1)

    def test_conn(self, conn_info):
        "" "test database connection" ""
        try:
            # Create a connection12
            pymysql.connect(conn_info[0], conn_info[1], str(conn_info[2]), conn_info[3], int(conn_info[4]))
            return True
        except Exception as e:
            print(e)
            return False

    def deal_file(self, file_name):
        "" "process storage folders and files" ""
        # Create folder if it does not exist
        if not os.path.exists(self.folder_name):
            os.mkdir(self.folder_name)
        # Delete existing files
        if os.path.isfile(file_name):
            os.unlink(file_name)


# Program execution entry
if __name__ == '__main__':
    conn_info = input(
        'Please input MySQL database connection information (Format: host IP, user name, login password, database name, port), comma separated and input order can not be disordered, for example: 192.168.0.1,root,root,test_db,3306 : ')
    conn_list = conn_info.split(',')
    while conn_info == '' or len(conn_list) != 5:
        conn_info = input(
            'Please input MySQL database connection information (Format: host IP, user name, login password, database name, port), comma separated and input order can not be disordered, for example: 192.168.0.1,root,root,test_db,3306 : ')
        conn_list = conn_info.split(',')

    # Test database connection problems
    dd_test = DataDict(conn_list)
    db_conn = dd_test.test_conn(conn_list)
    # Enter data table name
    dd = DataDict(conn_list)
    dd.run()
