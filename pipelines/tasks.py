import csv
import os
import psycopg2


class BaseTask:
    """Base Pipeline Task"""
    def __init__(self):
        self.conn = psycopg2.connect(dbname=os.getenv('DBNAME'), user=os.getenv('POSTGRES_USER'),
                                     password=os.getenv('POSTGRES_PASSWORD'), host=os.getenv('HOST'),
                                     port=os.getenv('PORT'))
        self.cursor = self.conn.cursor()

    def run(self):
        raise RuntimeError('Do not run BaseTask!')

    def short_description(self):
        pass

    def __str__(self):
        task_type = self.__class__.__name__
        return f'{task_type}: {self.short_description()}'


class CopyToFile(BaseTask):
    """Copy table data to CSV file"""

    def __init__(self, table, output_file):
        super().__init__()
        self.table = table
        self.output_file = output_file

    def short_description(self):
        return f'{self.table} -> {self.output_file}'

    def run(self):
        copy_table_to_file_command = f"COPY {self.table} TO STDOUT DELIMITER ',' CSV HEADER"
        with open(f'{self.output_file}', "w") as file:
            self.cursor.copy_expert(copy_table_to_file_command, file)
        print(f"Copy table `{self.table}` to file `{self.output_file}`")

        self.conn.commit()


class LoadFile(BaseTask):
    """Load file to table"""

    def __init__(self, table, input_file):
        super().__init__()
        self.table = table
        self.input_file = input_file

    def short_description(self):
        return f'{self.input_file} -> {self.table}'

    def run(self):
        input_file = open(f"{self.input_file}", "r")
        columns = input_file.readline().strip().split(",")

        create_query = f"DROP TABLE IF EXISTS {self.table}; \n"
        create_query += f"CREATE TABLE {self.table} ( "
        for column in columns:
            create_query += column + " VARCHAR, \n "
        create_query = create_query[:-4]
        create_query += ");"
        self.cursor.execute(create_query)
        self.conn.commit()
        print(f"........Table '{self.table}' has been created successfully........")

        self.cursor.copy_from(input_file, f'{self.table}', sep=',')
        input_file.close()
        self.conn.commit()
        print("...............Data has been added successfully...............\n")

        print(f"Load file `{self.input_file}` to table `{self.table}`:")
        self.cursor.execute(f"SELECT * FROM {self.table}")
        records = self.cursor.fetchall()
        for row in records:
            print("Id = ", row[0])
            print("Name = ", row[1])
            print("Url  = ", row[2], "\n")

        self.conn.commit()


class RunSQL(BaseTask):
    """Run custom SQL query"""

    def __init__(self, sql_query, title=None):
        super().__init__()
        self.title = title
        self.sql_query = sql_query

    def short_description(self):
        return f'{self.title}'

    def run(self):
        self.cursor.execute(self.sql_query)
        print(f"Run SQL ({self.title}):\n{self.sql_query}")
        self.cursor.close()
        self.conn.close()

postgresql_func = """
    CREATE OR REPLACE FUNCTION domain_of_url(url TEXT) 
    RETURNS TEXT AS $$
    DECLARE domain TEXT;
    BEGIN 
        SELECT split_part(url, '/', 3) INTO domain;
        RETURN domain;
    END;
    $$ LANGUAGE plpgsql; 
"""

class CTAS(BaseTask):
    """SQL Create Table As Task"""

    def __init__(self, table, sql_query, title=None):
        super().__init__()
        self.table = table
        self.sql_query = sql_query
        self.title = title or table

    def short_description(self):
        return f'{self.title}'

    def run(self):

        self.cursor.execute(f"DROP TABLE IF EXISTS {self.table}")
        self.cursor.execute(postgresql_func)
        self.conn.commit()
        create_table_command = f"CREATE TABLE {self.table} AS {self.sql_query}"
        self.cursor.execute(create_table_command)

        print(f"........Table '{self.table}' has been created successfully........")

        self.cursor.execute(f"SELECT * FROM {self.table}")
        norm_records = self.cursor.fetchall()
        for row in norm_records:
            print("Id = ", row[0])
            print("Name = ", row[1])
            print("Url  = ", row[2])
            print("Domain_of_url  = ", row[3], '\n')

        self.conn.commit()
