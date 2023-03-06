import csv
import os
import psycopg2


conn = psycopg2.connect(dbname=os.getenv('DBNAME'), user='arina',
                        password=os.getenv('POSTGRES_PASSWORD'), host='postgresql',
                        port=os.getenv('PORT'))
cursor = conn.cursor()


class BaseTask:
    """Base Pipeline Task"""

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
        self.table = table
        self.output_file = output_file

    def short_description(self):
        return f'{self.table} -> {self.output_file}'

    def run(self):
        copy_table_to_file_command = "COPY norm TO STDOUT DELIMITER ',' CSV HEADER"
        with open(f'{self.output_file}', "w") as file:
            cursor.copy_expert(copy_table_to_file_command, file)
        print(f"Copy table `{self.table}` to file `{self.output_file}`")

        conn.commit()


class LoadFile(BaseTask):
    """Load file to table"""

    def __init__(self, table, input_file):
        self.table = table
        self.input_file = input_file

    def short_description(self):
        return f'{self.input_file} -> {self.table}'

    def run(self):
        cursor.execute(f"DROP TABLE IF EXISTS {self.table}")

        create_table_command = (
            f'''
            CREATE TABLE {self.table} (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL, 
                url VARCHAR(100) NOT NULL
            ) 
            '''
        )
        cursor.execute(create_table_command)
        conn.commit()
        print(f"........Table '{self.table}' has been created successfully........")

        with open(self.input_file, 'r') as f:
            reader = csv.reader(f)
            next(reader)
            for row in reader:
                cursor.execute(f"INSERT INTO {self.table}(id, name, url) VALUES (%s, %s, %s)", row)
        conn.commit()
        print("...............Data has been added successfully...............\n")

        print(f"Load file `{self.input_file}` to table `{self.table}`:")
        cursor.execute(f"SELECT * FROM {self.table}")
        records = cursor.fetchall()
        for row in records:
            print("Id = ", row[0], )
            print("Name = ", row[1])
            print("Url  = ", row[2], "\n")

        conn.commit()


class RunSQL(BaseTask):
    """Run custom SQL query"""

    def __init__(self, sql_query, title=None):
        self.title = title
        self.sql_query = sql_query

    def short_description(self):
        return f'{self.title}'

    def run(self):
        conn = psycopg2.connect(dbname=os.getenv('DBNAME'), user='arina',
                                password=os.getenv('POSTGRES_PASSWORD'), host='postgresql',
                                port=os.getenv('PORT'))
        cursor = conn.cursor()
        cursor.execute(self.sql_query)
        print(f"Run SQL ({self.title}):\n{self.sql_query}")
        cursor.close()
        conn.close()

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
        self.table = table
        self.sql_query = sql_query
        self.title = title or table

    def short_description(self):
        return f'{self.title}'

    def run(self):

        cursor.execute(f"DROP TABLE IF EXISTS {self.table}")
        cursor.execute(postgresql_func)
        conn.commit()
        create_table_command = f"CREATE TABLE {self.table} AS {self.sql_query}"
        cursor.execute(create_table_command)

        print(f"........Table '{self.table}' has been created successfully........")

        cursor.execute(f"SELECT * FROM {self.table}")
        norm_records = cursor.fetchall()
        for row in norm_records:
            print("Id = ", row[0])
            print("Name = ", row[1])
            print("Url  = ", row[2])
            print("Domain_of_url  = ", row[3], '\n')

        conn.commit()
