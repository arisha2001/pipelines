import click


class Pipeline:
    def __init__(self, name, version, tasks):
        self.name = name
        self.version = version
        self.tasks = tasks

    def add_data(self):
        print('ooo')

    def run(self):
        for task_number, task in enumerate(self.tasks, start=1):
            click.secho(f"Running tasks number {task_number}:", fg='green')
            task.run()
            print()

    def list(self):
        print('Tasks:')
        for task_number, task in enumerate(self.tasks, start=1):
            print(f"{task_number}: {task}")