import luigi
from time import sleep
import os

class PrintWordTask(luigi.Task):
    path = luigi.Parameter()
    word = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.path)

    def run(self):
        with open(self.path, 'w') as out_file:
            out_file.write(self.word)
            out_file.close()
        
# Add it to the requires of the Hello and World task
# Can't add it to HelloWorld because HelloWorld is dependent on
# Hello and World task

# HelloTask- MakeDirectory() task gets started because it's
# required- WorldTask- MakeDirectory() complete
class MakeDirectory(luigi.Task):
    path = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.path)

    def run(self):
        os.makedirs(self.path)

# Opens a file called hello.txt, sleeps for 30 seconds
# writes hello to it and closes the file

# The output basically says that if hello.txt already exists
# don't recreate the file
class HelloTask(luigi.Task):
    path = luigi.Parameter()

    def run(self):
        # with open('hello.txt', 'w') as hello_file:
        with open(path, 'w') as hello_file:
            hello_file.write("Hello")
            hello_file.close()
    def output(self):
        return luigi.LocalTarget("hello.txt")

    # Check if that directory is already been made
    # only going to run that once
    def requires(self):
        return [
            MakeDirectory(path=os.path.dirname(self.path))
        ]

# Sleeps for 30 seconds, opens a file called world.txt,
# writes World and closes the file

# Same idea as above- if world.txt already exists don't
# recreate the file
class WorldTask(luigi.Task):
    path = luigi.Parameter()

    def run(self):
        # with open('world.txt', 'w') as world_file:
        with open(path, 'w') as world_file:
            world_file.write('World')
            world_file.close()
    def output(self):
        return luigi.LocalTarget('world.txt')
    def requires(self):
        return [
            MakeDirectory(path=os.path.dirname(self.path))
        ]

# Opens the hello.txt, reads a line, opens the world_txt and
# reads a line and then writes a line to hello_world.txt and closes
# the file
class HelloWorldTask(luigi.Task):
    id = luigi.Parameter(default='test')

    # self.input()[0].path refers to the tasks above
    # also retrieving the output from the tasks above
    def run(self):
        with open(self.input()[0].path, 'r') as hello_file:
            hello = hello_file.read()
        with open(self.input()[1].path, 'r') as world_file:
            world = world_file.read()
        with open(self.output().path, 'w') as output_file:
            content = '{} {}!'.format(hello, world)
            output_file.write(content)
            output_file.close()
        # with open('hello.txt', 'r') as hello_file:
        #     hello = hello_file.read()
        # with open('world.txt', 'r') as world_file:
        #     world = world_file.read()
        # with open('hello_world.txt', 'w') as output_file:
        #     content = '{} {}!'.format(hello, world)
        #     output_file.write(content)
        #     output_file.close()

    # List instantiating the Hello Task and the World
    # task you defined before
    def requires(self):
        return [
            HelloTask(
                path = 'results/{}/hello.txt'.format(self.id)
            ),
            WorldTask(
                path = 'results/{}/world.txt'.format(self.id)
            ),
        ]
        # return [HelloTask(), WorldTask()]

    def output(self):
        path = 'results/{}/hello_world.txt'.format(self.id)
        return luigi.LocalTarget(path)
        # return luigi.LocalTarget('hello_world.txt')

if __name__ == '__main__':
    luigi.run()
