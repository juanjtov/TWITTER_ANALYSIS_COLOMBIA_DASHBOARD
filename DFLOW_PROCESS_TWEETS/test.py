import argparse

argv=None
parser = argparse.ArgumentParser()
parser.add_argument(
    '--input_topic',
    help='Input topic to read Tweets')
parser.add_argument(
    '--output',
    help='Output file to write results to.')
known_args, pipeline_args = parser.parse_known_args(argv)
print(known_args)
print("******")
print(pipeline_args)