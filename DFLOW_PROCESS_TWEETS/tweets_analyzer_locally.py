#Gcloud DataFlow Pipeline
#Reading tweets from pub/sub


"""A Tweet Analysis Dataflow."""

import argparse
import logging
import re
import sys

from google.cloud import pubsub_v1
from google.cloud import bigquery

import apache_beam as beam
from apache_beam import Pipeline, ParDo, io, DoFn, PTransform, WindowInto
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


# class WordExtractingDoFn(beam.DoFn):
#   """Parse each line of input text into words."""
#   def process(self, element):
#     """Returns an iterator over the words of this element.
#     The element is a line of text.  If the line is blank, note that, too.
#     Args:
#       element: the element being processed
#     Returns:
#       The processed element.
#     """
#     return re.findall(r'[\w\']+', element, re.UNICODE)

#Write your pub/sub topic
TOPIC = "projects/twitternlp-314312/topics/from-tweepy"

class WriteToGCS(beam.DoFn):
    def __init__(self, output_path):
        self.output_path = output_path
    
    def process(self, elements):
        """Write messages in a batch to store locally."""
        print('******ELEMENTS********')
        print(elements)
        print('******END ELEMENT*******')

        filename = "-".join(['timestamp', 'window_start', 'window_end', 'str(shard_id)'])
        with io.WriteToText(self.output_path).open(filename=filename, mode="w") as f:
            for element in elements:
                f.write(f"{element.getKey()},{element.getValue()}\n".encode("utf-8"))
            # for message_body, publish_time in batch:
            #     print(f"Message Body:{message_body} ")
            #     print('**************************************')
            #     print(f"Publish Time{publish_time}")
            #     f.write(f"{message_body},{publish_time}\n".encode("utf-8"))
        


class PubSubToDict(beam.DoFn):
    def process(self, element):
        """pubsub input is a byte string"""
        data = element.decode('utf-8')
        """do some custom transform here"""
        
        return data

#def run(argv=None, save_main_session=True):
def run():

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input_topic',
        default=TOPIC,
        help='Input topic to read Tweets')
    parser.add_argument(
        '--output_path',
        default='/tmp/samples/',
        help='Path of the output  file including the prefix.')
    known_args, pipeline_args = parser.parse_known_args()
    print(known_args)
    print('///****///')
    print(pipeline_args)

    output_path = known_args.output_path
    #Create the Pipeline
    # Set `save_main_session` to True so DoFns can access globally imported modules.
    '''
    save_main_session cause the state of the global namespace to be pickled and loaded on the Dataflow worker
    '''
    pipeline_options = PipelineOptions(
        pipeline_args, streaming=True, save_main_session=True
    )

    #Output_types is a way to declare Type Hints Inline
    with Pipeline(options=pipeline_options) as pipeline:
    
        (
            pipeline
            # Because `timestamp_attribute` is unspecified in `ReadFromPubSub`, Beam
            # binds the publish time returned by the Pub/Sub server for each message
            # to the element's timestamp parameter, accessible via `DoFn.TimestampParam`.
            # https://beam.apache.org/releases/pydoc/current/apache_beam.io.gcp.pubsub.html#apache_beam.io.gcp.pubsub.ReadFromPubSub

            | 'Read from Pub/Sub' >> io.ReadFromPubSub(topic=TOPIC).with_output_types(bytes)
            | 'Decode' >> beam.Map(lambda x: x.decode('utf-8'))
            | 'write to someplace' >> beam.ParDo(WriteToGCS(output_path))
        ) 

    # result = p.run()
    # result.wait_until_finish()
    

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()