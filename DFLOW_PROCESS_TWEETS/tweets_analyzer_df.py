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


class WriteToGCS(DoFn):
    def __init__(self, output_path):
        self.output_path = output_path

    def process(self, key_value):
        """Write messages in a batch to Google Cloud Storage."""

        # ts_format = "%H:%M"
        # window_start = window.start.to_utc_datetime().strftime(ts_format)
        # window_end = window.end.to_utc_datetime().strftime(ts_format)
        shard_id, batch = key_value
        filename = "-".join([self.output_path, 'window_start', 'window_end', 'str(shard_id)'])

        with io.gcsio.GcsIO().open(filename=filename, mode="w") as f:
            for message_body, publish_time in batch:
                print(f"Message Body:{message_body} ")
                print('**************************************')
                print(f"Publish Time{publish_time}")
                f.write(f"{message_body},{publish_time}\n".encode("utf-8"))

#Write your pub/sub topic
TOPIC = "projects/twitternlp-314312/topics/from-tweepy"

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

    # parser.add_argument(
    #     '--staging_location',
    #     default='gs://tweets_pubsub_buckt/staging/',
    #     help='Staging Location')

    # parser.add_argument(
    #     '--temp_location',
    #     default='gs://tweets_pubsub_buckt/staging/',
    #     help='Staging Location')

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
            | 'write to GCS' >> beam.ParDo(WriteToGCS(output_path))
            # | 'Write to GCS' >> io.WriteToText(output_path)
        ) 

   
    

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()