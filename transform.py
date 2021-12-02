from __future__ import absolute_import

import argparse
import logging
from datetime import datetime

from past.builtins import unicode

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToBigQuery
from apache_beam.dataframe.io import read_csv
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.internal.clients import bigquery


class AirbnbParser:

    def __init__(self):

        self.input_schema = 'id:int64,name:string,host_id:int64,host_name:string,neighbourhood_group:string,\
                        neighbourhood:string,latitude:float64,longtitude:float64,room_type:string,price:int64,\
                        minimum_nights:int64,number_of_reviews:int64,last_review:string,reviews_per_month:string,\
                        calculated_host_listings_count:int64,availability_365:int64'       

        self.output_schema = 'neighbourhood:string,count:int64'       

        self.input_rec = {
            'id':'',
            'name': '',
            'host_id': '',
            'host_name': '',
            'neighbourhood_group': '',
            'neighbourhood': '',
            'latitude': '',
            'longitude': '',
            'room_type': '',
            'price': '',
            'minimum_nights': '',
            'number_of_reviews': '',
            'last_review': '',
            'reviews_per_month': '',
            'calculated_host_listings_count': '',
            'availability_365':''
        }

        self.output_rec = {
            'neighbourhood': '',
            'count': ''
        }

    def read(self, line):
        data = line.split(',')
        self.input_rec['id'] = data[0]
        self.input_rec['name'] = data[1]
        self.input_rec['host_id'] = data[2]
        self.input_rec['host_name'] = data[3]
        self.input_rec['neighbourhood_group'] = data[4]
        self.input_rec['neighbourhood'] = data[5]
        self.input_rec['latitude'] = data[6]
        self.input_rec['londitude'] = data[7]
        self.input_rec['room_type'] = data[8]
        self.input_rec['price'] = data[9]
        self.input_rec['minimum_nights'] = data[10]
        self.input_rec['number_of_reviews'] = data[11]
        self.input_rec['last_review'] = data[12]
        self.input_rec['reviews_per_month'] = data[13]
        self.input_rec['calculated_host_listings_count'] = data[14]
        self.input_rec['availability_365'] = data[15]

        return self.input_rec

    def parse(self, line):
        data = line.split(',')
        self.output_rec['id'] = data[0]
        self.output_rec['neighbourhood'] = data[1]

        return self.input_rec

class RecordParserOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
      # Use add_value_provider_argument for arguments to be templatable
      # Use add_argument as usual for non-templatable arguments
      parser.add_value_provider_argument(
          '--input',
          default='gs://airbnb-ny-2019-sweta/AB_NYC_2019.csv',
          help='Path of the file in gcs to read from')
    
def run(argv=None):
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--input_data',
        dest='input_data',
        default='path_to_bucket',
        help='Input file to process.')

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    parser_args = pipeline_options.view_as(RecordParserOptions)

    obj = AirbnbParser()
    
    with beam.Pipeline(options=pipeline_options) as p:
        neighbourhood = p | read_csv('gs://airbnb-ny-2019-sweta/AB_NYC_2019.csv') 
        neighbourhood.fillna({'name':"no name",'host_name':"no name", 'last_review':"no review",'reviews_per_month':"no review"}, inplace=True) 
        neighbourhood.to_csv('gs://airbnb-ny-2019-sweta/AB_NYC_2019_cleaned.csv')
        
        # Count of listings by the neighbourhood field
        agg = neighbourhood.groupby('neighbourhood').id.sum()
        output = agg.to_csv('gs://airbnb-ny-2019-sweta/ab_nyc_output.csv-*') \
            |'Read Input File' >> ReadFromText('gs://airbnb-ny-2019-sweta/ab_nyc_output.csv-*') \
            |'Parse Record' >> beam.Map(lambda line: obj.parse(line))
        

        output | 'Insert to BQ' >> beam.io.gcp.bigquery.WriteToBigQuery(
            project='airbnb-2019-sweta',
            dataset='airbnb_2019_df',
            table='airbnb_2019_output',
            schema=obj.output_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )
