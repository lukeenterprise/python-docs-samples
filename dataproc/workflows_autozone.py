#!/usr/bin/env python

# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This sample walks a user through creating a workflow using autozone 
# for Cloud Dataproc using the Python client library.

# [START dataproc_inline_workflow_autozone]
from google.cloud import dataproc_v1 as dataproc


def instantiate_inline_workflow(project_id, region):
    """This sample walks a user through submitting a workflow for a Cloud Dataproc 
       using the Python client library.

       Args:
           project_id (string): Project to use for creating resources.
           region (string): Region where the resources should live.
           cluster_name (string): Name to use for creating a cluster.
    """

    # Create a client with the endpoint set to the desired cluster region.
    workflow_client = dataproc.WorkflowTemplateServiceClient(
        client_options={'api_endpoint': '{}-dataproc.googleapis.com:443'.format(region)}
    )
    
    parent = workflow_client.region_path(project_id, region)

    template = {
        'jobs': [{
            'hadoop_job':{
                'main_jar_file_uri': 'file:///usr/lib/hadoop-mapreduce/hadoop-mapreduce-examples.jar',
                'args': [
                    'teragen', 
                    '1000', 
                    'hdfs:///gen/'
                ]
            },
            'step_id': 'teragen'
        },
        {
            'hadoop_job': {
                'main_jar_file_uri': 'file:///usr/lib/hadoop-mapreduce/hadoop-mapreduce-examples.jar',
                'args': ['terasert', 'hdfs:///gen/', 'hdfs:///sort/']
            },
            'step_id': 'terasort',
            'prerequisite_step_ids': [
                'teragen'
            ]  
        }],
        'placement': {
            'managed_cluster': {
                'cluster_name': 'my-managed-cluster',
                'config': {
                    'gce_cluster_config': {
                        'zone_uri': ''
                    }
                }
            }
        }  
    }

    # Submit the request to instantiate the workflow from an inline template. 
    operation = workflow_client.instantiate_inline_workflow(parent, template)
    result = operation.result()

    # Output a success message.
    print('Workflow ran successfully.')
# [START dataproc_inline_workflow_managed_cluster]
