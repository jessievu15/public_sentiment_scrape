import boto3

bedrock = boto3.client('bedrock-agent-runtime', region_name='ap-southeast-2')

KB_ID = 'B5VJGONMEI'

response = bedrock.retrieve_and_generate(
    input={
        'text': '''Summarise all the public sentiment signals on Medibank across all sources. Summarise any news on the private health insurance industry.
        Include: a brief summary of all the news articles provided, any trends or patterns you notice, especially for the private health insurance industry.'''
    },
    retrieveAndGenerateConfiguration={
        'type': 'KNOWLEDGE_BASE',
        'knowledgeBaseConfiguration': {
            'knowledgeBaseId': KB_ID,
            'modelArn': 'arn:aws:bedrock:ap-southeast-2::foundation-model/amazon.nova-pro-v1:0',
            'retrievalConfiguration': {
                'vectorSearchConfiguration': {
                    'numberOfResults': 20
                }
            }
        }
    }
)

summary = response['output']['text']
print(summary)

