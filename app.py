import boto3
import datetime
import os
import requests

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

SCOPES = ['https://www.googleapis.com/auth/calendar']

s3 = boto3.resource('s3', region_name='ap-northeast-1', aws_access_key_id=os.environ['ACCESS_KEY'], aws_secret_access_key=os.environ['SECRET_ACCESS_KEY'])

def lambda_handler(e, context):
    bucket = s3.Bucket(os.environ['S3_BUCKET_NAME'])
    bucket.download_file('credentials.json', '/tmp/credentials.json')
    creds = service_account.Credentials.from_service_account_file('credentials.json', scopes=SCOPES)
    calendar_name = os.environ['CALENDAR_NAME']
    calendars = calendar_name.split(',')
    try:
        service = build('calendar', 'v3', credentials=creds)

        now = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=9)))
        maxDatetime = now + datetime.timedelta(days=90)

        notion_headers = {
            'Authorization': 'Bearer %s' % (os.environ['NOTION_TOKEN']),
            'Notion-Version': '2022-02-22',
            'Content-Type': 'application/json'
        }
        notion_body = {
            'filter': {
                'and': [
                    {
                        'property': 'Date',
                        'date': {
                            'on_or_after': (now - datetime.timedelta(days=30)).isoformat()
                        }
                    },
                    {
                        'property': 'Date',
                        'date': {
                            'before': maxDatetime.isoformat()
                        }
                    }
                ]
            }
        }
        notion_events = {}
        has_more = True
        while has_more:
            notion_events_response = requests.post('https://api.notion.com/v1/databases/%s/query' % (os.environ['NOTION_DATABASE_ID']), headers=notion_headers, json=notion_body)
            if notion_events_response.status_code != 200:
                print(notion_events_response.json())
                return
            notion_events_response_json = notion_events_response.json()
            for event in notion_events_response_json['results']:
                text = event['properties']['ID']['rich_text']
                if len(text) == 0:
                    continue
                id = text[0]['plain_text']
                title_text = event['properties']['Name']['title']
                title = title_text[0]['plain_text'] if len(title_text) > 0 else ''
                event_start = datetime.datetime.fromisoformat(event['properties']['Date']['date']['start'])
                event_end = datetime.datetime.fromisoformat(event['properties']['Date']['date']['end']) if event['properties']['Date']['date']['end'] is not None else event_start + datetime.timedelta(days=1) - datetime.timedelta(seconds=1)
                notion_events[id] = {
                    'page_id': event['id'],
                    'title': title,
                    'start': event_start,
                    'end': event_end
                }
            has_more = notion_events_response_json['has_more']
            if has_more:
                notion_body['start_cursor'] = notion_events_response_json['next_cursor']
        for calendar in calendars:
            events_result = service.events().list(
                calendarId=calendar,
                timeMin=now.isoformat(),
                timeMax=maxDatetime.isoformat(),
                maxResults=2500,
                singleEvents=True,
                orderBy='startTime'
            ).execute()
            events = events_result.get('items', [])
            if not events:
                return
            for event in events:
                startDate = datetime.datetime.fromisoformat(event['start']['dateTime']) if 'dateTime' in event['start'] else datetime.datetime.fromisoformat(event['start']['date'])
                endDate = datetime.datetime.fromisoformat(event['end']['dateTime']) if 'dateTime' in event['end'] else datetime.datetime.fromisoformat(event['end']['date']) - datetime.timedelta(seconds=1)
                is_all_day = True if 'date' in event['start'] and 'date' in event['end'] else False
                is_one_day = False
                if is_all_day:
                    if startDate.strftime('%Y-%m-%d') == endDate.strftime('%Y-%m-%d'):
                        is_one_day = True
                status = 'accepted'
                if 'attendees' in event:
                    for attendee in event['attendees']:
                        if 'self' in attendee and attendee['self']:
                            status = attendee['responseStatus']
                            break
                if status == 'declined':
                    continue
                updating_body = {
                    'properties': {
                        'Name': {
                            'title': [
                                {
                                    'type': 'text',
                                    'text': {
                                        'content': event['summary']
                                    }
                                }
                            ]
                        },
                        'Date': {
                            'date': {
                                'start': startDate.strftime('%Y-%m-%d') if is_all_day else startDate.isoformat(),
                                'end': (None if is_one_day else endDate.strftime('%Y-%m-%d')) if is_all_day else endDate.isoformat()
                            }
                        },
                        'ID': {
                            'rich_text': [
                                {
                                    'type': 'text',
                                    'text': {
                                        'content': event['id']
                                    }
                                }
                            ]
                        }
                    }
                }
                if event['id'] in notion_events:
                    notion_event = notion_events.pop(event['id'])
                    if (
                        notion_event['title'] == event['summary'] and
                        notion_event['start'].timestamp() == startDate.timestamp() and
                        notion_event['end'].timestamp() == endDate.timestamp()
                    ):
                        continue
                    requests.patch('https://api.notion.com/v1/pages/%s' % (notion_event['page_id']), headers=notion_headers, json=updating_body)
                    continue
                updating_body['parent'] = {
                    'database_id': os.environ['NOTION_DATABASE_ID']
                }
                requests.post('https://api.notion.com/v1/pages', headers=notion_headers, json=updating_body)
        for key in notion_events:
            event_to_delete = notion_events[key]
            if event_to_delete['end'].timestamp() < now.timestamp():
                continue
            requests.patch('https://api.notion.com/v1/pages/%s' % (event_to_delete['page_id']), headers=notion_headers, json={'archived': True})
    except HttpError as error:
        print(error)
