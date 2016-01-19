from __future__ import print_function
import pprint
import feedparser
import boto3
import json
import base64
import re
import time
from httplib2 import Http
from urllib import quote_plus
from urlparse import parse_qs, urlsplit
from pytz import timezone
from datetime import datetime
import requests
from bs4 import BeautifulSoup

from oauth2client.client import SignedJwtAssertionCredentials
from oauth2client.client import flow_from_clientsecrets
from apiclient.discovery import build
from apiclient.errors import HttpError
from apiclient.http import BatchHttpRequest

centralTz = timezone('America/Chicago')

def getGoogleService():
    kms = boto3.client('kms')
    print("Loading encrypted credentials from ./credentials.json")
    jsonCreds = json.load(open('credentials.json'))
    decryptedJson = kms.decrypt(CiphertextBlob=base64.b64decode(jsonCreds['CiphertextBlob']))
    credentials = json.loads(decryptedJson['Plaintext'])
   
    print("Credentials loaded for project ID %s (type %s)" % (credentials['project_id'], credentials['type']))
    print("Credentials client email %s (private key ID %s)" % (credentials['client_email'], credentials['private_key_id']))

    client_email = credentials['client_email']
    private_key = credentials['private_key']
    httpCreds = SignedJwtAssertionCredentials(client_email, private_key, 'https://www.googleapis.com/auth/calendar')
    http = Http()
    print("Authorizing Google Calendar APIs with credentials")
    httpCreds.authorize(http)

    print("Building Google Calendar API service object")
    calendar = build('calendar', 'v3', http=http)
    print("Successfully logged in to Google Calendar API")
    return calendar

def googleApiCall(function, retries=5):
    for i in range(0,retries):
        try:
            return function()
        except HttpError as e:
            print(dir(http_status))
            http_status = int(e['resp']['status'])
            if http_status == 403:
                print("Error 403 during Google API call. Waiting %s seconds and retrying." % str(2**retries))
                time.sleep(2**retries)
            else:
                raise

def getCalendarList(service):
    googleCalendarList = {}
    pageToken = None
    while True:
        clist = service.calendarList().list(pageToken=pageToken).execute()
        for c in clist['items']:
            googleCalendarList[c['id']] = c
        if 'nextPageToken' in clist:
            pageToken = clist['nextPageToken']
        else:
            break
    return googleCalendarList

def augmentEntryDatetimes(e):
    # Entry 'published' strftime and 'published_parsed' time.struct_time only refer
    # to the start time of an event. For all-day events, the time starts at midnight Central Timezone.
    # For events spanning multiple days, a date range is only present in the title.
    # Similarly, for an event specifying an end time, the tiem is only present in the title.
    # 
    # Examples:
    #
    # All-day event (single day):
    #   { 
    #     'title': u'Description: 1/13/2016',
    #     'published': u'Wed, 13 Jan 2016 06:00:00 GMT',
    #     'published_parsed': time.struct_time(tm_year=2016, tm_mon=1, tm_mday=13, tm_hour=6, tm_min=0, tm_sec=0, tm_wday=2, tm_yday=13, tm_isdst=0)
    #   }
    #   * Note: tm_isdst=0 here, which is correct  (because 'published' time is 6 hours from GMT, meaning CST or -6 GMT is in effect.)
    #
    # All-day event (multiple days):
    #   { 
    #     'title': u'Description: 7/19/2017 - 7/28/2017',
    #     'published': u'Wed, 19 Jul 2017 05:00:00 GMT',
    #     'published_parsed': time.struct_time(tm_year=2017, tm_mon=7, tm_mday=19, tm_hour=5, tm_min=0, tm_sec=0, tm_wday=2, tm_yday=200, tm_isdst=0)
    #   }
    #   * Note: tm_isdst=0 here, even though it should be 1 (because 'published' time is 5 hours from GMT, meaning CDT or -5 GMT is in effect.)
    #
    # Time-bounded event (single day):
    #   { 
    #     'title': u'Description: 1/17/2016 2 PM - 3:30 PM',
    #     'published': u'Sun, 17 Jan 2016 20:00:00 GMT',
    #     'published_parsed': time.struct_time(tm_year=2016, tm_mon=1, tm_mday=17, tm_hour=20, tm_min=0, tm_sec=0, tm_wday=6, tm_yday=17, tm_isdst=0)
    #   }
    #   * Note: tm_isdst=0 here, which is correct  (because 'published' time is 6 hours from GMT, meaning CST or -6 GMT is in effect.)
    #
    # Time-bounded event (multiple days):
    #   { 
    #     'title': u'Description: 1/22/2016 5:30 PM - 1/23/2016 10 AM',
    #     'published': u'Fri, 22 Jan 2016 23:30:00 GMT',
    #     'published_parsed': time.struct_time(tm_year=2016, tm_mon=1, tm_mday=22, tm_hour=23, tm_min=30, tm_sec=0, tm_wday=4, tm_yday=22, tm_isdst=0)
    #   }
    #   * Note: tm_isdst=0 here, which is correct  (because 'published' time is 6 hours from GMT, meaning CST or -6 GMT is in effect.)

    matches = re.match(r"^(.*): ([0-9]{1,2}/[0-9]{1,2}/[0-9]{4})$", e['title'])
    if matches:
        e['allDay'] = True
        e['startDatetime'] = centralTz.localize(datetime.strptime(matches.groups()[1], '%m/%d/%Y'))
        e['stopDatetime'] = None
        e['shortTitle'] = matches.groups()[0]
    else:
        matches = re.match(r"^(.*): ([0-9]{1,2}/[0-9]{1,2}/[0-9]{4}) - ([0-9]{1,2}/[0-9]{1,2}/[0-9]{4})$", e['title'])
        if matches:
            e['allDay'] = True
            e['startDatetime'] = centralTz.localize(datetime.strptime(matches.groups()[1], '%m/%d/%Y'))
            e['stopDatetime'] = centralTz.localize(datetime.strptime(matches.groups()[2], '%m/%d/%Y'))
            e['shortTitle'] = matches.groups()[0]
        else:
            matches = re.match(r"^(.*): ([0-9]{1,2}/[0-9]{1,2}/[0-9]{4}) ([0-9]{1,2}(:[0-9]{2})? (AM|PM))$", e['title'])
            if matches:
                e['allDay'] = False
                e['startDatetime'] = centralTz.localize(datetime.strptime(matches.groups()[1] + ' ' + matches.groups()[2], '%%m/%%d/%%Y %%I%s %%p' % (':%M' if matches.groups()[3] else '')))
                e['stopDatetime'] = None
                e['shortTitle'] = matches.groups()[0]
            else: 
                matches = re.match(r"^(.*): ([0-9]{1,2}/[0-9]{1,2}/[0-9]{4}) ([0-9]{1,2}(:[0-9]{2})? (AM|PM)) - ([0-9]{1,2}(:[0-9]{2})? (AM|PM))$", e['title'])
                if matches:
                    e['allDay'] = False
                    e['startDatetime'] = centralTz.localize(datetime.strptime(matches.groups()[1] + ' ' + matches.groups()[2], '%%m/%%d/%%Y %%I%s %%p' % (':%M' if matches.groups()[3] else '')))
                    e['stopDatetime'] = centralTz.localize(datetime.strptime(matches.groups()[1] + ' ' + matches.groups()[5], '%%m/%%d/%%Y %%I%s %%p' % (':%M' if matches.groups()[6] else '')))
                    e['shortTitle'] = matches.groups()[0]
                else:
                    matches = re.match(r"^(.*): ([0-9]{1,2}/[0-9]{1,2}/[0-9]{4}) ([0-9]{1,2}(:[0-9]{2})? (AM|PM)) - ([0-9]{1,2}/[0-9]{1,2}/[0-9]{4}) ([0-9]{1,2}(:[0-9]{2})? (AM|PM))$", e['title'])
                    if matches:
                        e['allDay'] = False
                        e['startDatetime'] = centralTz.localize(datetime.strptime(matches.groups()[1] + ' ' + matches.groups()[2], '%%m/%%d/%%Y %%I%s %%p' % (':%M' if matches.groups()[3] else '')))
                        e['stopDatetime'] = centralTz.localize(datetime.strptime(matches.groups()[5] + ' ' + matches.groups()[6], '%%m/%%d/%%Y %%I%s %%p' % (':%M' if matches.groups()[7] else '')))
                        e['shortTitle'] = matches.groups()[0]
                    else:
                        print("WARNING: Event failed to be augmented. Event details are:")
                        pprint.pprint(e)
    return e

def createRssEventUrl(orgId):
    return 'http://midiowacouncilbsa.doubleknot.com/rss/RSS_Events.aspx?subscriblink=TRUE&orglist=' + str(orgId) + '&orgkey=' + str(orgId) + '&cnt=500&hideDesc=TRUE'

def getRssEvents(orgId):
    events = {}
    d = feedparser.parse(createRssEventUrl(orgId))
    for entry in d['entries']:
        if 'id' in entry:
            events[entry['id']] = augmentEntryDatetimes(entry)
        
    return {'summary': d['feed']['title_detail']['value'], 'events': events}

def findMinMaxRssDatetime(events):
    maxDatetime = centralTz.localize(datetime(1970, 1, 1, 0, 0))
    minDatetime = centralTz.localize(datetime(2970, 1, 1, 0, 0))
    for eventId, e in events.items():
        if 'startDatetime' not in e:
            continue
        eventDatetime = e['stopDatetime'] if 'stopDatetime' in e and e['stopDatetime'] is not None else e['startDatetime']
        if eventDatetime is not None:
            if eventDatetime > maxDatetime:
                if e['allDay']:
                    maxDatetime = eventDatetime.replace(hour=23, minute=59, second=59)
                else:
                    maxDatetime = eventDatetime
        eventDatetime = e['startDatetime']
        if eventDatetime is not None:
            if eventDatetime < minDatetime:
                if e['allDay']:
                    minDatetime = eventDatetime.replace(hour=0, minute=0, second=0)
                else:
                    minDatetime = eventDatetime
    return (minDatetime, maxDatetime)


def createCalendar(service, summary):
    calBody = { 
            "kind": "calendar#calendar",
            "description": summary + ' Event and Activities',
            "timeZone": "America/Chicago",
            "summary": summary}
    calAclBody = {
            "kind": "calendar#aclRule",
            "scope": {
                "type": "user",
                "value": "midiowacouncil.services@gmail.com",
            },
            "role": "owner"}
    newcal = googleApiCall(lambda: service.calendars().insert(body=calBody).execute())
    googleApiCall(lambda: service.acl().insert(calendarId=newcal['id'], body=calAclBody).execute())
    return newcal

def getCalendarEvents(service, calendarId, minDatetime=None, maxDatetime=None):
    calEvents = {}
    pageToken = None
    while True:
        eventList = googleApiCall(lambda: service.events().list(
                        calendarId=calendarId, 
                        timeMin=minDatetime.isoformat("T") if minDatetime else None,
                        timeMax=maxDatetime.isoformat("T") if maxDatetime else None,
                        pageToken=pageToken).execute())
        for event in eventList['items']:
            if 'extendedProperties' in event:
                if 'private' in event['extendedProperties']:
                    if 'rssId' in event['extendedProperties']['private']:
                        calEvents[event['extendedProperties']['private']['rssId']] = event
        if 'nextPageToken' in eventList:
            pageToken = eventList['nextPageToken']
        else:
            break
    return calEvents

def createDatetimeBody(rssEvent, prefix):
    body = {}
    if rssEvent[prefix + 'Datetime'] is not None:
        if rssEvent['allDay'] == True:
            body['date'] = rssEvent[prefix + 'Datetime'].strftime('%Y-%m-%d')
        else:
            body['timeZone'] = rssEvent[prefix + 'Datetime'].tzinfo.zone
            body['dateTime'] = rssEvent[prefix + 'Datetime'].strftime('%Y-%m-%dT%H:%M:%S')
    return body

def isDatetimeBodyDiff(a, b):
    # If timeZone is present, dateTime is present
    # Otherwise, date is present
    if 'timeZone' in a and 'timeZone' in b:
        if 'dateTime' in a and 'dateTime' in b:
            aDatetime = re.split('-[0-9]{1,2}:[0-9]{2}$|\+[0-9]{1,2}:[0-9]{2}$', a['dateTime'])[0]
            bDatetime = re.split('-[0-9]{1,2}:[0-9]{2}$|\+[0-9]{1,2}:[0-9]{2}$', b['dateTime'])[0]
            if aDatetime != bDatetime:
                print("aDT = %s, bDT = %s" %(aDatetime, bDatetime))
                return True
        else:
            print("dateTime not in both but TZ present in both")
            return True
    elif 'timeZone' in a or 'timeZone' in b:
        print("TZ present in one but not both")
        return True
    if 'date' in a and 'date' in b:
        if a['date'] != b['date']:
            print("dates differ")
            return True
    elif 'date' in a or 'date' in b:
        return True
    return False

def compareOrCreateEvent(service, calendarId, batchRequest, rssEvent, calEvents):
    if rssEvent['id'] not in calEvents:
        startBody = createDatetimeBody(rssEvent, 'start')
        endBody = createDatetimeBody(rssEvent, 'stop')
        eventBody = {
                "kind": "calendar#event",
                "start": startBody,
                "summary": rssEvent['shortTitle'],
                "description": rssEvent['link'],
                "extendedProperties": {
                    "private": {
                        "rssId": rssEvent['id']}}}
        if endBody:
            eventBody['end'] = endBody
        else:
            eventBody['end'] = startBody
        location = getEventLocation(rssEvent['link'])
        if location:
            eventBody['location'] = location
        print("Creating new event: %s (all-day: %s, start: %s, end: %s)" % (
            rssEvent['shortTitle'], 
            str(rssEvent['allDay']), 
            eventBody['start']['dateTime'] if 'dateTime' in eventBody['start'] else eventBody['start']['date'], 
            eventBody['end']['dateTime'] if 'dateTime' in eventBody['end'] else eventBody['end']['date']))
        batchRequest.add(service.events().insert(calendarId=calendarId, body=eventBody))
        return True
    else:
        print("Found existing event: %s (%s)" % (rssEvent['shortTitle'], rssEvent['id']))
        startBody = createDatetimeBody(rssEvent, 'start')
        endBody = createDatetimeBody(rssEvent, 'stop')
        eventBody = {
                "kind": "calendar#event",
                "start": startBody,
                "summary": rssEvent['shortTitle'],
                "description": rssEvent['link'],
                "extendedProperties": {
                    "private": {
                        "rssId": rssEvent['id']}}}
        if endBody:
            eventBody['end'] = endBody
        else:
            eventBody['end'] = startBody
        location = getEventLocation(rssEvent['link'])
        if location:
            eventBody['location'] = location

        eventChanged = False

        if isDatetimeBodyDiff(eventBody['start'], calEvents[rssEvent['id']]['start']) or \
           isDatetimeBodyDiff(eventBody['end'], calEvents[rssEvent['id']]['end']):
            print("Event start/end changed")
            eventChanged = True
        if eventBody['summary'] != calEvents[rssEvent['id']]['summary'] or \
           eventBody['description'] != calEvents[rssEvent['id']]['description']:
            print("Event summary/description changed")
            eventChanged = True

        if eventChanged:
            print("Event has changed. Updating.")
            print("Calculated event body:")
            pprint.pprint(eventBody)
            print("Google calendar event body:")
            pprint.pprint(calEvents[rssEvent['id']])

            batchRequest.add(service.events().update(calendarId=calendarId, eventId=calEvents[rssEvent['id']]['id'], body=eventBody))
            return True
    return False

def isValidEvent(rssEvent):
    return 'id' in rssEvent and 'startDatetime' in rssEvent

def getEventLocation(eventUrl):
    r = requests.get(eventUrl)
    #doc = html.fromstring(r.content)
    #mapsUrl = doc.xpath('//a[starts-with(@href, "http://maps.google.com/")]')
    doc = BeautifulSoup(r.content, 'html.parser')
    mapsUrls = doc.find_all('a', href=re.compile("^http://maps.google.com/"))
    if mapsUrls:
        url = urlsplit(mapsUrls[0].get('href'))
        return parse_qs(url.query)['q'][0]
    return None

def cleanAllEvents():
    ORG_LIST = [ 1935, 1940, 1936, 1944, 1937, 1938, 1943, 2103, 1941, 1939, 1945, 1942 ]
    calendarService = getGoogleService()
    print("Retrieving list of Google Calendars")
    googleCalendarList = getCalendarList(calendarService)

    print("Processing RSS events for Org. IDs: %s" % ', '.join([str(o) for o in ORG_LIST]))
    for org in ORG_LIST:
        print("Retrieving RSS feed at \"%s\"" % createRssEventUrl(org))
        rssEvents = getRssEvents(org)
        print("Found %s RSS feed events" % len(rssEvents['events']))
        for googleCalendar in googleCalendarList.itervalues():
            if rssEvents['summary'] == googleCalendar['summary']:
                print("Found existing google calendar \"%s\" - \"%s\"" % (googleCalendar['id'], googleCalendar['summary']))
                calEvents = getCalendarEvents(calendarService, googleCalendar['id'])
                batchRequest = BatchHttpRequest()
                print("Deleting %s events" % len(calEvents))
                for e in calEvents.itervalues():
                    batchRequest.add(calendarService.events().delete(calendarId = googleCalendar['id'], eventId = e['id']))
                batchRequest.execute()
                break

def listCalendarUrls():
    ORG_LIST = [ 1935, 1940, 1936, 1944, 1937, 1938, 1943, 2103, 1941, 1939, 1945, 1942 ]
    calendarService = getGoogleService()
    print("Retrieving list of Google Calendars")
    googleCalendarList = getCalendarList(calendarService)

def lambda_handler(event, context):
    ORG_LIST = [ 1935, 1940, 1936, 1944, 1937, 1938, 1943, 2103, 1941, 1939, 1945, 1942 ]
    calendarService = getGoogleService()
    print("Retrieving list of Google Calendars")
    googleCalendarList = getCalendarList(calendarService)

    print("Processing RSS events for Org. IDs: %s" % ', '.join([str(o) for o in ORG_LIST]))
    for org in ORG_LIST:
        print("Retrieving RSS feed at \"%s\"" % createRssEventUrl(org))
        rssEvents = getRssEvents(org)
        print("Found %s RSS feed events" % len(rssEvents['events']))

        # Find last-dated item in RSS feed
        minEventDatetime, maxEventDatetime = findMinMaxRssDatetime(rssEvents['events'])
        
        # Retrieve existing calendars. Create if not found.
        targetCalendar = None
        for googleCalendar in googleCalendarList.itervalues():
            if rssEvents['summary'] == googleCalendar['summary']:
                print("Found existing google calendar \"%s\" - \"%s\"" % (googleCalendar['id'], googleCalendar['summary']))
                targetCalendar = googleCalendar
                break

        if targetCalendar is None:
            print("Creating new google calendar \"%s\"" % rssEvents['summary'])
            targetCalendar = createCalendar(calendarService, rssEvents['summary'])
            print("Created new calendar \"%s\" - \"%s\"" % (targetCalendar['id'], targetCalendar['summary']))

        # Read and cache google events up to last-dated RSS item
        print("Retrieving all events in calendar \"%s\" from %s to %s" % (targetCalendar['summary'], minEventDatetime.isoformat('T'), maxEventDatetime.isoformat('T')))
        calEvents = getCalendarEvents(calendarService, targetCalendar['id'], minEventDatetime, maxEventDatetime)
        print("Found %s Google Calendar events" % len(calEvents))

        # Compare RSS item to google cached events: if missing, create
        batchCount = 0
        createCount = 0
        batchRequest = BatchHttpRequest()
        for rssEvent in rssEvents['events'].itervalues():
            if isValidEvent(rssEvent):
                if compareOrCreateEvent(calendarService, targetCalendar['id'], batchRequest, rssEvent, calEvents):
                    batchCount += 1
            if batchCount == 100:
                createCount += batchCount
                print("Calling batch request to create %s events" % batchCount)
                googleApiCall(lambda: batchRequest.execute())
                batchCount = 0
                batchRequest = BatchHttpRequest()
        if batchCount:
            createCount += batchCount
            print("Calling batch request to create %s events" % batchCount)
            googleApiCall(lambda: batchRequest.execute())

        if createCount + len(calEvents) != len(rssEvents['events']):
            print("WARNING: Mismatched event counts: %s RSS events, %s existing Google Calendar Events, %s created Google Calendar Events" % (
                len(rssEvents['events']), len(calEvents), createCount))

    print("Done processesing all RSS feeds and Google Calendar events")
    print("Exiting")

if __name__ == '__main__':
    lambda_handler(None, None)
