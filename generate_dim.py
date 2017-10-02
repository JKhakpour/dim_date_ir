
# coding: utf-8

from __future__ import unicode_literals, print_function
from math import ceil
import datetime
import re
from pandas import DataFrame, date_range
from jdatetime import GregorianToJalali, JalaliToGregorian
from umalqurra.hijri_date import HijriDate
import urllib2
from lxml import etree
import gevent
from gevent.threadpool import ThreadPool
import csv
import codecs

en_digits = u'1234567890'
fa_digits = u'۱۲۳۴۵۶۷۸۹۰'
ar_digits = u'١٢٣٤٥٦٧٨٩٠'
en_digits = [ord(char) for char in en_digits]
ar_digits = [ord(char) for char in ar_digits]
en2fa_tbl = dict(zip(en_digits, fa_digits))
ar2fa_tbl = dict(zip(ar_digits, fa_digits))

header = {}
url_pattern = 'http://www.time.ir/fa/event/list/{is_greg}/{year}/{month}/{day}'
events_file = 'dim_date_events.csv'
dim_date_file = 'dim_date.csv'

hijri_months = [
    'محرم',
    'صفر',
    'ربیع الاول',
    'ربیع الثانی',
    'جمادی الاول',
    'جمادی الثانی',
    'رجب',
    'شعبان',
    'رمضان',
    'شوال',
    'ذیقعده',
    'ذیحجه'
]

jalali_months = [
    'فروردین',
    'اردیبهشت',
    'خرداد',
    'تیر',
    'مرداد',
    'شهریور',
    'مهر',
    'آبان',
    'آذر',
    'دی',
    'بهمن',
    'اسفند'
]

gregorian_months_en = [
    'January',
    'February',
    'March',
    'April',
    'May',
    'June',
    'July',
    'August',
    'September',
    'October',
    'November',
    'December'

]
    
gregorian_months_fa = [
    'ژانویه',
    'فوریه',
    'مارس',
    'آوریل',
    'مه',
    'ژوئن',
    'ژوئیه',
    'اوت',
    'سپتامبر',
    'اکتبر',
    'نوامبر',
    'دسامبر'
]

week_days = [
    'دوشنبه',
    'سه شنبه',
    'چهار شنبه',
    'پنج شنبه',
    'جمعه',
    'شنبه',
    'یکشنبه',
]

week_days_en = [
    'Monday',
    'Tuesday',
    'Wednesday',
    'Thursday',
    'Friday',
    'Saturday',
    'Sunday',
]

greg_months_en2fa = { gregorian_months_en[i]:gregorian_months_fa[i] for i in range(len(gregorian_months_en))}
hijri_months_dict = { i:hijri_months[i] for i in range(len(hijri_months))}
jalali_months_dict = { i:jalali_months[i] for i in range(len(jalali_months))}
gregorian_months_en_dict = { i:gregorian_months_en[i] for i in range(len(gregorian_months_en))}
gregorian_months_fa_dict = { i:gregorian_months_fa[i] for i in range(len(gregorian_months_fa))}
week_days_dict = { i:week_days[i] for i in range(len(week_days))}
week_days_en_dict = { i:week_days_en[i] for i in range(len(week_days_en))}

def normalizer(dt_str):
    dt_str = dt_str.strip(' ] [').replace('[','').replace(']','')
    dt_str = dt_str.translate(en2fa_tbl)
    dt_str = dt_str.translate(ar2fa_tbl)
    for month_en, month_fa in greg_months_en2fa.items():
        dt_str = dt_str.replace(month_en, month_fa)
    # change 'MMM dd' to 'dd MMM'
    fa_date = re.sub(ur'(.*) ([۰۱۲۳۴۵۶۷۸۹]+)', ur'\2 \1', dt_str, re.UNICODE)
    return fa_date

def jalaliToGregdatetime(year, month, day):
    greg = JalaliToGregorian(year, month, day)
    return datetime.datetime(greg.gyear, greg.gmonth, greg.gday)

def jalali_week(year, month, day, o_year = None, o_month=1, o_day=1):
    if o_year == None:
        o_year = year
    year_start = jalaliToGregdatetime(o_year, o_month, o_day)
    dt = jalaliToGregdatetime(year, month, day)
    ys_week_day = year_start.weekday()
    if ys_week_day == 6:
        first_week_start = year_start + datetime.timedelta(6)
    else: 
        first_week_start = year_start + datetime.timedelta(5-ys_week_day)
    week_number = int(ceil((dt - first_week_start).days/7.))
    if week_number == 0:
        # compute diff from previous year's origin
        return jalali_week(year, month, day, year-1)
    elif week_number > 0:
        return week_number  

def valid_date(d):
    try:
        return datetime.datetime.strptime(d, "%Y-%m-%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError("%s is not a valid date." %d)


def page_url(year, month, day, is_greg=1):
    return url_pattern.format(is_greg = is_greg, year = year, month = month, day = day)


def get_events(dt):
    year = dt.year
    month = dt.month
    day = dt.day
    req = urllib2.Request(page_url(year, month, day), headers=header)
    page = urllib2.urlopen(req)
    htmlparser = etree.HTMLParser()
    tree = etree.parse(page, htmlparser)
    print('.', end='')
    if day == 1:
        print("%d %d" % (year, month))
    elements = [el for el in tree.xpath("""//*[contains(@class, 'list-unstyled')]/li[not(contains(@class, 'eventHoliday'))]""")]
    for el in elements:
        event_name = [t.strip() for t in el.xpath("text()") if t.strip()][0] if [t for t in el.xpath("text()") if t.strip()] else ""
        date = el.xpath("span[position()= 1]/text()")[0] if el.xpath("span[position()= 1]/text()") else ""
        orig_date = el.xpath("span[position()= 2]/text()")[0] if el.xpath("span[position()= 2]/text()") else date.encode('utf8')
        events.append({'gregorain_date': str(dt),
                        'is_holiday': 0, 
                        'event_name': event_name.encode('utf8'), 
                        'event_origin': normalizer(unicode(orig_date)).encode('utf8')
                        })
    elements = [el for el in tree.xpath("""//*[contains(@class, 'list-unstyled')]/li[contains(@class, 'eventHoliday')]""")]
    for el in elements:
        event_name = [t.strip() for t in el.xpath("text()") if t.strip()][0] if [t for t in el.xpath("text()") if t.strip()] else ""
        date = el.xpath("span[position()= 1]/text()")[0] if el.xpath("span[position()= 1]/text()") else ""
        orig_date = el.xpath("span[position()= 2]/text()")[0] if el.xpath("span[position()= 2]/text()") else date.encode('utf8')
        events.append({'gregorain_date': str(dt), 
                        'is_holiday': 1, 
                        'event_name': event_name.encode('utf8'), 
                        'event_origin': normalizer(unicode(orig_date)).encode('utf8')
                        })
events = []
def crawl(start_date, end_date):
    global events
    events= []
    pool = ThreadPool(20)

    for dt in date_range(start_date, end_date):
        pool.spawn(get_events, dt)

    gevent.wait()

    #columns = reduce(lambda s1, s2: s1.union(s2), (set(e.keys()) for e in events))
    columns = [u'gregorain_date', u'gregorian_year', u'gregorian_month', u'gregorian_day', u'is_holiday', u'event_origin', u'event_name']
    with codecs.open(events_file, 'w+') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=columns)
        writer.writeheader()
        for e in events:
            writer.writerow(e)


def genrate_dim_date(start_date, end_date):
    data = DataFrame({'gregorian_date': date_range(start_date, end_date)})
    data['gregorian_year'] = data.apply(lambda x: x.gregorian_date.year, axis=1)
    data['gregorian_month'] = data.apply(lambda x: x.gregorian_date.month, axis=1)
    data['gregorian_day'] = data.apply(lambda x: x.gregorian_date.day, axis=1)

    data['jalali_year'] = data.apply(lambda x: GregorianToJalali(x.gregorian_year,x.gregorian_month,x.gregorian_day).jyear, axis=1)
    data['jalali_month'] = data.apply(lambda x: GregorianToJalali(x.gregorian_year,x.gregorian_month,x.gregorian_day).jmonth, axis=1)
    data['jalali_day'] = data.apply(lambda x: GregorianToJalali(x.gregorian_year,x.gregorian_month,x.gregorian_day).jday, axis=1)
    data['jalali_month_name'] = data.jalali_month.map(jalali_months_dict)
    data['week_day'] = data.apply(lambda x:  datetime.datetime(x.gregorian_year,x.gregorian_month,x.gregorian_day).weekday(), axis=1).map(week_days_dict)
    data['week_day_en'] = data.apply(lambda x:  datetime.datetime(x.gregorian_year,x.gregorian_month,x.gregorian_day).weekday(), axis=1).map(week_days_en_dict)

    data['hijri_year'] = data.apply(lambda x: int(HijriDate(x.gregorian_year,x.gregorian_month,x.gregorian_day, gr=True).year), axis=1)
    data['hijri_month'] = data.apply(lambda x: int(HijriDate(x.gregorian_year,x.gregorian_month,x.gregorian_day, gr=True).month), axis=1)
    data['hijri_day'] = data.apply(lambda x: int(HijriDate(x.gregorian_year,x.gregorian_month,x.gregorian_day, gr=True).day), axis=1)
    data['hijri_month_name'] = data.hijri_month.map(hijri_months_dict)

    data['gregorian_month_name_en'] = data.gregorian_month.map(gregorian_months_en_dict)
    data['gregorian_month_name_fa'] = data.gregorian_month.map(gregorian_months_fa_dict)
    data['gregorian_week_number'] = data.apply(lambda x: datetime.date(x.gregorian_year,x.gregorian_month,x.gregorian_day).isocalendar()[1], axis=1 )
    data['jalali_week_number'] = data.apply(lambda x: jalali_week(x.jalali_year,x.jalali_month,x.jalali_day), axis=1 )

    data.to_csv(dim_date_file, encoding='utf8')


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Generate dim_date data with Gregorain, Hijri and Jalali dates. This module also crawls time.ir data for you :)")
    parser.add_argument('--start', help='start date in YYYY-MM-DD format', default='1991-3-21', type=valid_date)
    parser.add_argument('--end', help='end date in YYYY-MM-DD format', default='2031-3-22', type=valid_date)
    # parser.add_argument('--end', help='end date in YYYY-MM-DD format', default='1991-4-22', type=valid_date) # for test on smaller intervals
    parser.add_argument('--crawl', help='crawl time.ir data', action='store_true')
    parser.add_argument('--only-crawl', help='just crawl time.ir data (dont generate dim_date file)', action='store_true')
    args = parser.parse_args()
    print(args.end)
    print(args)
    if args.crawl or args.only_crawl:
        print('Crawling data from time.ir into dim_date_events')
        crawl(args.start, args.end)
        print('string to %s' % events_file)
    
    if not args.only_crawl:
        print('Generating dim_date file')
        genrate_dim_date(args.start, args.end)
        print('storing to %s' % dim_date_file)

