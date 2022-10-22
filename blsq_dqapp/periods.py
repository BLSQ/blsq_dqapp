import math
from datetime import date
from dateutil.relativedelta import relativedelta
from dateutil.parser import parse as parseDate
from abc import ABC, abstractmethod


def last_day_of_month(any_day):
    next_month = any_day.replace(
        day=28) + relativedelta(days=4)  # this will never fail
    return next_month - relativedelta(days=next_month.day)


class DateRange():
    def __init__(self, start, end):
        self.start = start
        self.end = end


class ExtractPeriod:
    def call(self, date_range):
        array = []
        current_date = self.first_date(date_range)
        while True:
            array.append(self.dhis2_format(current_date))
            current_date = self.next_date(current_date)
            if(current_date > date_range.end):
                break

        return array

    def quarter_start(self, current_date):
        quarterMonth = (current_date.month - 1) // 3 * 3 + 1
        return date(current_date.year, quarterMonth, 1)

    @abstractmethod
    def first_date(self, date_range):
        pass

    @abstractmethod
    def dhis2_format(self, date_range):
        pass

    @abstractmethod
    def next_date(self, current_date):
        pass


class ExtractDailyPeriod(ExtractPeriod):
    def next_date(self, date):
        return date + relativedelta(days=1)

    def dhis2_format(self, date):
        return date.strftime("%Y%m%d")

    def first_date(self, date_range):
        return date_range.start



class ExtractWeeklyPeriod(ExtractPeriod):
    def next_date(self, date):
        return date + relativedelta(days=7)

    def dhis2_format(self, date):
        year=date.isocalendar()[0]
        week=date.isocalendar()[1]
        return date.strftime(f"{year}W{week}")

    def first_date(self, date_range):
        return date_range.start

class ExtractMonthlyPeriod(ExtractPeriod):
    def next_date(self, date):
        return date + relativedelta(months=1)

    def dhis2_format(self, date):
        return date.strftime("%Y%m")

    def first_date(self, date_range):
        return date_range.start.replace(day=1)


class ExtractQuarterlyPeriod(ExtractPeriod):
    def next_date(self, current_date):
        quarterStart = self.quarter_start(current_date)
        nextDate = quarterStart + relativedelta(months=3)
        return nextDate

    def dhis2_format(self, current_date):
        return current_date.strftime("%Y") + "Q" + str(math.ceil((current_date.month / 3.0)))

    def first_date(self, date_range):
        return self.quarter_start(date_range.start)
    
    
class ExtractSixMonthlyPeriod(ExtractPeriod):
    def next_date(self, date):
        return date + relativedelta(months=6)

    def dhis2_format(self, date):
        semester_number=(date.month-1//6)+1
        return date.strftime("%Y")+f"S{semester_number}"

    def first_date(self, date_range):
        return date_range.start.replace(day=1)

class ExtractSixMonthlyAprilPeriod(ExtractPeriod):
    def next_date(self, date):
        return date + relativedelta(months=6)

    def dhis2_format(self, date):
        semester_number=(date.month-1//10)+1
        return date.strftime("%Y")+f"AprilS{semester_number}"

    def first_date(self, date_range):
        return date_range.start.replace(day=1)


class ExtractYearlyPeriod(ExtractPeriod):
    def next_date(self, date):
        return date.replace(year=date.year + 1, month=1, day=1)

    def dhis2_format(self, date):
        return date.strftime("%Y")

    def first_date(self, date_range):
        return date_range.start.replace(month=1, day=1)


class ExtractFinancialJulyPeriod(ExtractPeriod):
    def next_date(self, date):
        return date.replace(year=date.year + 1)

    def dhis2_format(self, date):
        return date.strftime("%YJuly")

    def first_date(self, date_range):
        anniv_date = date_range.start.replace(month=1, day=1) + \
            relativedelta(months=6)
        final_date = None
        if date_range.start < anniv_date:
            final_date = anniv_date - relativedelta(years=1)
        else:
            final_date = anniv_date

        return final_date
    
class ExtractFinancialOctoberPeriod(ExtractPeriod):
    def next_date(self, date):
        return date.replace(year=date.year + 1)

    def dhis2_format(self, date):
        return date.strftime("%YOct")

    def first_date(self, date_range):
        anniv_date = date_range.start.replace(month=1, day=1) + \
            relativedelta(months=10)
        final_date = None
        if date_range.start < anniv_date:
            final_date = anniv_date - relativedelta(years=1)
        else:
            final_date = anniv_date

        return final_date


CLASSES_MAPPING = {
    "daily":ExtractDailyPeriod,
    "weekly": ExtractWeeklyPeriod,
    "monthly": ExtractMonthlyPeriod,
    "quarterly": ExtractQuarterlyPeriod,
    "sixmonthly":ExtractSixMonthlyPeriod,
    "sixmonthly_april":ExtractSixMonthlyAprilPeriod,
    "yearly": ExtractYearlyPeriod,
    "financial_july": ExtractFinancialJulyPeriod,
    "financial_october": ExtractFinancialOctoberPeriod
}


class YearParser:
    @staticmethod
    def parse(period):
        if (len(period) != 4):
            return
        year = int(period[0:4])
        start_date = date(year=year, month=1, day=1)
        end_date = start_date.replace(month=12, day=31)

        return DateRange(start_date, end_date)


class YearQuarterParser:
    @staticmethod
    def parse(period):
        if "Q" not in period:
            return
        components = period.split("Q")
        quarter = int(components[1])
        year = int(components[0])
        month_start = (3 * (quarter - 1)) + 1
        month_end = month_start+2
        start_date = date(year=year, month=month_start, day=1)
        end_date = date(year=year, month=month_end, day=1) + \
            relativedelta(day=31)

        return DateRange(start_date, end_date)


class YearMonthParser:
    @staticmethod
    def parse(period):
        if len(period) != 6:
            return
        year = int(period[0:4])
        month = int(period[4:6])
        start_date = date(year=year, month=month, day=1)
        end_date = start_date + relativedelta(day=31)

        return DateRange(start_date, end_date)
    
class YearWeekParser:
    @staticmethod
    def parse(period):
        if "W" not in period:
            return
        components = period.split("W")
        week = int(components[1])
        year = int(components[0])
        start_date = date.fromisocalendar(year=year,week=week, day=1)
        end_date = date.fromisocalendar(year=year,week=week, day=7)

        return DateRange(start_date, end_date)

class YearDayParser:
    @staticmethod
    def parse(period):
        if "W" in period or len(period) != 8 :
            return
        year = int(period[:4])
        month=int(period[4:6])
        day=int(period[6:])
        start_date = date(year=year, month=month, day=day)
        end_date = date(year=year, month=month, day=day)

        return DateRange(start_date, end_date)


class FinancialJulyParser:
    @staticmethod
    def parse(period):
        if "July" not in period :
            return
        year = int(period[0:4])
        month = 7
        start_date = date(year=year, month=month, day=1)
        end_date = last_day_of_month(start_date - relativedelta(days=1)) + \
            relativedelta(years=1)

        return DateRange(start_date, end_date)
    
class FinancialOctoberParser:
    @staticmethod
    def parse(period):
        if "Oct" not in period :
            return
        year = int(period[0:4])
        month = 10
        start_date = date(year=year, month=month, day=1)
        end_date = last_day_of_month(start_date - relativedelta(days=1)) + \
            relativedelta(years=1)

        return DateRange(start_date, end_date)
    

PARSERS = [YearParser, YearQuarterParser,YearWeekParser,YearDayParser,
           YearMonthParser, FinancialJulyParser,FinancialOctoberParser]

CACHE = {}


class Periods:
    @staticmethod
    def split(period, frequency):        
        date_start = Periods.as_date_range(period[0]).start
        date_end = Periods.as_date_range(period[1]).end
        date_range=DateRange(date_start, date_end)
        mapper = CLASSES_MAPPING[frequency]
        periods = tuple(mapper().call(date_range))
        return periods


    @staticmethod
    def as_date_range(period):
        dateRange = None
        for parser in PARSERS:
            dateRange = parser.parse(period)
            if dateRange:
                break
        return dateRange