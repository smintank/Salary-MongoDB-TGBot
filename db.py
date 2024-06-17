from datetime import datetime, timedelta

from pymongo import MongoClient

from config import MONGO_URI, DB_NAME, COLLECTION_NAME


def get_period(group_type: str) -> tuple:
    period = {
        "year": {'$year': '$dt'},
        "month": {'$month': '$dt'}
    }
    period_id = {
        "year": '$_id.year',
        "month": '$_id.month'
    }

    if group_type == "month":
        return period, period_id

    period["day"] = {'$dayOfMonth': '$dt'}
    period_id["day"] = '$_id.day'

    if group_type == "day":
        return period, period_id

    period["hour"] = {'$hour': '$dt'}
    period_id["hour"] = '$_id.hour'

    if group_type == "hour":
        return period, period_id

    raise ValueError("Invalid group_type")


def get_connection():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    return collection


def get_next_period_date(current_date, period):
    if period == "month":
        days_in_month = {
            1: 31, 2: 28, 3: 31, 4: 30, 5: 31, 6: 30,
            7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31
        }
        if current_date.month == 2 and current_date.year % 4 == 0:
            return current_date + timedelta(days=29)
        return current_date + timedelta(days=days_in_month[current_date.month])
    elif period == "day":
        return current_date + timedelta(days=1)
    return current_date + timedelta(hours=1)


async def get_documents(query: dict) -> dict:
    collection = get_connection()
    dt_from: datetime = datetime.fromisoformat(query["dt_from"])
    dt_upto: datetime = datetime.fromisoformat(query["dt_upto"])
    group_type: str = query.get("group_type")

    period, period_id = get_period(group_type)

    pipeline = [
        {
            '$match': {
                'dt': {
                    '$gte': dt_from,
                    '$lte': dt_upto
                }
            }
        },
        {
            '$group': {
                '_id': period,
                'dataset': {'$sum': '$value'}
            }
        },
        {
            '$project': {
                '_id': 0,
                'labels': period_id,
                'dataset': 1
            }
        },
        {
            '$sort': {'labels': 1}
        },
        {
            '$group': {
                '_id': None,
                'dataset': {'$push': '$dataset'},
                'labels': {'$push': '$labels'},
            }
        },
        {
            '$project': {
                '_id': 0,
                'dataset': 1,
                'labels': 1,
            }
        }
    ]

    documents = collection.aggregate(pipeline)
    documents = list(documents)[0]

    labels = []
    dataset = []
    index = 0
    current_dt = dt_from
    while True:
        if current_dt > dt_upto:
            break
        elif current_dt == dt_upto:
            labels.append(datetime.strftime(current_dt, '%Y-%m-%dT%H:00:00'))
            dataset.append(0)
            current_dt = get_next_period_date(current_dt, group_type)
        else:
            label = documents['labels'][index]
            document_dt = datetime(year=label['year'], month=label['month'],
                                   day=label.get('day', 1),
                                   hour=label.get('hour', 0))
            if current_dt == document_dt:
                labels.append(
                    datetime.strftime(current_dt, '%Y-%m-%dT%H:00:00'))
                dataset.append(documents['dataset'][index])
                current_dt = get_next_period_date(current_dt, group_type)
                index += 1
            else:
                labels.append(
                    datetime.strftime(current_dt, '%Y-%m-%dT%H:00:00'))
                dataset.append(0)
                current_dt = get_next_period_date(current_dt, group_type)

    documents['labels'] = labels
    documents['dataset'] = dataset

    return documents


if __name__ == "__main__":
    print(get_documents({
        "dt_from": "2022-09-01T00:00:00",
        "dt_upto": "2022-12-31T23:59:00",
        "group_type": "month"
    }))
