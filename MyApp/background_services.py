import threading

from django.db.models import Count
from django.db.models.functions import TruncDate
from django.http import HttpResponse

import pandas as pd
from common.models import MyMembersModel


def process_gbq(df):

    pd.to_datetime(df['created_date'])
    df.set_index(df['created_date'], inplace=True)

    df['count_cumsum'] = df['count'].cumsum(axis=0)
    df['label_cumsum'] = df.groupby('name')['count'].cumsum()
    df['thing_cumsum'] = (
        df
        .groupby(df.index)['profile__thing']
        .cumsum()
    )
    df['frequency_of_thing'] = (
        df['label_cumsum'] / df['thing_cumsum'] * 100
    )

    df = df[
        ['name', 'slug', 'label_cumsum', 'thing_cumsum', 'frequency_of_thing']
    ]
    df = df[pd.notnull(df['thing_cumsum'])]
    df.reset_index(inplace=True)

    df.to_gbq(
        'pixt_tags_data.pixt_tags',
        project_id='pixt-app-1',
        if_exists='replace'
    )

    df.last('3M').to_pickle('members_df.pkl')

    return df


def push_bigquery(request):
    if request.META.get('HTTP_X_APPENGINE_CRON'):
        members_qs = (
            MyMembersModel
            .objects
            .prefetch_related("profile__thing")
            .annotate(
                created_date=TruncDate('profile__thing__timestamp'),
                count=Count('profile__thing')
            )
            .order_by('created_date')
            .distinct()
        ).values('created_date', 'name', 'slug', 'count', 'profile__thing')

        members_df = pd.DataFrame(members_qs)

        t = threading.Thread(target=process_gbq, args=(members_df,))
        t.start()

        # Do other stuff ...

        t.join()

        return HttpResponse(status=200)
    else:
        return HttpResponse(status=403)
