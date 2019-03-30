# Using Python and Pandas With Google BigQuery.

Influen$e relies heavily on data generated through multiple micro-services and API calls, all in real time. Early on, I wanted to set up the platform to use Google BigQuery for data warehousing, allowing us to work with real-time data in smaller chunks.

<section class="uk-section uk-section-small">
	<div class="content-box uk-container uk-container-small uk-padding-small uk-width-1-2@s">
		<h4 class="uk-text-center">Shortcut this</h4>
		<h5 class="uk-text-center">
            If you know what you're doing, you can get the code from GitHub.
        </h5>
		<div class="uk-text-center">
			<a class="uk-button uk-button-large uk-button-secondary"
                href="https://github.com/MichaelLisboa/python-pandas-bigquery">
				Get it on GitHub
			</a>
		</div>
	</div>
</section>

#### Trying to make a complicated set up as easy as possible to implement.
When I first started researching how to do this, it all seemed really complicated. I was scouring the web and reading articles, pulling little bits of useful information from many different sources.

In the end, I came up with a hacked together solution that I refined down to, what I believe, is the simplest execution.

##### What we're going to do.

1. Query data in Django.
2. Model that data, using Pandas, to a workable DataFrame.
3. Push the data to Google BigQuery.
4. Create a Cron job to run nightly pushes.

While this is a real-world example, the point of this exercise is to get a basic understanding of the steps required for setting it all up. So, I'll simplify things by only using a single query.

#### Setting up Google BigQuery.

I'm going to skip setting up a Google Cloud project, assuming you've already made it this far. We'll start with getting BigQuery set up.

First, you need to go [here and enable the API in your Google Project](https://console.cloud.google.com/flows/enableapi?apiid=bigquery&_ga=2.182927334.-1396141278.1552049696&pli=1&angularJsUrl=%2Fflows%2Fenableapi%3Fapiid%3Dbigquery%26_ga%3D2.182927334.-1396141278.1552049696%26pli%3D1&authuser=1 "Google BigQuery API")

Google has a really really good walkthrough of [setting up the environment here.](https://cloud.google.com/bigquery/docs/quickstarts/quickstart-web-ui "BigQuery quick start")

Once you have the API setup in your project, an option for BigQuery should be present in your console menu. There you'll see a screen like this:

<img src="https://raw.githubusercontent.com/MichaelLisboa/python-pandas-bigquery/master/static/images/gbq-step-1.png" />

##### Create a DataSet
Clicking on the "Create Dataset" button will display this screen:

<img src="https://raw.githubusercontent.com/MichaelLisboa/python-pandas-bigquery/master/static/images/gbq-step-2.png" />

Name your DataSet, in this case I'm calling it `MyDataId`, and choose the zone closest to you. For me, I've selected Singapore. Go ahead and click the Create DataSet button to get your first DataSet set up in BigQuery.

Now that we have a DataSet, we need to add tables to it. To keep things simple, we're going to add only one table. Select your `MyDataId`, and click Create Table to add a new empty table and call it `MyDataTable`. Don't worry about other settings at the moment, an empty table that's editable as text works for our case.

<img src="https://raw.githubusercontent.com/MichaelLisboa/python-pandas-bigquery/master/static/images/gbq-step-5.png" />

Okay, we now have a DataSet with an empty table set up. We'll be able to reference our table in Python using `MyDataId.MyDataTable`.

##### Let's make our Django View.

Go to your Django app `views.py` and create a new function. I'm calling it `push_bigquery()`. We'll start with creating a queryset for "MyMembers":
```
from django.db.models import Count
from django.db.models.functions import TruncDate
from django.http import HttpResponse

import pandas as pd
from common.models import MyMembersModel


def push_bigquery(request):
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

```
What we've done here is query our `MyMembers` table and related `Things`, as `values()`, which will return a `dict`-like queryset.

Sidenote, I'm also truncating the timstamp using `TruncDate`, which casts the expression to a date rather than using the built-in SQL `truncate` method.

Then we create our Pandas DataFrame from the values with `members_df = pd.DataFrame(members_qs)`

##### Basic data modeling.

We're going to do some simple modeling to format our DataFrame. Again, we're keeping this simple, I'm assuming you're already knowledgable about formatting data with Pandas.

Add the following to your function:
```
pd.to_datetime(df['created_date'])
members_df.set_index(df['created_date'], inplace=True)

members_df['count_cumsum'] = members_df['count'].cumsum(axis=0)

members_df['label_cumsum'] = members_df.groupby('name')['count'].cumsum()

members_df['thing_cumsum'] = (
    members_df
    .groupby(df.index)['profile__thing']
    .cumsum()
)

members_df['frequency_of_thing'] = (
    members_df['label_cumsum'] / members_df['thing_cumsum'] * 100
)
```
I've started by converting the `created_date` series to datetime and set it as the index.

Then we're getting the cumulative sum of the number of "things", and cumulative sum of "labels" associated with "things". And finally the cumulative sum of "things" by each "member".

Yeah, I know that sounds really confusing...

We're doing this because we're looking to get the frequency of things `members_df['frequency_of_thing']` by individual members and comparing that to the whole community.

Next we want to format our DataFrame by selecting the Series' we want, filtering out any NaN values and resetting the index:
```
members_df = members_df[
    ['name', 'slug', 'label_cumsum', 'thing_cumsum', 'frequency_of_thing']
]
members_df = members_df[pd.notnull(df['thing_cumsum'])]
members_df.reset_index(inplace=True)
```
Okay, our `members_df` is where we want it so we can push it to BigQuery. You'll find this frustratingly simple:

```
members_df.to_gbq(
    'MyDataId.MyDataTable',
    project_id='your-gcp-project',
    if_exists='replace'
)
```
That's it.

We're using Pandas `to_gbq` to send our DataFrame to BigQuery.

- `'MyDataId.MyDataTable'` references the DataSet and table we created earlier.
- `project_id` is obviously the ID of your Google Cloud project.
- `if_exists` is set to replace the content of the BigQuery table if the table already exists.

##### About if_exists.

In this case, if the table already exists in BigQuery, we're replacing all of the data. You don't want to do that in the real world.

`if_exists` has a couple of other arguments,

- `fail`, which raises an exception if you try to write to the table, and
- `append` which is our preferred option in this case, which will append your data to the existing table.

##### One other thing

In my case, not only do I want to push all my data to BigQuery, I also wanted a subset of that data for fast lookups across different services. To do this I pickeled the last 3 months of my dataset with:

```
members_df.last('3M').to_pickle('members_df.pkl')
```
Our final function looks like this:
```
from django.db.models import Count
from django.db.models.functions import TruncDate
from django.http import HttpResponse

import pandas as pd
from common.models import MyMembersModel


def push_bigquery(request):
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

    pd.to_datetime(df['created_date'])
    members_df.set_index(df['created_date'], inplace=True)

    members_df['count_cumsum'] = members_df['count'].cumsum(axis=0)

    members_df['label_cumsum'] = members_df.groupby('name')['count'].cumsum()

    members_df['thing_cumsum'] = (
        members_df
        .groupby(df.index)['profile__thing']
        .cumsum()
    )

    members_df['frequency_of_thing'] = (
        members_df['label_cumsum'] / members_df['thing_cumsum'] * 100
    )

    members_df = members_df[
        ['name', 'slug', 'label_cumsum', 'thing_cumsum', 'frequency_of_thing']
    ]
    members_df = members_df[pd.notnull(df['thing_cumsum'])]
    members_df.reset_index(inplace=True)

    members_df.to_gbq(
        'MyDataId.MyDataTable',
        project_id='your-gcp-project',
        if_exists='replace'
    )

    members_df.last('3M').to_pickle('members_df.pkl')

    return HttpResponse(status=200)
```

We could do a bit more optimization with Pandas and Threading, but this is good enough for the purpose of this article.

##### Google Cloud App Engine &amp; Cron jobs
We're going to set up a Cron job on Google App Engine to run our BigQuery program nightly.

Let's create a url pattern for our `push_bigquery` view and add it to the app urls.py:

```
# app/urls.py

from django.conf.urls import url

from . import views

urlpatterns = [

    ... other url patterns ...

    url(
        r'^push-gbq/$',
        views.push_bigquery
    ),
]
```
Now you should be able to visit http://localhost:8000/push-gbq/ and the function will run, cerating the DataFrame, pushing it to BigQuery, and pickeling your DataFrame.

To make sure it works, take a look at BigQuery in your Google Cloud Console. You should see your table update with your data. Also, you should see a new `.pkl` file called "members_df.pkl" in your local project root directory.

Next, in your project root directory, create a file called `cron.yaml` and add this:

```
# root/cron.yaml

cron:
- description: "Push GBQ CRON"
  url: /push-gbq/
  schedule: every 24 hours
  retry_parameters:
    min_backoff_seconds: 120
    max_doublings: 5
```
This creates a Cron job for App Engine that will visit your push_bigquery function at `www.your-website/push-gbq/` every 24 hours to push your latest data to BigQuery. Depending on your situation, you'll probably want to change the frequency this runs in your `cron.yaml` file.

Push the Cron job to App Engine with this terminal command:

`gcloud app deploy cron.yaml`

##### Security

Make sure you set up your function to only accept requests with the `x-appengine-cron` header!

Now you can deploy your project `gcloud app deploy` and test your Cron job by clicking the "Run now" button on your Cron page in Google Console.
