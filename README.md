# Using Python and Pandas With Google BigQuery.

Influen$e relies heavily data generated through multiple micro-services and API calls, all in real time. Early on, I wanted to set up the platform to use Google BigQuery for data warehousing, and work with real-time data in smaller chunks.

#### Trying to make a complicated set up as easy as possible to implement.
When I first started researching how to do this, it all seemed really complicated. I was scouring the web and reading articles, pulling little bits of useful information from many different sources.

In the end, I came up with a hacked together solution that I refined down to, what I believe, is the simplest execution.

##### What we're going to do.

1. Query data in Django.
2. Model that data, using Pandas, to a workable DataFrame.
3. Push the data to Google BigQuery.
4. Create a Cron job to run nightly pushes.

While this is a real-world example, the point of this exercise is to get a basic understanding of the steps required for setting up this infrastructure. So, I'll simplify things by only using a single query.
