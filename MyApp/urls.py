from django.conf.urls import url

from . import views
from . import background_services as bs


urlpatterns = [
    # ... patterns ...
]

# CRON JOBS
urlpatterns += [
    url(
        r'^generate-sitemap/$',
        bs.generate_sitemap
    ),
    url(
        r'^push-gbq/$',
        bs.push_bigquery
    ),
]
