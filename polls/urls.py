from django.conf.urls import url

from . import views

urlpatterns = [
    url(r'^dup_detector', views.dup_detector, name='dup_detector'),
]
