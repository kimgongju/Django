from django.urls import path, include
from rest_framework import routers
from .views import *

router = routers.DefaultRouter()

urlpatterns = [
    path('', include(router.urls)),
    path('test/', test_receive_request)
    # path('download-csv', export_csv(HttpRequest), name)
]
