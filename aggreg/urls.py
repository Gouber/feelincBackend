from django.urls import path

from . import views

urlpatterns = [
    path('', views.index, name='index'),
    path('liveClear/', views.liveClear, name='liveClear'),
    path('live/', views.live, name='live')
]