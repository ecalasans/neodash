from django.urls import path, include
from .views import IdentificacaoAPIView, index, AntropAPIView, ident, sysLogin
from dashboard.datasets.datasets import getDatasetsToCSV


urlpatterns = [
    path('', index, name='index'),
    path('login/', sysLogin, name='login'),
    path('ident/', ident, name='ident'),
]