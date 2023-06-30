from django.urls import path, include
from .views import IdentificacaoAPIView, index, AntropAPIView, ident, login
from dashboard.datasets.datasets import getDatasetsToCSV


urlpatterns = [
    path('', index, name='index'),
    path('login/', login, name='login'),
    path('ident/', ident, name='ident'),
    path('antrop/', AntropAPIView.as_view(), name='antrop')
]