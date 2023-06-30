from django.urls import path, include
from .views import IdentificacaoAPIView, index, AntropAPIView, ident
from dashboard.datasets.datasets import getDatasetsToCSV


urlpatterns = [
    path('', index, name='index'),
    path('ident/', ident, name='ident'),
    path('antrop/', AntropAPIView.as_view(), name='antrop')
]