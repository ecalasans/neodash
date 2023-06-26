from django.urls import path, include
from .views import IdentificacaoAPIView, index, AntropAPIView, ident


urlpatterns = [
    path('', index, name='index'),
    path('ident/', ident, name='ident'),
    path('antrop/', AntropAPIView.as_view(), name='antrop')
]