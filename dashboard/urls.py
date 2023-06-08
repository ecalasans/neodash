from django.urls import path, include
from .views import IdentificacaoAPIView


urlpatterns = [
    path('ident/', IdentificacaoAPIView.as_view(), name='ident')
]