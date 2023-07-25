from django.urls import path, include
from .views import (
    IdentificacaoAPIView, AntropAPIView, GenericAPIView,
    index, ident, sysLogin, sysLogout)
from .graphs import ident_graphs



urlpatterns = [
    path('', index, name='index'),
    path('login/', sysLogin, name='login'),
    path('logout/', sysLogout, name='logout'),
    path('ident/', ident, name='ident'),
    #path('api/ident/', IdentificacaoAPIView.as_view(), name='api_ident'),
    #path('api/antrop/', AntropAPIView.as_view(), name='api_antrop'),
    path('api/<str:dataset>/', GenericAPIView.as_view(), name='api_generic')

]