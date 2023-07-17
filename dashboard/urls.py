from django.urls import path, include
from .views import IdentificacaoAPIView, index, AntropAPIView, ident, sysLogin, sysLogout
from .graphs import ident_graphs



urlpatterns = [
    path('', index, name='index'),
    path('login/', sysLogin, name='login'),
    path('logout/', sysLogout, name='logout'),
    path('ident/', ident, name='ident'),
]