from django.shortcuts import render
from django.contrib.auth import login, logout, authenticate
from django.contrib.auth.decorators import login_required

from .graphs import handle_dataset

from rest_framework.views import APIView
from rest_framework.response import Response

from .serializers import DataSetSerializer


#######################################################################################################################
# LOGIN
#######################################################################################################################
def login(request):
    return render(request, 'dashboard/registration/login.html')


@login_required
def index(request):
    """
    dfs = handle_dataset.exportAllDatasets()

    request.session['ident'] = dfs['ident'].to_json()
    request.session['ant_maternos'] = dfs['ant_maternos'].to_json()
    request.session['parto'] = dfs['parto'].to_json()
    request.session['antrop'] = dfs['antrop'].to_json
    request.session['admissao'] = dfs['admissao'].to_json()
    request.session['resp'] = dfs['resp'].to_json()
    request.session['card'] = dfs['card'].to_json()
    request.session['neuro'] = dfs['neuro'].to_json()
    request.session['oftalmo'] = dfs['oftalmo'].to_json()
    request.session['hemato'] = dfs['hemato'].to_json()
    request.session['renal'] = dfs['renal'].to_json()
    request.session['infecto'] = dfs['infecto'].to_json()
    request.session['atb'] = dfs['atb'].to_json()
    request.session['imuno'] = dfs['imuno'].to_json()
    request.session['metab'] = dfs['metab'].to_json()
    request.session['cir'] = dfs['cir'].to_json()
    request.session['nut'] = dfs['nut'].to_json()
    request.session['desfecho'] = dfs['desfecho'].to_json()

    """

    return render(request, 'dashboard/index.html')


def ident(request):
    return render(request, 'dashboard/ident.html')


#######################################################################################################################
# APIs
#######################################################################################################################
class IdentificacaoAPIView(APIView):

    def get(self, request):
        ident = handle_dataset.getIdent()
        serializer = DataSetSerializer(instance=ident)
        serialized = serializer.to_representation(instance=ident)

        return Response(serialized)


class AntropAPIView(APIView):

    def get(self, request):
        antrop = handle_dataset.getAntrop()
        serializer = DataSetSerializer(instance=antrop)
        serialized = serializer.to_representation(instance=antrop)

        return Response(serialized)
