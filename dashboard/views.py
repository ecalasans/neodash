import re

from django.shortcuts import render, redirect
from django.contrib.auth import login, logout, authenticate
from django.contrib.auth.decorators import login_required
from django.conf import settings
from django.http import HttpResponse

from .graphs import handle_dataset_async
import asyncio
import inspect

from rest_framework.views import APIView
from rest_framework.response import Response

from .serializers import DataSetSerializer

def asyncronizeIt(function, *args):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    result = loop.run_until_complete(function)
    loop.close()
    return result


def configDataFrames(dataset):
    funcoes = asyncio.run(handle_dataset_async.extractGetFunctions(script=handle_dataset_async))

    for f in funcoes:
        chave = str.upper(re.split(pattern=r"get", string=f.__name__)[1])

        settings.DATASETS['chave'] = asyncio.run(f(dataset))

        print('Configurando DATASETS[{}]...'.format(chave))



#######################################################################################################################
# LOGIN
#######################################################################################################################
def sysLogin(request):
    if request.method == 'GET':
        return render(request, 'dashboard/registration/login.html')
    elif request.method == 'POST':
        user = authenticate(request, username=request.POST['user_login'], password=request.POST['user_password'])

        if user is not None:
            login(request, user)
            print('Logado')
            dataset = asyncio.run(handle_dataset_async.startTempDFs())
            configDataFrames(dataset=dataset)
            return redirect('index')
        else:
            return HttpResponse('Usuário inexistente!')

def sysLogout(request):
    logout(request)
    return redirect('login')

#######################################################################################################################
# HOME
#######################################################################################################################

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
    return render(
        request,
        'dashboard/index.html',
        context={
            'usuario': request.user.first_name
        }
    )

#######################################################################################################################
# GRÁFICOS
#######################################################################################################################
def ident(request):
    return render(request, 'dashboard/ident.html')


#######################################################################################################################
# APIs
#######################################################################################################################
class IdentificacaoAPIView(APIView):

    def get(self, request):
        # ident = handle_dataset.getIdent()
        # serializer = DataSetSerializer(instance=ident)
        # serialized = serializer.to_representation(instance=ident)
        #
        # return Response(serialized)
        pass


class AntropAPIView(APIView):

    def get(self, request):
        # antrop = handle_dataset.getAntrop()
        # serializer = DataSetSerializer(instance=antrop)
        # serialized = serializer.to_representation(instance=antrop)
        #
        # return Response(serialized)
        pass
