import re
import asyncio
import aiohttp
import inspect
import json

from asgiref.sync import sync_to_async, async_to_sync

from django.shortcuts import render, redirect
from django.contrib.auth import login, logout, authenticate
from django.contrib.auth.decorators import login_required
from django.conf import settings
from django.http import HttpResponse

from .graphs import handle_dataset_async

from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.generics import ListAPIView
from rest_framework import viewsets

from .serializers import DataSetSerializer

dataset = asyncio.run(handle_dataset_async.startTempDFs())


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

        settings.DATASETS["{}".format(chave)] = asyncio.run(f(dataset))

        print('Configurando DATASETS[{}]...'.format(chave))
        print(type(settings.DATASETS[chave]))

    settings.DATASETS_FULL = True

def setSessionVariable(request, key, value):
    request.session[key] = value
    request.session.save()



#######################################################################################################################
# LOGIN
#######################################################################################################################
async def sysLogin(request):
    if request.method == 'GET':
        return render(request, 'dashboard/registration/login.html')
    elif request.method == 'POST':
        data = {
            'username': request.POST.get('user_login'),
            'password': request.POST.get('user_password')

        }
        result = None
        async with aiohttp.ClientSession() as session:
            async with session.post('http://localhost:8000/api/authentication/', data=data) as r:
                if r:
                    r_dump = json.dumps(await r.json())

                    result = json.loads(r_dump)

                    await sync_to_async(setSessionVariable)(request, 'first_name', result['first_name'])
                    await sync_to_async(setSessionVariable)(request, 'last_name', result['last_name'])

                    return redirect('index')

                else:
                    print('Deu bosta')
                    return HttpResponse('Deu bosta')






def sysLogout(request):
    logout(request)
    return redirect('login')


#######################################################################################################################
# HOME
#######################################################################################################################


def index(request):
    if settings.DATASETS_FULL == False:
        configDataFrames(dataset)

    return render(
        request,
        'dashboard/index.html',
        context={
            'usuario': "{} {}".format(request.session.get('first_name'), request.session.get('last_name'))
        }
    )


#######################################################################################################################
# GRÁFICOS
#######################################################################################################################
def ident(request):
    return render(
        request,
        'dashboard/ident.html',
        context={
            'usuario': "{} {}".format(request.session.get('first_name'), request.session.get('last_name'))
        }
    )


#######################################################################################################################
# APIs
#######################################################################################################################
class IdentificacaoAPIView(APIView):

    def get(self, request):
        if settings.DATASETS['IDENT'] != '':
            ident = settings.DATASETS['IDENT']
        else:
            ident = asyncio.run(handle_dataset_async.getIdent(dataset))

        serializer = DataSetSerializer(instance=ident, many=True)
        serialized = serializer.to_representation(ident)

        return Response(serialized)


class AntropAPIView(ListAPIView):
    queryset = settings.DATASETS['ANTROP']
    serializer_class = DataSetSerializer

    def get_queryset(self):
        if settings.DATASETS['ANTROP'] != '':
            return settings.DATASETS['ANTROP']
        else:
            return asyncio.run(handle_dataset_async.getAntrop(dataset))


class GenericAPIView(ListAPIView):
    queryset = None
    serializer_class = DataSetSerializer

    def get_queryset(self):
        param = self.kwargs.get('dataset')

        if settings.DATASETS[str.upper(param)] != '':
            return settings.DATASETS[param]
        else:
            # Captura as funções
            funcoes = asyncio.run(handle_dataset_async.extractGetFunctions(script=handle_dataset_async))

            # Seleciona e executa a função que bate com o endpoint
            nome_funcao = "get{}".format(str.capitalize(param))

            funcao = [f for f in funcoes if f.__name__ == nome_funcao][0]

            funcao_sign = inspect.signature(funcao)

            arguments = {
                'dataset': dataset
            }

            funcao_bind = funcao_sign.bind(**arguments)
            funcao_bind.apply_defaults()

            # Retorna o resultado
            result = asyncio.run(funcao(**arguments))

            return result




