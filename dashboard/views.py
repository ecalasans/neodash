from django.shortcuts import render
from django.http import HttpResponse
from django.http import request
from . import handle_dataset

from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status

from .serializers import IdentSerializer


# Create your views here.

def index(request):
    return HttpResponse('Funcionando')


class IdentificacaoAPIView(APIView):

    def get(self, request):
        ident = handle_dataset.getIdent()
        serializer = IdentSerializer(instance=ident)
        serialized = serializer.to_representation(instance=ident)

        return Response(serialized)
