from django.shortcuts import render
from .graphs import handle_dataset
from .graphs import ident_graphs

from rest_framework.views import APIView
from rest_framework.response import Response

from .serializers import DataSetSerializer

# Create your views here.

def index(request):
    return render(request, 'dashboard/index.html')

def ident(request):
    return render(request, 'dashboard/ident.html')


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


