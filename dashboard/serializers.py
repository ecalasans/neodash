from rest_framework import serializers
from . import handle_dataset


class IdentSerializer(serializers.Serializer):
    def to_representation(self, instance):
        data = instance.to_dict(orient='records')
        return data
