from rest_framework import serializers


class DataSetSerializer(serializers.Serializer):
    def to_representation(self, instance):
        data = instance.to_dict(orient='records')
        return data

