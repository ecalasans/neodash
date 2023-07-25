import pandas as pd
from rest_framework import serializers


class DataSetSerializer(serializers.Serializer):

    def to_representation(self, instance):
        if isinstance(self.instance, pd.DataFrame):
            self.instance = self.instance.fillna(0)
            serialized_data = self.instance.to_dict(orient='records')
            return serialized_data
        return super().to_representation(self.instance)