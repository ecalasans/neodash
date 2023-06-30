import os
from dashboard.graphs import handle_dataset
import pandas as pd


def getDatasetsToCSV():
    all_datasets = handle_dataset.exportAllDatasets()

    for chave, dataset in all_datasets.items():
        str_file = "{}.csv".format(chave)
        path = '../neodash/dashboard/datasets/'

        if os.path.exists("{}{}".format(path, str_file)):
            print("{} será atualizado!".format(str_file))
            os.remove("{}{}".format(path, str_file))
            dataset.to_csv("{}{}".format(path, str_file))
            print("{} criado com sucesso!".format(str_file))
        else:
            dataset.to_csv("{}{}".format(path, str_file))
            print("{} criado com sucesso!".format(str_file))



