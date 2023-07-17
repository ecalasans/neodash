##################################################################################
#  Imports

from dash import dcc, html
from dash.dependencies import Input, Output
from django_plotly_dash import DjangoDash
from django.conf import settings
import plotly.graph_objs as go
import pandas as pd

from dashboard.graphs import handle_dataset_async

import datetime as dt
import os
import asyncio

import locale

locale.setlocale(category=locale.LC_TIME, locale='pt_BR')

# Aquisição dos dados
dataset = asyncio.run(handle_dataset_async.startTempDFs())


df_ident = asyncio.run(handle_dataset_async.getIdent(dataset))
df_parto = asyncio.run(handle_dataset_async.getParto(dataset))


#################################################################################
# NASCIMENTOS POR ANO
# Manipulação de dados
join_dfs = pd.merge(df_ident, df_parto, left_index=True, right_index=True)
join_dfs = join_dfs.dropna(subset=['data_nasc'])
join_dfs = join_dfs.sort_values(by=['data_nasc'])
join_dfs.loc[:, 'nome_mes'] = join_dfs['data_nasc'].dt.strftime('%B')

anos = join_dfs['data_nasc'].dt.year.unique()
meses_num = join_dfs['data_nasc'].dt.month.sort_values().unique()

por_ano = join_dfs.groupby(join_dfs['data_nasc'].dt.year)['data_nasc'].count()

por_ano_mes = join_dfs.groupby([join_dfs['data_nasc'].dt.year,
                                join_dfs['data_nasc'].dt.month,
                                join_dfs['nome_mes']])['data_nasc'].count()
# Confecção da figura
data_app1 = go.Bar(
    x=por_ano.index,
    y=por_ano.values
)

layout_app1 = go.Layout(
    title='Nascimentos por Ano',
    autosize=True,
    xaxis={
        'dtick': 1
    },
)

fig_app1 = go.Figure(data=data_app1, layout=layout_app1)

# Confecção da aplicação
app1 = DjangoDash(name='NascAno')

app1.layout = html.Div(
    [
        dcc.Graph(
            id='nasc_ano_bar',
            figure=fig_app1,
        )
    ]
)
#################################################################################
# NASCIMENTOS POR MÊS EM DETERMINADO ANO
# Confecção da figura
year_options = []

for year in anos:
    year_options.append(
        {'label': str(year), 'value': year}
    )

fig_app2 = go.Figure()

# Confecção da aplicação
app2 = DjangoDash(name='NascAnoMes')

app2.layout = html.Div(
    children=[
        dcc.Dropdown(
            id='year_picker_app2',
            options=year_options,
            value=year_options[0]['label']
        ),
        dcc.Graph(
            id='nasc_ano_mes_bar',
            figure=fig_app2
        )
    ]
)


@app2.callback(
    Output(component_id='nasc_ano_mes_bar', component_property='figure'),
    [
        Input(component_id='year_picker_app2', component_property='value')
    ]
)
def updateFigure(selected_year):
    # Seleciona por ano
    filtered_by_year = join_dfs[join_dfs['data_nasc'].dt.year == selected_year]

    # Conta e agrupa por mês
    grouped_by_month = filtered_by_year.groupby([filtered_by_year['data_nasc'].dt.month,
                                                 filtered_by_year['nome_mes']])['data_nasc'].count()

    data = go.Bar(
        x=grouped_by_month.index.droplevel(0),
        y=grouped_by_month.values
    )

    layout = go.Layout(
        title="Nascimentos por mês no ano de {}".format(str(selected_year))
    )

    fig_app2 = go.Figure(data=data, layout=layout)

    return fig_app2
