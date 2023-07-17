import numpy as np
import pandas as pd
import requests
from dotenv import load_dotenv
import os
import re
import json
import asyncio
import aiohttp
import time
import inspect

load_dotenv('config.env')

temp_dfs = None

data = {
        'token': os.getenv('TOKEN_REDCAPRN'),
        'content': 'record',
        'format': 'json',
        'type': 'flat',
        'csvDelimiter': '',
        'rawOrLabel': 'raw',
        'rawOrLabelHeaders': 'raw',
        'exportCheckboxLabel': 'false',
        'exportSurveyFields': 'false',
        'exportDataAccessGroups': 'false',
        'returnFormat': 'json'
}

# Converte em dias ou horas
def convHorasDia(campo, unid = 24.0):
    valor = np.float16(re.match('\d+\s*', campo)[0])
    return np.round(valor/unid, 1)

# Converte semanas em dias
def paraDias(campo):
        if(re.match('[Tt]', campo) or campo == ''):
            return np.random.randint(low=259, high=295, size=1)[0]
        elif re.match('\d+\s*[Hh]', campo):
            return np.round(np.float16((re.match('\d+\s*')[0])/7), 1)
        elif re.match('\d+\s*[Dd]', campo):
            return np.round(np.float16(re.match('\d+\s*', campo)[0]))
        else:
            temp = re.split('m', campo)

            if (len(temp) == 1 or temp[1] == '' or re.match('\D', temp[1])):
                semanas = str.strip(temp[0])
                semanas = np.int8(re.match('\d+', semanas)[0])
                return semanas * 7
                
            else:
                semanas = str.strip(temp[0])
                dias = str.strip(temp[1])

                semanas = np.int8(re.match('\d+', semanas)[0])
                dias = np.int8(re.match('\d', dias)[0])

                return (semanas * 7) + dias
  

async def hitAPI():
    print('Iniciando chamada na API...')
    start = time.time()
    async with aiohttp.ClientSession() as session:
        async with session.post('https://neo.redcaprn.org/api/',data=data) as r:
            if r:
                r_dump = json.dumps(await r.json())
                end = time.time()
                total = end - start
                #print('Terminou withAioHTTP!')
                #print("aiohttp demorou {} segundos!".format(total))
                df = pd.read_json(r_dump, dtype=True, convert_dates=True)
                #print(df.info())
                return df
            else:
                end = time.time()
                #print('Terminou withAioHTTP!')
                total = end - start
                #print("Demorou {} segundos e não tinha nada!".format(total))
                return None
                

#  Extrai os dataframes
async def extractDFs():
    dataset = await hitAPI()

    ident = dataset[['nome_pac', 'nascido_hsc', 'setor_origem_pac', 'idade_materna',
                'cor_pele', 'proc_materna', 'escolaridade_materna', 'trabalho_mae',
                'gesta', 'abortos', 'filhos_vivos', 'natimortos', 'mortos_menor_1_ano',
                'ts_mae', 'cons_prenatal']]

    antmaternos = dataset[['fumo', 'alcool', 'freq_alcool', 'psicoativas', 'violencia',
                        'corioamnionite', 'rcui', 'tpp', 'dpp', 'has', 'dheg', 'hellp',
                        'sulfatada', 'oligoamnio', 'dm', 'hiv', 'hep_b', 'hep_c', 'sifilis',
                        'toxoplasmose', 'cmv', 'rubeola', 'covid_mae',
                        'outras_doencas_maternas', 'cort_antenatal']]

    parto = dataset[['data_nasc', 'sexo', 'tempo_br', 'tipo_parto', 'gemelar',
                'apgar_1_minuto', 'apgar_5_min', 'reanimacao', 'o2_21',
                'massagem_cardiaca', 'cpap_sp', 'drogas_reanimacao', 'liq_amniotico',
                'clamp_cu', 'ev_hipotermia', 'saco_plastico', 'touca', 'colchao']]

    antrop = dataset[['peso_nasc', 'estatura', 'pc', 'ig_nasc', 'ts_rn']]

    admissao = dataset[['data_adm', 'indic_adm', 'id_adm', 'peso_adm', 'est_adm', 'pc_adm',
                'temp_adm','hora_temp_adm']]

    resp = dataset[['apneia', 'sdr', 'ttrn', 'sam', 'hpprn', 'enfisema', 'hem_pulm',
                'pneu_congenita', 'pneu_adquirida', 'dbp', 'o2circ', 'hood', 'cpap',
                'vni', 'vm', 'ox_nitrico', 'tempo_o2', 'o2_28', 'o2_36', 'num_intub',
                'falha_extub', 'surfactante', 'dose_surf', 'hv_surf', 'extub_acid']]

    card = dataset[['pca', 'med_pca', 'med_util_pca', 'cc', 'cir_cardiaca', 'cvu', 'cva',
                'picc', 'dias_dissec', 'pun_ven_prof', 'dva', 'eco']]

    neuro = dataset[['usg_tf', 'outros_usgtf', 'hidrocefalia', 'leucomalacia', 'convulsao',
                'anticonvulsivante', 'eeg', 'eeg_alterado']]

    oftalmo = dataset[['rop', 'grau_rop', 'rop_plus', 'zona_avascular',
                    'cirurgia_rop', 'refl_vermelho']]

    hemato = dataset[['incomp_sang', 'ictericia', 'max_bt', 'fototerapia', 'dias_foto',
                    'est','anemia', 'menor_ht', 'conc_hemacias', 'plaquetopenia',
                    'conc_plaquetas', 'dhrn', 'hem_digestiva', 'tipo_hem_digestiva',
                    'vit_k', 'plasma', 'policitemia', 'max_ht']]

    renal = dataset[['ins_renal', 'max_ureia', 'max_creatinina']]

    infecto = dataset[['sepse_precoce', 'hmc_admissao', 'hmc_adm_res', 'sepse_tardia',
                    'tipo_inf_sepse_tardia___1', 'tipo_inf_sepse_tardia___2',
                    'hmc_sepse_tardia', 'meningite', 'cultura_lcr', 'res_cultura_lcr',
                    'ecn', 'cirurgia_ecn', 'covid_rn', 'conjuntivite', 'onfalite',
                    'sifilis_congenita', 'inf_cateter', 'infec_congenitas']]

    atb = dataset[['ampicilina', 'genta', 'cefepime', 'amica', 'meropenem', 'vanco',
                'pen_cristalina', 'anfo', 'fluco', 'oxa', 'dias_pen_cris',
                'dias_ampicilina', 'dias_genta', 'dias_cefepime', 'dias_amica',
                'dias_oxacilina', 'dias_meropenem', 'dias_vanco', 'dias_antifung',
                'outros_atb', 'imunoglob']]

    imuno = dataset[['v_hep_b', 'v_bcg', 'palivizumabe', 'imunoglobulina']]

    metab = dataset[['dist_glicose', 'min_glicose', 'max_glicose', 'dist_sodio', 'min_na',
                'max_na', 'dist_k', 'min_k', 'max_k', 'dist_calcio', 'min_ca', 'max_ca',
                'dist_mag', 'min_mg', 'max_mg', 'npt_padrao', 'npt', 'dv_prim_npt',
                'dias_npt', 'max_vig', 'max_aac', 'max_lip', 'col_pezinho',
                'pezinho_alterado', 'tp_alterado', 'maior_p', 'menor_p', 'maior_fa',
                'menor_fa', 'dmopt']]

    cir = dataset[['pntx', 'pneumoperit', 'pnmed', 'cir_abd', 'retinopexia', 'hernia_ing',
                'dvp_dve', 'malf_cong', 'tipo_anom_cong']]

    nut = dataset[['min_peso', 'id_peso_min', 'max_peso', 'id_pn', 'peso_28',
                'id_dieta_enteral', 'lme_lmo', 'fm85', 'colostroterapia', 'formula_art',
                'transicao_sonda_sm', 'idade_smld']]

    desfecho = dataset[['desfecho', 'loc_obito', 'causa_obito', 'data_alta', 'readmissao',
                    'motivo_readm', 'id_desfecho', 'igc_desfecho', 'peso_desfecho',
                    'est_desfecho', 'pc_desfecho', 'peleapele', 'tvpele_a_pele',
                    'orient_pos_canguru', 'idade_pos_canguru', 'reg_alim_alta']]

    return  {
        'ident': ident,
        'antmaternos': antmaternos,
        'parto': parto,
        'antrop': antrop,
        'admissao': admissao,
        'resp': resp,
        'card': card,
        'neuro': neuro,
        'oftalmo': oftalmo,
        'hemato': hemato,
        'renal': renal,
        'infecto': infecto,
        'atb': atb,
        'imuno': imuno,
        'metab': metab,
        'cir': cir,
        'nut': nut,
        'desfecho': desfecho
    }

#  Configura cache de dataframes
async def startTempDFs():
    global temp_dfs
    
    if temp_dfs is None:
        temp_dfs = await extractDFs()
        print('Inicializando extração de datasets...')
        return temp_dfs
    else:
        return temp_dfs

# Nomes dos dataframes
async def extractDFNames(datasets):
   return datasets.keys()

# Dataframe específico
async def extractDF(datasets, nome_df=''):
    if datasets is None:
        print('Chamando função...')
    else:
        print('Utilizando cache...')

    try: 
        print('Pesquisando {}'.format(nome_df))
        df_result = datasets[nome_df]
        print('Retornando {}'.format(df_result))
        return df_result
    except:
        print('Deu ruiim')
        

async def getIdent(dataset):
    ident = dataset['ident']

    # Manipulação de dados vazios
    # Nascidos no HSC
    ident['nascido_hsc'] = ident['nascido_hsc'].replace(
        ['', 1, 2], [np.nan, 'Sim', 'Não'])
    ident['nascido_hsc'] = ident['nascido_hsc'].astype(str)
    

    # Setor de origem do paciente
    ident['setor_origem_pac'] = ident['setor_origem_pac'].replace(
        ['','1','2', '3','4', '5'], ['Sem registro', 'CO', 'CC', 'AC', 'UCINCA', 'Outros']
        )
    

    # Cor da pele
    ident['cor_pele'] = ident['cor_pele'].replace(
        ['1', '2', '3','4', '5', '6', ''], ['Branca', 'Negra', 'Parda', 'Amarela', 'Indígena', 'Outra', 'Sem registro']
    )

    # Idade materna
    ident['idade_materna'] = ident['idade_materna'].replace('', np.nan)
    ident['idade_materna'] = pd.to_numeric(ident['idade_materna'])
    ident['idade_materna'] = ident['idade_materna'].fillna(np.round(ident['idade_materna'].mean(), 0))

    # Procedência materna
    ident['proc_materna'] = ident['proc_materna'].replace(
        ['', '1', '2', '3', '4'], ['Descohecida', 'Natal', 'Grande Natal', 'Interior', 'Desconhecida']
    )

    # Escolaridade materna
    ident['escolaridade_materna'] = ident['escolaridade_materna'].replace(
        ['', '1', '2', '3', '4', '5', '6', '7', '8'],
        ['Desconhecida', 'Fund. incompleto', 'Fund. completo', 'Médio incompleto',
         'Médio completo', 'Superior incompleto', 'Superior completo', 'Pós Graduação', 'Desconhecida']
    )

    # Trabalho
    ident['trabalho_mae'] = ident['trabalho_mae'].replace(
        ['1', '2', '3', ''], ['Sim', 'Não', 'Desconhecido', 'Desconhecido']
    )

    # Número de gestações
    ident['gesta'] = ident['gesta'].replace(['01', '02', '03', '04', '05', '06', '07', '08', '09', ''],
                                            ['1', '2', '3', '4', '5', '6', '7', '8', '9', np.nan])
    ident['gesta'] = pd.to_numeric(ident['gesta'])
    ident['gesta'] = ident['gesta'].fillna(np.round(ident['gesta'].mean(), 0))


    # Mortos com menos de 1 ano
    ident['mortos_menor_1_ano'] = ident['mortos_menor_1_ano'].replace(
        ['1', '2', ''], ['Sim', 'Não', 'Sem registro']
    )

    # Número de abortos
    ident['abortos'] = ident['abortos'].replace(['', '00', '01'], [np.nan, '0', '1'])
    ident['abortos'] = pd.to_numeric(ident['abortos'])
    ident['abortos'] = ident['abortos'].fillna(0)

    # Tipo sanguíneo da mãe
    ident['ts_mae'] = ident['ts_mae'].replace(
        ['1', '2', '3', '4', '5', '6', '7', '8', '9', ''],
        ['A+', 'A-', 'B+', 'B-', 'AB+', 'AB-', 'O+', 'O-', 'Sem registro', 'Sem registro']
    )

    # Consultas de Pré-Natal
    ident['cons_prenatal'] = ident['cons_prenatal'].replace(
        ['', '05', '06', '07', '08', '99'],
        [np.nan, '5', '6', '7', '8', np.nan]
    )
    ident['cons_prenatal'] = pd.to_numeric(ident['cons_prenatal'])
    ident['cons_prenatal'] = ident['cons_prenatal'].fillna(np.round(ident['cons_prenatal'].median(), 0))

    # Filhos vivos
    ident['filhos_vivos'] = ident['filhos_vivos'].replace(['01', '02', '03', '05', ''],
                                                          ['1', '2', '3', '5', np.nan])
    ident['filhos_vivos'] = pd.to_numeric(ident['filhos_vivos'])
    ident['filhos_vivos'] = ident['filhos_vivos'].fillna(np.random.choice([0,1], 1)[0])

    # Nascidos mortos
    ident['natimortos'] = ident['natimortos'].replace(['00', '01', ''],
                                                      ['0', '1', np.nan])
    ident['natimortos'] = pd.to_numeric(ident['natimortos'])
    ident['natimortos'] = ident['natimortos'].fillna(0)

    return ident  


async def getAntMaternos(dataset):
    antmaternos = dataset['antmaternos']

    # Fumo
    antmaternos['fumo'] = antmaternos['fumo'].replace(
        ['1', '2', '3', '', '0'], ['Sim', 'Não', 'Desconhecido', 'Sem registro', 'Sem registro']
    )

    # Alcoolismo
    antmaternos['alcool'] = antmaternos['alcool'].replace(
        ['1', '2', '3', ''], ['Sim', 'Não', 'Desconhecido', 'Sem registro']
    )

    # Frequencia do uso de álcool
    antmaternos['freq_alcool'] = antmaternos['freq_alcool'].replace(
        ['1', '2', ''], ['Sim', 'Não', 'Sem registro']
    )

    # Uso de drogas psicoativas
    antmaternos['psicoativas'] = antmaternos['psicoativas'].replace(
        ['1', '2', '3', '', '0'], ['Sim', 'Não', 'Desconhecido', 'Sem registro', 'Sem registro']
    )

    # Violência contra gestante
    antmaternos['violencia'] = antmaternos['violencia'].replace(
        ['1', '2', '3', '', '0'], ['Sim', 'Não', 'Desconhecido', 'Sem registro', 'Sem registro']
    )

    # Corioamnionite
    antmaternos['corioamnionite'] = antmaternos['corioamnionite'].replace(
        ['1', '2', '3', '', '0'], ['Sim', 'Não', 'Desconhecido', 'Sem registro', 'Sem registro']
    )

    # Retardo do crescimento intra-uterino
    antmaternos['rcui'] = antmaternos['rcui'].replace(
        ['1', '2', '3', '', '0'], ['Sim', 'Não', 'Desconhecido', 'Sem registro', 'Sem registro']
    )

    # Trabalho de parto prematuro
    antmaternos['tpp'] = antmaternos['tpp'].replace(
        ['1', '2', '3', '', '0'], ['Sim', 'Não', 'Desconhecido', 'Sem registro', 'Sem registro']
    )

    # Descolamento prematuro da placenta
    antmaternos['dpp'] = antmaternos['dpp'].replace(
        ['1', '2', '3', '', '0'], ['Sim', 'Não', 'Desconhecido', 'Sem registro', 'Sem registro']
    )

    # Hipertensão arterial materna prévia
    antmaternos['has'] = antmaternos['has'].replace(
        ['1', '2', '3', '', '0'], ['Sim', 'Não', 'Desconhecido', 'Sem registro', 'Sem registro']
    )

    # Doença hipertensiva específica da gravidez
    antmaternos['dheg'] = antmaternos['dheg'].replace(
        ['1', '2', '3', '', '0'], ['Sim', 'Não', 'Desconhecido', 'Sem registro', 'Sem registro']
    )

    # Síndrome HELLP
    antmaternos['hellp'] = antmaternos['hellp'].replace(
        ['1', '2', '3', '', '0'], ['Sim', 'Não', 'Desconhecido', 'Sem registro', 'Sem registro']
    )

    # Uso de sulfato de magnésio para eclâmpsia/HELLP
    antmaternos['sulfatada'] = antmaternos['sulfatada'].replace(
        ['0', '1', '2', '3', ''], ['Não', 'Sim', 'Não', 'Desconhecido', 'Sem registro']
    )

    # Oligoâmnio
    antmaternos['oligoamnio'] = antmaternos['oligoamnio'].replace(
        ['1', '2', '3', '', '0'], ['Sim', 'Não', 'Desconhecido', 'Sem registro', 'Sem registro']
    )

    # Diabetes mellitus
    antmaternos['dm'] = antmaternos['dm'].replace(
        ['1', '2', '3', '4', '5', '6', '', '0'], ['Não', 'Prévio não insulinodependente',
                                                  'Prévio insulinodependente', 'Gestacional não insulinodependente',
                                                  'Gestacional insulinodependente', 'Desconhecido',
                                                  'Sem registro', 'Sem registro']
    )

    # HIV
    antmaternos['hiv'] = antmaternos['hiv'].replace(
        ['1', '2', '3', '', '0'], ['Sim', 'Não', 'Desconhecido', 'Sem registro', 'Sem registro']
    )

    # Hepatite B
    antmaternos['hep_b'] = antmaternos['hep_b'].replace(
        ['1', '2', '3', '', '0'], ['Sim', 'Não', 'Desconhecido', 'Sem registro', 'Sem registro']
    )

    # Hepatite C
    antmaternos['hep_c'] = antmaternos['hep_c'].replace(
        ['1', '2', '3', '', '0'], ['Sim', 'Não', 'Desconhecido', 'Sem registro', 'Sem registro']
    )

    # Sífilis - VDRL materno
    antmaternos['sifilis'] = antmaternos['sifilis'].replace(
        ['1', '2', '3', '', '0'], ['Sim', 'Não', 'Desconhecido', 'Sem registro', 'Sem registro']
    )

    # Toxoplasmose
    antmaternos['toxoplasmose'] = antmaternos['toxoplasmose'].replace(
        ['1', '2', '3', '', '0'], ['Sim', 'Não', 'Desconhecido', 'Sem registro', 'Sem registro']
    )

    # Citomegalovirus
    antmaternos['cmv'] = antmaternos['cmv'].replace(
        ['1', '2', '3', '', '0'], ['Sim', 'Não', 'Desconhecido', 'Sem registro', 'Sem registro']
    )

    # Rubéola
    antmaternos['rubeola'] = antmaternos['rubeola'].replace(
        ['1', '2', '3', '', '0'], ['Sim', 'Não', 'Desconhecido', 'Sem registro', 'Sem registro']
    )

    # COVID-19 materno
    antmaternos['covid_mae'] = antmaternos['covid_mae'].replace(
        ['1', '2', '3', '', '0'], ['Sim', 'Não', 'Desconhecido', 'Sem registro', 'Sem registro']
    )

    # Corticóide antenatal
    antmaternos['cort_antenatal'] = antmaternos['cort_antenatal'].replace(
        ['1', '2', '3', '', '0'], ['Sim', 'Não', 'Desconhecido', 'Sem registro', 'Sem registro']
    )

    return antmaternos

# Parto
async def getParto(dataset):
    parto = dataset['parto']

    # Data de nascimento
    parto['data_nasc'] = pd.to_datetime(parto['data_nasc'])

    # Sexo
    parto['sexo'] = parto['sexo'].replace(
        ['1', '2', '3', ''], ['Feminino', 'Masculino', 'Indefinido', 'Sem registro']
    )

    # Tempo de bolsa rota
    parto['tempo_br'] = parto['tempo_br'].replace(['99', ''], np.nan)
    parto['tempo_br'] = pd.to_numeric(parto['tempo_br'])
    parto['tempo_br'] = parto['tempo_br'].replace(np.nan, parto['tempo_br'].median())

    # Tipo de parto
    parto['tipo_parto'] = parto['tipo_parto'].replace(
        ['1', '2', '3', ''], ['Normal', 'Cesariano', 'Fórceps', 'Sem registro']
    )

    # Gemelaridade
    parto['gemelar'] = parto['gemelar'].replace(
        ['1', '2', ''], ['Sim', 'Não', 'Sem registro']
    )

    # Apgar no primeiro minuto de vida
    parto['apgar_1_minuto'] = parto['apgar_1_minuto'].replace('', np.nan)
    parto['apgar_1_minuto'] = pd.to_numeric(parto['apgar_1_minuto'])
    parto['apgar_1_minuto'] = parto['apgar_1_minuto'].replace(np.nan, parto['apgar_1_minuto'].median())

    # Apgar no quinto minuto de vida
    parto['apgar_5_min'] = parto['apgar_5_min'].replace('', np.nan)
    parto['apgar_5_min'] = pd.to_numeric(parto['apgar_5_min'])
    parto['apgar_5_min'] = parto['apgar_5_min'].replace(np.nan, parto['apgar_5_min'].median())

    # Reanimação neonatal
    parto['reanimacao'] = parto['reanimacao'].replace(
        ['1', '2', '3', '4', '5', ''], ['Não', 'O2 inalatório', 'VPP com balão e máscara', 'VPP com IOT',
                                        'Desconhecido', 'Sem registro']
    )

    # Usou O2 > 21% na Reanimação
    parto['o2_21'] = parto['o2_21'].replace(
        ['0','1', '2', '3',''], ['Não','Sim', 'Não', 'Desconhecido','Sem registro']
    )

    # Recebeu massagem cardíaca na reanimação
    parto['massagem_cardiaca'] = parto['massagem_cardiaca'].replace(
        ['0', '1', np.nan], ['Não', 'Sim', 'Sem registro']
    )

    # Recebeu CPAP em sala de parto
    parto['cpap_sp'] = parto['cpap_sp'].replace(
        ['0', '1', np.nan, ''], ['Não', 'Sim', 'Sem registro', 'Sem registro']
    )

    # Usou medicamentos na reanimação
    parto['drogas_reanimacao'] = parto['drogas_reanimacao'].replace(
        ['0','1', '2', '3',''], ['Não','Sim', 'Não', 'Desconhecido','Sem registro']
    )

    # Tipo de líquido amniótico
    parto['liq_amniotico'] = parto['liq_amniotico'].replace(
        ['1', '2', '3', '4', '5', '6', '7', ''], ['Claro', 'Meconial', 'Sanguinolento', 'Achocolatado',
                                              'Outro', 'Purulento', 'Desconhecido', 'Sem registro']
    )

    # Tempo de clampeamento do cordão umbilical
    parto['clamp_cu'] = parto['clamp_cu'].replace(
        ['1', '2', '3', ''], ['< 1min', '> 1min', 'Sem registro', 'Sem registro']
    )

    # Medidas para evitar hipotermia
    parto['ev_hipotermia'] = parto['ev_hipotermia'].replace(
        ['0','1', '2', '3',''], ['Não','Sim', 'Não', 'Desconhecido','Sem registro']
    )

    # Usou saco plástico para evitar hipotermia
    parto['saco_plastico'] = parto['saco_plastico'].replace(
        ['0','1', '2', '3',''], ['Não','Sim', 'Não', 'Desconhecido','Sem registro']
    )

    # Usou touca térmica para evitar hipotermia
    parto['touca'] = parto['touca'].replace(
        ['0','1', '2', '3',''], ['Não','Sim', 'Não', 'Desconhecido','Sem registro']
    )

    # Usou colchão térmico para evitar hipotermia
    parto['colchao'] = parto['colchao'].replace(
        ['0','1', '2', '3',''], ['Não','Sim', 'Não', 'Desconhecido','Sem registro']
    )

    return parto

# Dados antropométricos
async def getAntrop(dataset):
    antrop = dataset['antrop']

    # Peso de nascimento
    antrop['peso_nasc'] = antrop['peso_nasc'].replace('', 0)
    antrop['peso_nasc'] =pd.to_numeric(antrop['peso_nasc'])

    # Estatura
    antrop['estatura'] = antrop['estatura'].replace('', 0)
    antrop['estatura'] = pd.to_numeric(antrop['estatura'])

    # Perímetro cefálico
    antrop['pc'] = antrop['pc'].replace('', 0)
    antrop['pc'] = pd.to_numeric(antrop['pc'])

    # Idade gestacional em dias
    ig_em_dias = []
    for index, item in antrop.iterrows():
        ig_em_dias.append(paraDias(item['ig_nasc']))
    
    antrop['ig_em_dias'] = [i for i in ig_em_dias]

    #antrop.loc[antrop['ig_nasc'].isin(['Termo', 'termo', '', 'TERMO']), 'ig_em_dias'] = np.random.randint(259, 294)
    antrop['ig_em_semanas'] = [np.round(i/7, 1) for i in antrop['ig_em_dias'].values]

    antrop['ig_categ'] = pd.cut(
        x=antrop['ig_em_dias'],
        bins=[0, 196, 238, 259, 294, 500],
        labels=["RNPTE", "RNPT Moderado", "RNPT Tardio", "Termo", "Pós-termo"],
        right=False
    )

    #antrop['ig_categ'] = antrop['ig_categ'].replace('NaN', 'Termo')

    for item in antrop['ig_categ'].unique():
        antrop.loc[(antrop['peso_nasc'] == 0) | (antrop['peso_nasc']).isna() | (antrop['ig_categ'] == item),'peso_nasc'] = np.round(
            antrop.loc[(antrop['ig_categ'] == item)]['peso_nasc'].mean(), 1)
        
        antrop.loc[
            (
                ((antrop['estatura'] == 0) | (antrop['estatura'].isna())) & 
                (antrop['ig_categ'] == item)
            ),'estatura'] = np.round(antrop.loc[antrop['ig_categ'] == item]['estatura'].mean(), 0)
        
        antrop.loc[
            (
                ((antrop['pc'] == 0) | (antrop['pc'].isna())) & 
                (antrop['ig_categ'] == item)
            ),'pc'] = np.round(antrop.loc[antrop['ig_categ'] == item]['pc'].mean(), 0)

    # Tipo sanguíneo
    antrop['ts_rn'] = antrop['ts_rn'].replace(
        ['1', '2', '3', '4', '5', '6', '7', '8', '9', ''],
        ['A+', 'A-', 'B+', 'B-', 'AB+', 'AB-', 'O+', 'O-', 'Sem registro', 'Sem registro']
    )
    
    return antrop

# Dados da Admissão
async def getAdm(dataset):
    adm = dataset['admissao']

    # Horário da admissão
    adm['id_adm_h'] = 0.0

    # Horas(hv, hmin, horas, etc.)
    for index, item in adm.iterrows():
        if re.match('\d+\s*[Hh]', item['id_adm']):
            temp = re.split('[Hh]', item['id_adm'])

            if temp[1] == '' or re.match('[A-Za-z]', temp[1]):
                horas = np.float16(temp[0])
                adm.loc[index, 'id_adm_h'] = horas
            else:
                horas = np.float16(temp[0])
                try:
                    minutos = np.float16(re.match('\d+', temp[1])[0])
                except TypeError:
                    minutos = 0
                adm.loc[index, 'id_adm_h'] = np.round(horas + (minutos / 60), 1)
        elif re.match('\d+\s*[Mm]', item['id_adm']):
            if re.match('\dm', item['id_adm']):
                continue
            else:
                minutos = re.match('\d+', item['id_adm'])[0]
                adm.loc[index, 'id_adm_h'] = np.round(np.float16(minutos)/60, 1)
        elif re.match('\d+\s*[dD]', item['id_adm']):
            temp = re.split('[dD]', item['id_adm'])

            if re.match('\d+[a-z]', temp[0]):
                continue
            else:
                adm.loc[index, 'id_adm_h'] = np.float16(temp[0]) * 24

    # Peso na admissão
    adm['peso_adm'] = adm['peso_adm'].replace('', np.nan)
    adm['peso_adm'] = pd.to_numeric(adm['peso_adm'])

    # Estatura na admissão
    adm['est_adm'] = adm['est_adm'].replace('', np.nan)
    adm['est_adm'] = pd.to_numeric(adm['est_adm'])

    # Perímetro cefálico na admissão
    adm['pc_adm'] = adm['pc_adm'].replace(['', '0'], np.nan)
    adm['pc_adm'] = pd.to_numeric(adm['pc_adm'])

    # Temperatura na admissão
    adm['temp_adm'] = adm['temp_adm'].replace(['-35', ''], ['35', np.nan])
    adm['temp_adm'] = pd.to_numeric(adm['temp_adm'])

    # Hora de verificação da temperatura de admissão
    adm['hora_temp_adm'] = adm['hora_temp_adm'].replace('', np.nan)

    # Data da admissão
    adm['data_adm_conv'] = pd.to_datetime(adm['data_adm'])

    return adm


# Intercorrências respiratórias
async def getResp(dataset):
    resp = dataset['resp']

    # Apneia
    resp['apneia'] = resp['apneia'].replace('', np.nan)

    # Sindrome do Desconforto Respiratório
    resp['sdr'] = resp['sdr'].replace('', np.nan)

    # Síndrome de aspiração meconial
    resp['sam'] = resp['sam'].replace('', np.nan)

    # Hipertensão pulmonar
    resp['hpprn'] = resp['hpprn'].replace('', np.nan)

    # Enfisema intersticial
    resp['enfisema'] = resp['enfisema'].replace('', np.nan)

    # Hemorragia pulmonar
    resp['hem_pulm'] = resp['hem_pulm'].replace('', np.nan)

    # Pneumonia congênita(intra-útero)
    resp['pneu_congenita'] = resp['pneu_congenita'].replace('', np.nan)

    # Pneumonia adquirida
    resp['pneu_adquirida'] = resp['pneu_adquirida'].replace('', np.nan)

    # Displasia broncopulmonar
    resp['dbp'] = resp['dbp'].replace('', np.nan)

    # Uso de O2 circulante(dias)
    resp['o2circ'] = resp['o2circ'].replace('', np.nan)
    resp['o2circ'] = pd.to_numeric(resp['o2circ'])

    # Uso de capacete com oxigênio(dias)
    resp['hood'] = resp['hood'].replace('', np.nan)
    resp['hood'] = pd.to_numeric(resp['hood'])

    # Uso de cpap(dias)
    resp['cpap'] = resp['cpap'].replace('', np.nan)
    resp['cpap'] = pd.to_numeric(resp['cpap'])

    # Uso de ventilação não-invasiva(VNI)(dias)
    resp['vni'] = resp['vni'].replace('', np.nan)
    resp['vni'] = pd.to_numeric(resp['vni'])

    # Uso de ventilação mecânica
    resp['vm'] = resp['vm'].replace('', np.nan)
    resp['vm'] = pd.to_numeric(resp['vm'])

    # Uso de óxido nítrico
    resp['ox_nitrico'] = resp['ox_nitrico'].replace('', np.nan)

    # Tempo total de uso de oxigênio(dias)
    resp['tempo_o2'] = resp['tempo_o2'].replace('', np.nan)

    # Oxigenoterapia no 28° dia de vida
    resp['o2_28'] = resp['o2_28'].replace('', np.nan)

    # Oxigenoterapia com 36sem de idade corrigida
    resp['o2_36'] = resp['o2_36'].replace('', np.nan)

    # Número de intubações
    resp['num_intub'] = resp['num_intub'].replace('', np.nan)
    resp['num_intub'] = pd.to_numeric(resp['num_intub'])

    # Falha na extubação
    resp['falha_extub'] = resp['falha_extub'].replace('', np.nan)

    # Uso de surfactante
    resp['surfactante'] = resp['surfactante'].replace('', np.nan)

    # Número de doses de surfactante utilizadas
    resp['dose_surf'] = resp['dose_surf'].replace('', np.nan)
    resp['dose_surf'] = pd.to_numeric(resp['dose_surf'])

    # Horas de vida na primeira dose de surfactante
    resp['hv_surf'] = resp['hv_surf'].replace('', np.nan)
    resp['hv_surf'] = pd.to_numeric(resp['hv_surf'])

    return resp

#  Intercorrências cardiológicas
async def getCard(dataset):
    card = dataset['card']

    # Persistência do canal arterial
    card['pca'] = card['pca'].replace('', np.nan)

    # Fechamento de canal com medicamento
    card['med_pca'] = card['med_pca'].replace('', np.nan)

    # Medicamento utilizado no tratamento do canal arterial
    card['med_util_pca'] = card['med_util_pca'].replace('', np.nan)

    # Cardiopatia congênita
    card['cc'] = card['cc'].replace('', np.nan)

    # Correção cirúrgica de cardiopatia congênita
    card['cir_cardiaca'] = card['cir_cardiaca'].replace('', np.nan)

    # Cateterismo umbilical venoso(dias)
    card['cvu'] = card['cvu'].replace('', np.nan)
    card['cvu'] = pd.to_numeric(card['cvu'])

    # Cateterismo umbilical arterial(dias)
    card['cva'] = card['cva'].replace('', np.nan)
    card['cva'] = pd.to_numeric(card['cva'])

    # Uso de PICC(dias)
    card['picc'] = card['picc'].replace(['', 'mínimo 9 dias'], [np.nan, '9'])
    card['picc'] = pd.to_numeric(card['picc'])

    # Dissecção venosa(dias)
    card['dias_dissec'] = card['dias_dissec'].replace('', np.nan)
    card['dias_dissec'] = pd.to_numeric(card['dias_dissec'])

    # Punção venosa profunda(subclávia, femural)
    card['pun_ven_prof'] = card['pun_ven_prof'].replace('', np.nan)
    card['pun_ven_prof'] = pd.to_numeric(card['pun_ven_prof'])

    # Uso de drogas vasoativas
    card['eco'] = card['eco'].replace('', np.nan)

    return card

#  Intercorrências neurológicas
async def getNeuro(dataset):
    neuro = dataset['neuro']

    # Ultrassonografia transfontanela
    neuro['usg_tf'] = neuro['usg_tf'].replace('', np.nan)

    # Hidrocefalia
    neuro['hidrocefalia'] = neuro['hidrocefalia'].replace('', np.nan)

    # Leucomalácia periventricular
    neuro['leucomalacia'] = neuro['leucomalacia'].replace('', np.nan)

    # Convulsão
    neuro['convulsao'] = neuro['convulsao'].replace('', np.nan)

    # Uso de anticonvulsivante que debelou a crise convulsiva
    neuro['anticonvulsivante'] = neuro['anticonvulsivante'].replace('', np.nan)

    # Realizou eletroencefalograma
    neuro['eeg'] = neuro['eeg'].replace('', np.nan)

    # Eletroencefalograma alterado
    neuro['eeg_alterado'] = neuro['eeg_alterado'].replace('', np.nan)

    return neuro

#  Intercorrências oftalmológicas
async def getOftalmo(dataset):
    oftalmo = dataset['oftalmo']

    # Retinopatia da prematuridade
    oftalmo['rop'] = oftalmo['rop'].replace('', np.nan)

    # Maior grau da ROP
    oftalmo['grau_rop'] = oftalmo['grau_rop'].replace('', np.nan)

    # ROP com Plus
    oftalmo['rop_plus'] = oftalmo['rop_plus'].replace('', np.nan)

    # Zona avascular
    oftalmo['zona_avascular'] = oftalmo['zona_avascular'].replace('', np.nan)

    # Fez cirurgia(retinopexia)
    oftalmo['cirurgia_rop'] = oftalmo['cirurgia_rop'].replace('', np.nan)

    # Reflexo vermelho
    oftalmo['refl_vermelho'] = oftalmo['refl_vermelho'].replace('', np.nan)

    return oftalmo

#  Intercorrências hematológicas
async def getHemato(dataset):
    hemato = dataset['hemato']

    # Incompatibilidade sanguínea
    hemato['incomp_sang'] = hemato['incomp_sang'].replace('', np.nan)

    # Desenvolveu icterícia
    hemato['ictericia'] = hemato['ictericia'].replace('', np.nan)

    # Máxima bilirrubina total
    hemato['max_bt'] = hemato['max_bt'].replace(['0', ''], np.nan)
    hemato['max_bt'] = pd.to_numeric(hemato['max_bt'])

    # Realizou fototerapia
    hemato['fototerapia'] = hemato['fototerapia'].replace('', np.nan)

    # Dias em fototerapia
    hemato['dias_foto'] = hemato['dias_foto'].replace('', np.nan)
    hemato['dias_foto'] = pd.to_numeric(hemato['dias_foto'])

    # Exsanguineotransfusão
    hemato['est'] = hemato['est'].replace('', np.nan)

    # Anemia
    hemato['anemia'] = hemato['anemia'].replace('', np.nan)

    # Menor hematócrito
    hemato['menor_ht'] = hemato['menor_ht'].replace(['0', ''], np.nan)
    hemato['menor_ht'] = pd.to_numeric(hemato['menor_ht'])

    # Concentrado de hemácias(número de transfusões)
    hemato['conc_hemacias'] = hemato['conc_hemacias'].replace('', np.nan)
    hemato['conc_hemacias'] = pd.to_numeric(hemato['conc_hemacias'])

    # Plaquetopenia
    hemato['plaquetopenia'] = hemato['plaquetopenia'].replace('', np.nan)

    # Recebeu concentrado de plaquetas
    hemato['conc_plaquetas'] = hemato['conc_plaquetas'].replace('', np.nan)

    # Doença hemorrágica do recém-nascido
    hemato['dhrn'] = hemato['dhrn'].replace('', np.nan)

    # Hemorragia digestiva
    hemato['hem_digestiva'] = hemato['hem_digestiva'].replace('', np.nan)

    # Tipo de hemorragia digestiva
    hemato['tipo_hem_digestiva'] = hemato['tipo_hem_digestiva'].replace('', np.nan)

    # Vitamina K em doses terapêuticas
    hemato['vit_k'] = hemato['vit_k'].replace('', np.nan)

    # Recebeu plasma
    hemato['plasma'] = hemato['plasma'].replace('', np.nan)

    # Policitemia
    hemato['policitemia'] = hemato['policitemia'].replace('', np.nan)

    # Máximo hematócrito
    hemato['max_ht'] = hemato['max_ht'].replace('', np.nan)
    hemato['max_ht'] = pd.to_numeric(hemato['max_ht'])

    return hemato

#  Intercorrências renais
async def getRenal(dataset):
    renal = dataset['renal']

    # Insuficiência renal
    renal['ins_renal'] = renal['ins_renal'].replace('', np.nan)

    # Máxima ureia
    renal['max_ureia'] = renal['max_ureia'].replace('', np.nan)
    renal['max_ureia'] = pd.to_numeric(renal['max_ureia'])    

    # Máxima creatinina
    renal['max_creatinina'] = [i.replace(',', '.') for i in renal['max_creatinina'].values]

    renal['max_creatinina'] = renal['max_creatinina'].replace('', np.nan)
    renal['max_creatinina'] = pd.to_numeric(renal['max_creatinina'])

    return renal

#  Intercorrências infecciosas
async def getInfecto(dataset):
    infecto = dataset['infecto']

    # Sepse neonatal precoce
    infecto['sepse_precoce'] = infecto['sepse_precoce'].replace('', np.nan)

    # Fez hemocultura na admissão
    infecto['hmc_admissao'] = infecto['hmc_admissao'].replace('', np.nan)

    # Sepse tardia
    infecto['sepse_tardia'] = infecto['sepse_tardia'].replace('', np.nan)

    # Cultura sepse tardia
    infecto['hmc_sepse_tardia'] = infecto['hmc_sepse_tardia'].replace('', np.nan)
    infecto['hmc_sepse_tardia'] = infecto['hmc_sepse_tardia'].replace('Negativa', '0')

    # Meningite
    infecto['meningite'] = infecto['meningite'].replace('', np.nan)

    # Fez cultura de líquor
    infecto['cultura_lcr'] = infecto['cultura_lcr'].replace('', np.nan)

    # Enterocolite necrosante
    infecto['ecn'] = infecto['ecn'].replace('', np.nan)

    # Fez cirurgia para enterocolite
    infecto['cirurgia_ecn'] = infecto['cirurgia_ecn'].replace('', np.nan)

    # COVID-19
    infecto['covid_rn'] = infecto['covid_rn'].replace('', np.nan)

    # Conjuntivite neonatal
    infecto['conjuntivite'] = infecto['conjuntivite'].replace('', np.nan)

    # Onfalite
    infecto['onfalite'] = infecto['onfalite'].replace('', np.nan)

    # Sífilis congênita
    infecto['sifilis_congenita'] = infecto['sifilis_congenita'].replace('', np.nan)

    # Infecção de catéter venoso
    infecto['inf_cateter'] = infecto['inf_cateter'].replace('', np.nan)

    # Infecções virais congênitas
    infecto['infec_congenitas'] = infecto['infec_congenitas'].replace('', np.nan)

    return infecto

#  Uso de antibióticos
async def getAtb(dataset):
    atb = dataset['atb']

    # Ampicilina
    atb['ampicilina'] = atb['ampicilina'].replace('', np.nan)

    # Gentamicina
    atb['genta'] = atb['genta'].replace('', np.nan)

    # Cefepime
    atb['cefepime'] = atb['cefepime'].replace('', np.nan)

    # Amicacina
    atb['amica'] = atb['amica'].replace('', np.nan)

    # Meropenem
    atb['meropenem'] = atb['meropenem'].replace('', np.nan)

    # Vancomicina
    atb['vanco'] = atb['vanco'].replace('', np.nan)

    # Penicilina cristalina
    atb['pen_cristalina'] = atb['pen_cristalina'].replace('', np.nan)

    # Anfotericina B
    atb['anfo'] = atb['anfo'].replace('', np.nan)

    # Fluconazol
    atb['fluco'] = atb['fluco'].replace('', np.nan)

    # Oxacilina
    atb['oxa'] = atb['oxa'].replace('', np.nan)

    # Outros antibióticos
    atb['outros_atb'] = atb['outros_atb'].replace('', np.nan)

    # Dias de penicilina cristalina
    atb['dias_pen_cris'] = atb['dias_pen_cris'].replace('', np.nan)
    atb['dias_pen_cris'] = pd.to_numeric(atb['dias_pen_cris'])

    # Dias de ampicilina
    atb['dias_ampicilina'] = atb['dias_ampicilina'].replace('', np.nan)
    atb['dias_ampicilina'] = pd.to_numeric(atb['dias_ampicilina'])

    # Dias de gentamicina
    atb['dias_genta'] = atb['dias_genta'].replace('', np.nan)
    atb['dias_genta'] = pd.to_numeric(atb['dias_genta'])

    # Dias de cefepime
    atb['dias_cefepime'] = atb['dias_cefepime'].replace('', np.nan)
    atb['dias_cefepime'] = pd.to_numeric(atb['dias_cefepime'])

    # Dias de amicacina
    atb['dias_amica'] = atb['dias_amica'].replace('', np.nan)
    atb['dias_amica'] = pd.to_numeric(atb['dias_amica'])

    # Dias de oxacilina
    atb['dias_oxacilina'] = atb['dias_oxacilina'].replace('', np.nan)
    atb['dias_oxacilina'] = pd.to_numeric(atb['dias_oxacilina'])

    # Dias de meropenem
    atb['dias_meropenem'] = atb['dias_meropenem'].replace('', np.nan)
    atb['dias_meropenem'] = pd.to_numeric(atb['dias_meropenem'])

    # Dias de vancomicina
    atb['dias_vanco'] = atb['dias_vanco'].replace('', np.nan)
    atb['dias_vanco'] = pd.to_numeric(atb['dias_vanco'])

    # Dias de antifúngico
    atb['dias_antifung'] = atb['dias_antifung'].replace('', np.nan)
    atb['dias_antifung'] = pd.to_numeric(atb['dias_antifung'])

    # Imunoglobulina
    atb['imunoglob'] = atb['imunoglob'].replace('', np.nan)
    atb['imunoglob'] = pd.to_numeric(atb['imunoglob'])

    return atb

# Dados vacinais
async def getImuno(dataset):
    imuno = dataset['imuno']

    # Vacina Hepatite B
    imuno['v_hep_b'] = imuno['v_hep_b'].replace('', np.nan)

    # Vacina BCG
    imuno['v_bcg'] = imuno['v_bcg'].replace('', np.nan)

    # Palivuzumabe
    imuno['palivizumabe'] = imuno['palivizumabe'].replace('', np.nan)

    return imuno

#  Intercorrências metabólicas
async def getMetab(dataset):
    metab = dataset['metab']

    # Distúrbios da glicose
    metab['dist_glicose'] = metab['dist_glicose'].replace('', np.nan)

    # Glicemia mínima
    metab['min_glicose'] = metab['min_glicose'].replace('', np.nan)
    metab['min_glicose'] = pd.to_numeric(metab['min_glicose'])

    # Glicemia máxima
    metab['max_glicose'] = metab['max_glicose'].replace('', np.nan)
    metab['max_glicose'] = pd.to_numeric(metab['max_glicose'])

    # Distúrbios do sódio
    metab['dist_sodio'] = metab['dist_sodio'].replace('', np.nan)

    # Mínimo sódio
    metab['min_na'] = metab['min_na'].replace('', np.nan)
    metab['min_na'] = pd.to_numeric(metab['min_na'])

    # Máximo sódio
    metab['max_na'] = metab['max_na'].replace('', np.nan)
    metab['max_na'] = pd.to_numeric(metab['max_na'])

    # Distúrbios do potássio
    metab['dist_k'] = metab['dist_k'].replace('', np.nan)

    # Mínimo potássio
    metab['min_k'] = metab['min_k'].replace('', np.nan)
    metab['min_k'] = pd.to_numeric(metab['min_k'])

    # Máximo potássio
    metab['max_k'] = metab['max_k'].replace('', np.nan)
    metab['max_k'] = pd.to_numeric(metab['max_k'])

    # Distúrbios do cálcio
    metab['dist_calcio'] = metab['dist_calcio'].replace('', np.nan)

    # Mínimo cálcio
    metab['min_ca'] = metab['min_ca'].replace('', np.nan)
    metab['min_ca'] = pd.to_numeric(metab['min_ca'])

    # Máximo cálcio
    metab['max_ca'] = metab['max_ca'].replace('', np.nan)
    metab['max_ca'] = pd.to_numeric(metab['max_ca'])

    # Distúrbios do magnésio
    metab['dist_mag'] = metab['dist_mag'].replace('', np.nan)

    # Mínimo magnésio
    metab['min_mg'] = metab['min_mg'].replace('', np.nan)
    metab['min_mg'] = pd.to_numeric(metab['min_mg'])

    # Máximo magnésio
    metab['max_mg'] = metab['max_mg'].replace('', np.nan)
    metab['max_mg'] = pd.to_numeric(metab['max_mg'])

    # Uso de aminoven/NPT padrão
    metab['npt_padrao'] = metab['npt_padrao'].replace('', np.nan)

    # Usou NPT
    metab['npt'] = metab['npt'].replace('', np.nan)

    # Dias de vida no primeiro dia de NPT
    metab['dv_prim_npt'] = metab['dv_prim_npt'].replace('', np.nan)

    for index, item in metab.iterrows():
        if type(metab.loc[index, 'dv_prim_npt']) is not str:
            continue
        elif re.match('\d+\s*[HhDd]', metab.loc[index, 'dv_prim_npt']):
            metab.loc[index, 'dv_prim_npt'] = convHorasDia(metab.loc[index, 'dv_prim_npt'])
    
    metab['dv_prim_npt'] = pd.to_numeric(metab['dv_prim_npt'], errors='coerce')

    # Tempo de NPT
    metab['dias_npt'] = metab['dias_npt'].replace('', np.nan)
    metab['dias_npt'] = pd.to_numeric(metab['dias_npt'])

    # Maior VIG da NPT
    metab['max_vig'] = metab['max_vig'].replace('', np.nan)
    metab['max_vig'] = pd.to_numeric(metab['max_vig'])

    # Maior AAC da NPT
    metab['max_aac'] = metab['max_aac'].replace('', np.nan)
    metab['max_aac'] = pd.to_numeric(metab['max_aac'])

    # Maior lipídio da NPT
    metab['max_lip'] = metab['max_lip'].replace('', np.nan)
    metab['max_lip'] = pd.to_numeric(metab['max_lip'])

    # Coletou teste do pezinho
    metab['col_pezinho'] = metab['col_pezinho'].replace('', np.nan)

    # Teste do pezinho alterado
    metab['pezinho_alterado'] = metab['pezinho_alterado'].replace('', np.nan)

    # Diagnóstico do teste do pezinho
    metab['tp_alterado'] = metab['tp_alterado'].replace('', np.nan)

    # Maior fósforo
    metab['maior_p'] = metab['maior_p'].replace('', np.nan)
    metab['maior_p'] = pd.to_numeric(metab['maior_p'])

    # Menor fósforo
    metab['menor_p'] = metab['menor_p'].replace('', np.nan)
    metab['menor_p'] = pd.to_numeric(metab['menor_p'])

    # Maior fosfatase alcalina
    metab['maior_fa'] = metab['maior_fa'].replace('', np.nan)
    metab['maior_fa'] = pd.to_numeric(metab['maior_fa'])

    # Menor fosfatase alcalina
    metab['menor_fa'] = metab['menor_fa'].replace('', np.nan)
    metab['menor_fa'] = pd.to_numeric(metab['menor_fa'])

    # Doença metabólica óssea
    metab['dmopt'] = metab['dmopt'].replace('', np.nan)

    return metab

#  Intercorrências cirúrgicas
async def getCir(dataset):
    cir = dataset['cir']

    # Pneumotórax
    cir['pntx'] = cir['pntx'].replace('', np.nan)

    # Pneumomediastino
    cir['pnmed'] = cir['pnmed'].replace('', np.nan)

    # Pneumoperitônio
    cir['pneumoperit'] = cir['pneumoperit'].replace('', np.nan)

    # Cirurgia abdominal
    cir['cir_abd'] = cir['cir_abd'].replace('', np.nan)

    # Retinopexia
    cir['retinopexia'] = cir['retinopexia'].replace('', np.nan)

    # Hérnia inguinal
    cir['hernia_ing'] = cir['hernia_ing'].replace('', np.nan)

    # DVP ou DVE
    cir['dvp_dve'] = cir['dvp_dve'].replace('', np.nan)

    # Malformações congênitas
    cir['malf_cong'] = cir['malf_cong'].replace('', np.nan)

    # Tipo de malformação congênita
    cir['tipo_anom_cong'] = cir['tipo_anom_cong'].replace('', np.nan)

    return cir

#  Evolução nutricional
async def getNut(dataset):
    nut = dataset['nut']

    # Peso mínimo
    nut['min_peso'] = nut['min_peso'].replace('', np.nan)
    nut['min_peso'] = pd.to_numeric(nut['min_peso'])

    # Idade do peso mínimo
    nut['id_peso_min'] = nut['id_peso_min'].replace('', np.nan)
    nut['id_peso_min'] = pd.to_numeric(nut['id_peso_min'])

    # Peso máximo
    nut['max_peso'] = nut['max_peso'].replace('', np.nan)
    nut['max_peso'] = pd.to_numeric(nut['max_peso'])

    # Idade em que recuperou peso do nascimento
    nut['id_pn'] = nut['id_pn'].replace('', np.nan)
    nut['id_pn'] = pd.to_numeric(nut['id_pn'])

    # Peso aos 28 dias
    nut['peso_28'] = nut['peso_28'].replace('', np.nan)
    nut['peso_28'] = pd.to_numeric(nut['peso_28'])

    # Idade de início da dieta enteral
    nut['id_dieta_enteral'] = nut['id_dieta_enteral'].replace('', np.nan)
    for index, item in nut.iterrows():
        if type(nut.loc[index, 'id_dieta_enteral']) is not str:
            continue
        elif re.match('\d+\s*[HhDd]', nut.loc[index, 'id_dieta_enteral']):
            nut.loc[index, 'id_dieta_enteral'] = convHorasDia(nut.loc[index, 'id_dieta_enteral'])
    
    nut['id_dieta_enteral'] = pd.to_numeric(nut['id_dieta_enteral'], errors='coerce')

    # Idade que conseguiu ficar em aleitamento materno
    nut['lme_lmo'] = nut['lme_lmo'].replace('', np.nan)
    nut['lme_lmo'] = pd.to_numeric(nut['lme_lmo'])

    # Usou FM 85
    nut['fm85'] = nut['fm85'].replace('', np.nan)

    # Colostroterapia
    nut['colostroterapia'] = nut['colostroterapia'].replace('', np.nan)

    # Usou fórmula artificial
    nut['formula_art'] = nut['formula_art'].replace('', np.nan)

    # Transição sonda para aleitamento
    nut['transicao_sonda_sm'] = nut['transicao_sonda_sm'].replace('', np.nan)
    for index, item in nut.iterrows():
        if type(nut.loc[index, 'transicao_sonda_sm']) is not str:
            continue
        elif re.match('\d+\s*[HhDd]', nut.loc[index, 'transicao_sonda_sm']):
            nut.loc[index, 'transicao_sonda_sm_num'] = convHorasDia(nut.loc[index, 'transicao_sonda_sm'])
        elif re.match('\d+\s*[Ss]', nut.loc[index, 'transicao_sonda_sm']):
            nut.loc[index, 'transicao_sonda_sm_num'] = paraDias(nut.loc[index, 'transicao_sonda_sm'])
        else:
            nut.loc[index, 'transicao_sonda_sm_num'] = paraDias(nut.loc[index, 'transicao_sonda_sm'])

    nut['transicao_sonda_sm_num'] = pd.to_numeric(nut['transicao_sonda_sm_num'])

    # Idade em que ficou em seio materno em livre demanda
    #nut['idade_smld'] = nut['idade_smld'].replace('', np.nan)
    nut['idade_smld_num'] = [paraDias(i) for i in nut['idade_smld'].values]
    nut['idade_smld_num'] = pd.to_numeric(nut['idade_smld_num'])

    return nut

#  Desfecho
async def getDesfecho(dataset):
    desfecho = dataset['desfecho']

    # Desfecho do paciente no serviço
    desfecho['desfecho'] = desfecho['desfecho'].replace(['', '1'], np.nan)

    # Local do óbito
    desfecho['loc_obito'] = desfecho['loc_obito'].replace('', np.nan)

    # Data da alta
    desfecho['data_alta'] = desfecho['data_alta'].replace('', np.nan)
    desfecho['data_alta_conv'] = pd.to_datetime(desfecho['data_alta'], dayfirst=True)

    # Causa óbito
    desfecho['causa_obito'] = desfecho['causa_obito'].replace(['', '5'], np.nan)

    # Readmissão
    desfecho['readmissao'] = desfecho['readmissao'].replace('', np.nan)

    # Motivo da readmissão
    desfecho['motivo_readm'] = desfecho['motivo_readm'].replace('', np.nan)

    # Idade do desfecho(em dias)
    desfecho['id_desfecho'] = desfecho['id_desfecho'].replace('', np.nan)
    desfecho['id_desfecho'] = pd.to_numeric(desfecho['id_desfecho'])

    # Idade corrigida no desfecho
    desfecho['igc_desfecho'] = desfecho['id_desfecho'].replace('', np.nan)
    desfecho['igc_desfecho'] = pd.to_numeric(desfecho['igc_desfecho'])

    # Peso no desfecho(em gramas)
    desfecho['peso_desfecho'] = desfecho['peso_desfecho'].replace('', np.nan)
    desfecho['peso_desfecho'] = pd.to_numeric(desfecho['peso_desfecho'])

    # Estatura no desfecho(em cm)
    desfecho['est_desfecho'] = desfecho['est_desfecho'].replace('', np.nan)
    desfecho['est_desfecho'] = pd.to_numeric(desfecho['est_desfecho'])

    # Perímetro cefálico no desfecho(em cm)
    desfecho['pc_desfecho'] = desfecho['pc_desfecho'].replace('', np.nan)
    desfecho['pc_desfecho'] = pd.to_numeric(desfecho['pc_desfecho'])

    # Fez contato pele a pele na UTIN(verificar na prescrição)
    desfecho['peleapele'] = desfecho['peleapele'].replace('', np.nan)

    # Dias de vida quando fez o contato pele a pele
    desfecho['peleapele'] = desfecho['tvpele_a_pele'].replace('', np.nan)
    desfecho['peleapele'] = pd.to_numeric(desfecho['peleapele'])

    # Recebeu orientação sobre a posição Canguru na UTIN
    desfecho['orient_pos_canguru'] = desfecho['orient_pos_canguru'].replace('', np.nan)

    # Idade de início da posição Canguru
    #desfecho['idade_pos_canguru'] = desfecho['idade_pos_canguru'].replace('', np.nan)
    desfecho['idade_pos_canguru_num'] = [paraDias(i) for i in desfecho['idade_pos_canguru']]

    #desfecho['idade_pos_canguru'] = pd.to_numeric(desfecho['idade_pos_canguru'])

    # Regime alimentar na alta hospitalar
    desfecho['reg_alim_alta'] = desfecho['reg_alim_alta'].replace('', np.nan)

    # Tempo de permanência
    admissao = await getAdm(dataset)
    desfecho['tempo_permanencia'] = np.abs(np.int8((desfecho['data_alta_conv'] - admissao['data_adm_conv'])/np.timedelta64(1, 'D')))

    return desfecho

# Retorna uma lista de funções do tipo getNomeDataset
async def extractGetFunctions():
    pattern = r"get"

    # Captura todas as funções que começam com get
    funcoes = [
        obj for name, obj in inspect.getmembers(__import__(__name__))
        if inspect.iscoroutinefunction(obj) and re.search(pattern=pattern, string=name)
    ]

    return funcoes

async def extractGetFunctions(script):
    pattern = r"get"

    # Captura todas as funções que começam com get
    funcoes = [
        obj for name, obj in inspect.getmembers(script)
        if inspect.iscoroutinefunction(obj) and re.search(pattern=pattern, string=name)
    ]

    return funcoes

async def exportAllDatasets(dataset):
    df = await startTempDFs()

    funcoes = await extractGetFunctions()

    funcoes_names = [f.__name__ for f in funcoes]

    df_dict = {}

    for funcao_name in funcoes_names:
        # Referência à função a ser executada
        funcao_ref = globals()[funcao_name]

        # Assinatura da função - quais argumentos ela precisa?
        funcao_sign = inspect.signature(funcao_ref)

        # Argumento(s) da função
        arguments = {
            'dataset': df
        }

        df_name = re.split(r"get", str.lower(funcao_name))[1]

        # Passa o argumento para a função
        funcao_bind = funcao_sign.bind(**arguments)
        funcao_bind.apply_defaults()

        # Executa a função
        print('Executando a função {}()'.format(funcao_name))
        df_dict[df_name] = await funcao_ref(**arguments)

    return df_dict


async def main():
    df = await startTempDFs()

    funcoes = await extractGetFunctions()

    funcoes_names = [f.__name__ for f in funcoes]

    df_dict = {}

    for funcao_name in funcoes_names:
        # Referência à função a ser executada
        funcao_ref = globals()[funcao_name]

        # Assinatura da função - quais argumentos ela precisa?
        funcao_sign = inspect.signature(funcao_ref)

        # Argumento(s) da função
        arguments = {
            'dataset': df
        }

        df_name = re.split(r"get", str.lower(funcao_name))[1]

        # Passa o argumento para a função
        funcao_bind = funcao_sign.bind(**arguments)
        funcao_bind.apply_defaults()

        # Executa a função
        print('Executando a função {}()'.format(funcao_name))
        df_dict[df_name] = await funcao_ref(**arguments)

    print(df_dict.keys())
        

if __name__ == '__main__':
    asyncio.run(main())