import pandas as pd
import numpy as np
import datetime as dt
import time

citgis_types = {'data': str, 'lin_sg_linha': str, 'sub_lin_sg_linha': str, 'vei_nro_veiculo_gestor': str, 'par_cod_siu': str, 'horario_passagem': str, 'cod_viagem': str, 'inicio_viagem': str, 'sentido_itinerario': str}
dinheiro_types = {'NOME_OPERADORA': str, 'CODIGO_VEICULO': str, 'CODIGO_LINHA': str, 'SUB_LINHA': str, 'PC': str, 'CARTAO_USUARIO': str, 'TIPO_CARTAO': str, 'DATAHORA_UTILIZACAO': str, 'VALOR_COBRADO': str}
citop_types = {'NOME_OPERADORA': str, 'CODIGO_VEICULO': str, 'CODIGO_LINHA': str, 'CARTAO_USUARIO': str, 'TIPO_CARTAO': str, 'DATAHORA_UTILIZACAO': str, 'VALOR_COBRADO': str}

t0 = time.time()
print(time.ctime(t0))

manha = pd.read_csv("manha.csv",delimiter=";",dtype=citgis_types)
tarde = pd.read_csv("tarde.csv",delimiter=";",dtype=citgis_types)
dinheiro = pd.read_csv("dinheiro.csv",delimiter=";",dtype=dinheiro_types)
dinheiro = dinheiro.iloc[: , :-1] #remove ultima coluna
citop = pd.read_csv("0305220859.csv",delimiter=";", encoding='latin-1',dtype=citop_types)
citop = citop.iloc[: , :-1] #remove ultima coluna
sublinha_ped = pd.read_csv("SUBLINHAS_vs_PED_26042022.csv",delimiter=";", encoding='latin-1',dtype=str)
sublinha_ped = sublinha_ped[['Linha',' Sublinha',' PC',' Tipo Ponto',' Código SIU',' Coord. X',' Coord. Y']]
sublinha_ped.columns = ['CODIGO_LINHA', 'SUB_LINHA', 'PC', 'TIPO_PONTO', 'EMBARQUE', 'X_EMBARQUE', 'Y_EMBARQUE']
sublinha_ped['SENTIDO'] = ''
sublinha_ped['X_EMBARQUE'] = pd.to_numeric(sublinha_ped['X_EMBARQUE'],downcast='float')
sublinha_ped['Y_EMBARQUE'] = pd.to_numeric(sublinha_ped['Y_EMBARQUE'],downcast='float')

for j in range(0,len(sublinha_ped)):
    if j == 0:
        sublinha_ped.at[j,'SENTIDO'] = '1'
    elif sublinha_ped.at[j,'TIPO_PONTO'] == 'PED':
       sublinha_ped.at[j,'SENTIDO'] = sublinha_ped.at[j-1,'SENTIDO']
    elif sublinha_ped.at[j,'TIPO_PONTO'] == 'NOT':
        sublinha_ped.at[j,'SENTIDO'] = sublinha_ped.at[j-1,'SENTIDO']
    elif sublinha_ped.at[j,'TIPO_PONTO'] == 'PR':
        sublinha_ped.at[j,'SENTIDO'] = '2' 
    elif (sublinha_ped.at[j,'TIPO_PONTO'] == 'PC') & (sublinha_ped.at[j,'PC'] == '1'):
        sublinha_ped.at[j,'SENTIDO'] = '1'
    elif (sublinha_ped.at[j,'TIPO_PONTO'] == 'PC') & (sublinha_ped.at[j,'PC'] == '2'):
        sublinha_ped.at[j,'SENTIDO'] = '2'

sublinha_ped['SEQ_EMBARQUE'] = sublinha_ped.groupby(['CODIGO_LINHA','SUB_LINHA','PC']).cumcount() + 1
sublinha_ped['SEQ_EMBARQUE'] = sublinha_ped['SEQ_EMBARQUE'].astype(str)

citgis = pd.concat([manha,tarde])
citgis[['_', 'sub_lin_sg_linha']] = citgis['sub_lin_sg_linha'].str.split('-', 1, expand=True)
citgis[['_', 'inicio_viagem']] = citgis['inicio_viagem'].str.split(' ', 1, expand=True)
citgis = citgis.drop(columns='_')
citgis.columns = ['DATA', 'CODIGO_LINHA', 'SUB_LINHA', 'CODIGO_VEICULO', 'CODIGO_SIU', 'HORA', 'CODIGO_VIAGEM', 'INICIO_VIAGEM', 'SENTIDO_ITINERARIO']
citgis.dropna(subset=['CODIGO_SIU'], inplace=True)
citgis["HORA"] = pd.to_datetime(citgis["HORA"])
citgis["INICIO_VIAGEM"] = pd.to_datetime(citgis["INICIO_VIAGEM"])
citgis['SUB_LINHA'] = citgis['SUB_LINHA'].str.replace('0','')
citgis.reset_index(drop=True,inplace=True)

siu = citgis['CODIGO_SIU'].unique()
sublinha_ped = sublinha_ped[sublinha_ped['EMBARQUE'].isin(siu)]
sublinha_ped.dropna(subset=['EMBARQUE'], inplace=True)
sublinha_ped.reset_index(drop=True,inplace=True)

unicos = citgis['CODIGO_VEICULO'].unique()
citop = citop[citop['CODIGO_VEICULO'].isin(unicos)]
dinheiro = dinheiro[dinheiro['CODIGO_VEICULO'].isin(unicos)]

citop = citop[(citop["TIPO_CARTAO"] != "DEFICIENTE") & (citop["TIPO_CARTAO"] != "IDOSO") & (citop["TIPO_CARTAO"] != "DESEMB. TRAS/DIANT COM ACOMP.") & (citop["TIPO_CARTAO"] != "DESEMB. TRAS/DIANT SEM ACOMP.") & (citop["TIPO_CARTAO"] != "GRATUIDADE SEM CARTAO") & (citop["TIPO_CARTAO"] != "OPERACIONAL BHTRANS")]
citop = citop.sort_values(by=["CODIGO_VEICULO","DATAHORA_UTILIZACAO"])
citop.reset_index(drop=True,inplace=True)

### 9204
citop_9204 = citop[citop['CODIGO_LINHA'] == '9204']
cartao_9204 = citop_9204[citop_9204['CARTAO_USUARIO'] != 'DINHEIRO']['CARTAO_USUARIO'].unique()
citop = citop[citop['CARTAO_USUARIO'].isin(cartao_9204)]
citop.reset_index(drop=True,inplace=True)
###

i = 1
for j in range(1,len(citop)):
    if (citop.at[j,'CARTAO_USUARIO'] == citop.at[j-1,'CARTAO_USUARIO']) & (citop.at[j,'CODIGO_VEICULO'] == citop.at[j-1,'CODIGO_VEICULO']):
        i += 1
        aux = citop.at[j-1,'CARTAO_USUARIO']
        citop.at[j,'CARTAO_USUARIO'] = f'{aux}-{i}'
    elif citop.at[j,'CARTAO_USUARIO'] != citop.at[j-1,'CARTAO_USUARIO']:
        i = 1

citop[['DATA', 'HORA']] = citop['DATAHORA_UTILIZACAO'].str.split(' ', 1, expand=True)
citop[['FAIXA_HORARIA', 'MINUTO']] = citop['HORA'].str.split(':', 1, expand=True) #separa horas e minutos em duas colunas
citop = citop.drop(columns=['DATAHORA_UTILIZACAO','NOME_OPERADORA','VALOR_COBRADO', 'MINUTO'])
citop["HORA"] = pd.to_datetime(citop["HORA"])
citop["EMBARQUE"] = ''
citop["DESEMBARQUE"] = ''
citop["SUB_LINHA"] = ''
citop["PC"] = ''
citop.reset_index(drop=True,inplace=True)

citop = citop.sort_values(by=["DATA","HORA","CODIGO_LINHA"])
citop.reset_index(drop=True,inplace=True)
citgis = citgis.sort_values(by=["DATA","HORA","CODIGO_LINHA"])
citgis.reset_index(drop=True,inplace=True)

### EMBARQUE

for j in range(0,len(citop)):
    aux = citgis[(citgis['DATA'] == citop.at[j,'DATA']) & (citgis['CODIGO_VEICULO'] == citop.at[j,'CODIGO_VEICULO']) & (citgis['CODIGO_LINHA'] == citop.at[j,'CODIGO_LINHA'])].copy()
    index = aux['HORA'].searchsorted(citop.at[j,'HORA'])
    if aux.empty:
        citop.at[j,'EMBARQUE'] = 'Não identificado'
    elif (index == 0):
        if(aux.iloc[index]['INICIO_VIAGEM'] < citop.at[j,'HORA']) & (aux[aux['CODIGO_VIAGEM'] == aux.iloc[index]['CODIGO_VIAGEM']].iloc[0]['HORA'] > citop.at[j,'HORA']):
            if citop.at[j,'SUB_LINHA'] == '':
                citop.at[j,'SUB_LINHA'] = aux.iloc[index]['SUB_LINHA']
            if citop.at[j,'PC'] == '':
                citop.at[j,'PC'] = aux.iloc[index]['SENTIDO_ITINERARIO'].split('PC')[1]
            citop.at[j,'EMBARQUE'] = sublinha_ped[(sublinha_ped['CODIGO_LINHA'] ==  citop.at[j,'CODIGO_LINHA']) & (sublinha_ped['SUB_LINHA'] == citop.at[j,'SUB_LINHA']) & (sublinha_ped['PC'] == citop.at[j,'PC'])].iloc[0]['EMBARQUE']
        else:
            citop.at[j,'EMBARQUE'] = 'Não identificado'
    elif aux.iloc[index-1]['HORA'] - citop.at[j,'HORA'] > dt.timedelta(minutes=10):
        citop.at[j,'EMBARQUE'] = 'Não identificado > 10'
    else:
        if citop.at[j,'SUB_LINHA'] == '':
            citop.at[j,'SUB_LINHA'] = aux.iloc[index-1]['SUB_LINHA']
        if citop.at[j,'PC'] == '':
            citop.at[j,'PC'] = aux.iloc[index-1]['SENTIDO_ITINERARIO'].split('PC')[1]
        citop.at[j,'EMBARQUE'] = aux.iloc[index-1]['CODIGO_SIU']

xy_embarque = sublinha_ped[['CODIGO_LINHA','SUB_LINHA','PC','SEQ_EMBARQUE','EMBARQUE', 'X_EMBARQUE', 'Y_EMBARQUE', 'SENTIDO']].copy()
citop = pd.merge(citop, xy_embarque, how='left', on=['CODIGO_LINHA','SUB_LINHA','PC','EMBARQUE'])
citop['SEQ_DESEMBARQUE'] = ''
citop['X_DESEMBARQUE'] = ''
citop['Y_DESEMBARQUE'] = ''
citop_9204 = citop[citop['CODIGO_LINHA'] == '9204']
citop_9204.reset_index(drop=True,inplace=True)

### DESEMBARQUE

for j in range(0,len(citop_9204)):
    if pd.notna(citop_9204.at[j,'X_EMBARQUE']) & (citop_9204.at[j,'CARTAO_USUARIO'].find('-') == -1):
        aux = citop[(citop['DATA'] == citop_9204.at[j,'DATA']) & (citop['CARTAO_USUARIO'] == citop_9204.at[j,'CARTAO_USUARIO']) & pd.notna(citop['X_EMBARQUE'])].copy()
        temp = sublinha_ped[(sublinha_ped['SENTIDO'] == citop_9204.at[j,'SENTIDO']) & (sublinha_ped['CODIGO_LINHA'] == citop_9204.at[j,'CODIGO_LINHA']) & (sublinha_ped['SUB_LINHA'] == citop_9204.at[j,'SUB_LINHA'])].copy()
        if len(aux[aux['HORA'] > citop_9204.at[j,'HORA']]) == 0 | temp.empty:
            citop_9204.at[j,'DESEMBARQUE'] = 'Não identificado'
        else:
            temp['DISTANCIA'] = np.sqrt((aux[aux['HORA'] > citop_9204.at[j,'HORA']].iloc[0]['X_EMBARQUE'] - temp['X_EMBARQUE'])**2 + (aux[aux['HORA'] > citop_9204.at[j,'HORA']].iloc[0]['Y_EMBARQUE'] - temp['Y_EMBARQUE'])**2)
            index = temp['DISTANCIA'].idxmin()
            if temp.at[index,'DISTANCIA'] <= 600:
                citop_9204.at[j,'DESEMBARQUE'] = temp.at[index,'EMBARQUE']
                citop_9204.at[j,'SEQ_DESEMBARQUE'] = temp.at[index,'SEQ_EMBARQUE']
                citop_9204.at[j,'X_DESEMBARQUE'] = temp.at[index,'X_EMBARQUE']
                citop_9204.at[j,'Y_DESEMBARQUE'] = temp.at[index,'Y_EMBARQUE']
            else:
                citop_9204.at[j,'DESEMBARQUE'] = 'Não identificado > 600'
    else:
        citop_9204.at[j,'DESEMBARQUE'] = 'Não identificado'


### EXPANSAO
qtu = citop_9204.groupby(['CODIGO_LINHA','SUB_LINHA','PC','FAIXA_HORARIA'], as_index=False)['CARTAO_USUARIO'].count()
qtu.rename(columns = {'CARTAO_USUARIO':'TOTAL'}, inplace=True)
qtuo = citop_9204.loc[pd.notna(citop_9204['X_EMBARQUE'])].groupby(['CODIGO_LINHA','SUB_LINHA','PC','FAIXA_HORARIA','SEQ_EMBARQUE','EMBARQUE'], as_index=False)['X_EMBARQUE'].count()
qtuo.rename(columns = {'X_EMBARQUE':'QUANTIDADE'}, inplace=True)
qtuod = citop_9204.loc[(citop_9204['X_DESEMBARQUE'] != '')].groupby(['CODIGO_LINHA','SUB_LINHA','PC','FAIXA_HORARIA','SEQ_EMBARQUE','EMBARQUE'], as_index=False)['X_DESEMBARQUE'].count()
qtuod.rename(columns = {'X_EMBARQUE':'QUANTIDADE'}, inplace=True)

print(qtu)
print(qtuo)
print(qtuod)

citop_9204 = citop_9204[['DATA','HORA','FAIXA_HORARIA', 'CODIGO_VEICULO','CODIGO_LINHA','SUB_LINHA','PC','SENTIDO','CARTAO_USUARIO','TIPO_CARTAO','SEQ_EMBARQUE','EMBARQUE','X_EMBARQUE','Y_EMBARQUE','SEQ_DESEMBARQUE','DESEMBARQUE','X_DESEMBARQUE','Y_DESEMBARQUE']]
citop_9204.to_csv('9204.csv', sep = ';', index=False, encoding='utf-8-sig')
qtu.to_csv('qtu.csv', sep = ';', index=False, encoding='utf-8-sig')
qtuo.to_csv('qtuo.csv', sep = ';', index=False, encoding='utf-8-sig')
qtuod.to_csv('qtud.csv', sep = ';', index=False, encoding='utf-8-sig')
print(citop_9204[pd.notna(citop_9204['X_EMBARQUE'])])
print(f'Done. ({time.time() - t0:.3f}s)')
