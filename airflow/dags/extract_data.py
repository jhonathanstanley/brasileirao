import json
import os
import pandas as pd
import requests
from lxml import etree
from sqlalchemy import create_engine
from bs4 import BeautifulSoup as bs
from config.config import *

#configurando conexões de banco e API.
url = str(BASE_URL) + 'campeonatos/803/classificacao'
token = TOKEN
engine = create_engine('postgresql://postgres:jhou0911@localhost/postgres')

#realizando requisição na API e salvando resultado em um dataframe pandas
try:
    request_session = requests.Session()
    r = request_session.get(url=url, headers={'Authorization': token})
    data = json.loads(r.content.decode('utf-8'))
    classificacao_df = pd.DataFrame.from_dict(data['data'])
    request_session.close()
except requests.exceptions.RequestException as e:
    raise SystemExit(e)
finally:
    request_session.close()


#data_cleaning
final_df = classificacao_df[['idEquipe', 'equipe', 'posicao', 'pontos', 'jogos', 'vitorias', 'derrotas', 'empates', 'vitoriasMandante', 'vitoriasVisitante', 'empatesMandante', 
                            'empatesVisitante', 'derrotasMandante', 'derrotasVisitante', 'maximoPontosPossivel', 'golsPro', 'golsContra', 'saldoDeGols', 'idCampeonato', 'campeonato', ]]

final_df['pontosPorRodada'] = round(final_df.pontos / final_df.jogos, 2)
final_df.to_sql(name='classificacao', con=engine, if_exists='replace')