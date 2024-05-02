#First we import the libraries

import pandas as pd
import time
import requests
from bs4 import BeautifulSoup
import json
import random
import time
from datetime import datetime
import datetime
import os
import numpy as np 
from millify import millify,prettify #tuve que isntalarlo con pip: no hay documentacion de este en anaconda
from tqdm import tqdm #Recuerda que este es el script en donde tienes tu API de google cloud console, con sus restricciones



def get_stats(DEVELOPER_KEY, CHANNEL_ID):

    '''
    Esta función extrae las estadísticas de un canal de youtube, a partir de su ID y la clave de la API de google
        El primer parámetro corresponde a DEVELOPER_KEY: Clave de la API de google
        El segundo parámetro corresponde a CHANNEL_ID: ID del canal de youtube
    
    La función devuelve un diccionario con las estadísticas del canal
     
    '''
    
    url_channel_stats = 'https://youtube.googleapis.com/youtube/v3/channels?part=statistics&id='+CHANNEL_ID+'&key='+DEVELOPER_KEY
    channel_stats = requests.get(url_channel_stats).json() #El json producto del request de la API
    channel_statsDict = channel_stats['items'][0]['statistics'] #Diccionario con las estadísticas, subproducto dle json
    date = pd.to_datetime('today').strftime('%Y-%m-%d') #Elemento con la fecha de hoy
    data_channel = {
    'Created_At': date,
    'Total_views': int(float(channel_statsDict['viewCount'])),
    'Suscribers': int(float(channel_statsDict['subscriberCount'])),
    'video_count': int(float(channel_statsDict['videoCount']))
}
    return data_channel

#Esta funcion 1)extrae la info en un json, 2)crea un diccionario de stats, 3)crea un diccionario con la info final


def channel_stats(df, DEVELOPER_KEY):

    '''
    Esta función extrae las estadísticas de varios canales de youtube, a partir de un dataframe con las ID de los canales
        El primer parámetro corresponde a df: Dataframe con las ID de los canales de youtube
        El segundo parámetro corresponde a api_key: Clave de la API de google

    La función devuelve un dataframe con las estadísticas de los canales    
    '''
    date = []
    views = []
    suscriber = []
    video_count = []
    channel_name = []
    
    tiempo =  [1,2.5,3,2] #Esta lista está relacionada a una buena práctica respecto al request de la API
    
    for i in tqdm(range(len(df)), colour = 'green'):#tqdm es una libreria que permite obtener una 'barra de carga' en in ciclo for
        
        stats_temp = get_stats(DEVELOPER_KEY, df['channel_id'][i])
        #Observa que el insumo es un dataframe con la info de la ID de varios canales
        #Recuerda que esta(i.e. get_stats()) es una funcion que devuelve un diccionario con las estadisticas del canal seleccionado
        
        date.append(stats_temp['Created_At']) #Recuerda que, como stats_temp es un diccionario, --
        views.append(stats_temp['Total_views']) #--podemos acceder a la información de las estadísticas por medio de --
        suscriber.append(stats_temp['Suscribers']) #-- los pares key-value
        video_count.append(stats_temp['video_count'])
        channel_name.append(df['channel_name'][i]) #Recuerda que en stats_name NO se almacena el id del canal, sino en el dataframe
        
        
        time.sleep(random.choice(tiempo)) 
        #Explicacion:
        '''Cada vez que entre a la posicion i del dataframe y se extraigan las estadísticas en stats_temp,
        guardame la date, viwews, suscribers, video_counts y channel_name, y ME ESPERARÁS 1, 2.5, 3 o 2 segundos
        de acuerdo a lo que escoja aleatoriamente y LUEGO vovlerá a hacer la petición CON EL FIN DE NOR ECARGAR LOS 
        SERVIDORES DE TERCEROS.'''
        
    #Ahora creamos un nuevo dataframe con las listas generadas en el paso anterior:
    #--a. Primero creamos un diccionario
    
    data = {
        'Channel_name': channel_name,
        'Suscribers': suscriber,
        'Video_count': video_count,
        'Total_views': views,
        'Created_At': date
    }
    
    #Hasta aqui se ha creado un diccionario con las estadisticas de todos los canales. El paso final
    #--deberia ser el paso de toda esta informacion a una estructura tabular: i.e. un dataframe
    
    #b. Luego creamos un dataframe a partir de este diccionario
    df_channels_final = pd.DataFrame(data)
    
    return df_channels_final
    
    