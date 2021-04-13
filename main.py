#!/usr/bin/python
from pymongo import MongoClient
import sys
import pandas as pd

def main():

    arguments = list(sys.argv)

    Host = '127.0.0.1'
    Port = 52471

    DSN = "mongodb://{}:{}".format(Host, Port)

    conn = MongoClient(DSN)

    #creem una base de dades on crearem les col·leccions
    bd = conn['projecte']
    while len(arguments) != 0:
        if arguments[0] == '-f':
            file = arguments[1]
            new_bd(file, bd)
            arguments.pop(0)
            arguments.pop(0)
        if arguments[0] == '--delete_all':
            if arguments[1] == '--bd':
                BD = arguments[2]
                try:
                    BD.dropDatabase()
                except:
                    raise AssertionError('Error : this bd does not exist')
                arguments.pop(1)
                arguments.pop(1)
            else:
                raise AssertionError('Error : no bd specified ')
            arguments.pop(0)
        else:
            arguments.pop(0)
    conn.close()


def new_bd(file, bd):
    #colecciones que hemos definido nosotras en el ejercicio 1
    coleciones = ['autores', 'revista', 'cuentos', 'volumen']
    xls = pd.ExcelFile(file, engine='openpyxl')
    hojas = {}
    for i in range(len(xls.sheet_names)):
        nom = xls.sheet_names[i]
        df1 = (pd.read_excel(xls, nom)).to_dict()
        hoja_c = corretgir_info(df1)
        hojas[nom] = hoja_c

    creades = bd.list_collection_names()
    for col in coleciones:
        #evitar volver a crear una coleccion ya existente
        if col not in creades:
            coll = bd.create_collection(col)
        if col == 'autores':
            hoja_autores = hojas['autores']
            for fila in range(len(hoja_autores['Autors'])):
                nombre = hoja_autores['Autors'][fila]
                if nombre != 'nan':
                    anonimo = 0
                    if nombre[:11].lower() == "desconocido":
                        nombre = nombre[13:]
                        anonimo = 1
                    elif nombre.lower() == "anonimo":
                        anonimo = 1
                    #comprovamos si ya existe un documento con ese nombre
                    exist = coll.find({'_id': nombre}).count()
                    if exist <= 0:
                        #no existe aun
                        alias = hoja_autores['alias'][fila]
                        if alias != 'nan':
                            coll.insert({'_id': nombre, 'extrangero': hoja_autores['extranjero'][fila], 'anonimo': anonimo,
                                             'alias': alias, 'cuentos': [hoja_autores['cuento'][fila]]})
                        else:
                            coll.insert({'_id': nombre, 'extrangero': hoja_autores['extranjero'][fila], 'anonimo': anonimo, 'cuentos': [hoja_autores['cuento'][fila]]})
                    else:
                        #esta bé??
                        #afegiriem el conte a la llista de contes d'aquest autor ja creat prèviament
                        coll.update({'_id': nombre},{'$push':{'cuentos':{'$each':[hoja_autores['cuento'][fila]]}}})

        elif col == 'revista':
            hoja_revista = hojas['numeros_revistes']
            hoja_colaboraciones = hojas['colaboraciones']
            for fila in range(len(hoja_revista['Revista'])):
                revista = hoja_revista['Revista'][fila]
                fecha = hoja_revista['Fecha'][fila]
                if revista != 'nan':
                    exist = coll.find({'_id': {'titulo':revista,'fecha':fecha}}).count()
                    if exist <= 0:
                        #no existe aun
                        volumen = hoja_revista['volumen'][fila]
                        if volumen != 'nan':
                            coll.insert({'_id':{'titulo':revista,'fecha':fecha},'numero':hoja_revista['Numero'][fila], 'volumen':volumen, 'cuentos':[]})
                        else:
                            coll.insert(
                                {'_id': {'titulo': revista, 'fecha': fecha}, 'numero': hoja_revista['Numero'][fila],'cuentos':[]})
            for fila in range(len(hoja_colaboraciones['revista'])):
                revista = hoja_colaboraciones['revista'][fila]
                fecha = hoja_colaboraciones['fecha'][fila]
                if revista != 'nan':
                    exist = coll.find({'_id': {'titulo': revista, 'fecha': fecha}}).count()
                    if exist <= 0:
                        volumen = hoja_colaboraciones['tomo'][fila]
                        numero = hoja_colaboraciones['numero'][fila]
                        coll.insert({'_id': {'titulo': revista, 'fecha': fecha}, 'numero': numero,
                                     'volumen': volumen,'cuentos':[]})

        elif col == 'cuentos':
            hoja_cuentos = hojas['Cuentos']
            hoja_temas = hojas['temas']
            hoja_generos = hojas['Genero']
            hoja_colaboraciones = hojas['colaboraciones']
            hoja_traducciones = hojas['traducciones']

            for fila in range(len(hoja_cuentos['revista'])):
                titulo = hoja_cuentos['titulo'][fila]
                if titulo != 'nan':
                    coll_A = bd.get_collection('autores')
                    autor = coll_A.find({'cuentos':titulo}).projection({'_id':1})
                    #autor pot contenir més d'un document
                    for doc in autor:
                        exist = coll.find({'_id': {'titulo': titulo, 'autor': doc}}).count()
                        if exist <= 0:
                            coll.insert({'_id': {'titulo': titulo, 'autor': doc},'temas':[],'generos':[]})
                    titulo_revista = hoja_cuentos['revista'][fila]
                    fecha = hoja_cuentos['fecha'][fila]
                    coll_R = bd.get_collection('revista')
                    coll_R.update({'_id':{'titulo':titulo_revista,'fecha':fecha}},{'$push': {'cuentos':{'$each': [{'titulo_cuento':titulo,
                                            'fiabilidad':hoja_cuentos['fiabilidad'][fila],'paginas':hoja_cuentos['paginas'][fila]}]}}})


            for fila in range(len(hoja_temas['titulo'])):
                titulo = hoja_temas['titulo'][fila]
                if titulo != 'nan':
                    coll.update({'_id':{'titulo':titulo}},{'$push': {'temas': {'$each': [hoja_temas['temas'][fila]]}}})
            for fila in range(len(hoja_generos['titulo'])):
                titulo = hoja_generos['titulo'][fila]
                if titulo != 'nan':
                    coll.update({'_id':{'titulo':titulo}},{'$push': {'generos': {'$each': [hoja_generos['Género'][fila]]}}})
            for fila in range(len(hoja_traducciones['titulo'])):
                titulo = hoja_traducciones['titulo'][fila]
                if titulo != 'nan':
                    autor = hoja_traducciones['Firmado'][fila]
                    titulo_original = hoja_traducciones['Titulo original'][fila]
                    if autor != 'nan':
                        if titulo_original != 'nan':
                            coll.update({'_id':{'titulo':titulo,'autor':autor}},{'$set':{'traducciones':{'titulo_original':titulo_original,'traductor':hoja_traducciones['Traductor'][fila]}}})
                        else:
                            coll.update({'_id': {'titulo': titulo,'autor':autor}}, {'$set': {
                                'traducciones': {'traductor': hoja_traducciones['Traductor'][fila]}}})
                    else:
                        if titulo_original != 'nan':
                            coll.update({'_id':{'titulo':titulo}},{'$set':{'traducciones':{'titulo_original':titulo_original,'traductor':hoja_traducciones['Traductor'][fila]}}})
                        else:
                            coll.update({'_id': {'titulo': titulo}}, {'$set': {
                                'traducciones': {'traductor': hoja_traducciones['Traductor'][fila]}}})
            for fila in range(len(hoja_colaboraciones['revista'])):
                titulo = hoja_colaboraciones['titulo'][fila]
                if titulo != 'nan':
                    autor = coll_A.find({'cuentos': titulo}).projection({'_id': 1})
                    # autor pot contenir més d'un document
                    for doc in autor:
                        exist = coll.find({'_id': {'titulo': titulo, 'autor': doc}}).count()
                        if exist <= 0:
                            traductor = hoja_colaboraciones['traductor'][fila]
                            if traductor != 'nan':
                                coll.insert({'_id': {'titulo': titulo, 'autor': doc},'traducciones':{'traductor':traductor},
                                             'clasificacion':hoja_colaboraciones['clasificacion'][fila],'notas':hoja_colaboraciones['notas'][fila],
                                             'versos':hoja_colaboraciones['versos'][fila]})
                    titulo_revista = hoja_colaboraciones['revista'][fila]
                    fecha = hoja_colaboraciones['fecha'][fila]
                    pagina1 = hoja_colaboraciones['pinicial'][fila]
                    pagina2 = hoja_colaboraciones['pfinal'][fila]
                    if pagina2 != 'nan':
                        pagina1 +='-'+pagina2
                    coll_R.update({'_id':{'titulo': titulo_revista, 'fecha': fecha}}, {'$push': {'cuentos':{'$each': [{'titulo_cuento': titulo,
                                                 'paginas':pagina1}]}}})

        elif col == 'volumen':
            hoja_volumen = hojas['volumenes_cuentos']
            for fila in range(len(hoja_volumen['titulo_volumen'])):
                titulo = hoja_volumen['titulo_volumen'][fila]
                if titulo != 'nan':
                    fecha = hoja_volumen['fecha'][fila]
                    editorial = hoja_volumen['Editorial'][fila]
                    exist = coll.find({'_id': {'titulo': titulo, 'fecha': fecha, 'editorial':editorial}}).count()
                    if exist <= 0:
                        coll.insert({'_id': {'titulo': titulo, 'fecha': fecha, 'editorial':editorial},'lugar':hoja_volumen['lugar'][fila],'cuentos':[]})
                    else:
                        # esta bé??
                        # afegiriem el conte a la llista de contes d'aquest autor ja creat prèviament
                        pagina = hoja_volumen['páginas'][fila]
                        if pagina != 'nan':
                            coll.update({'_id': {'titulo': titulo, 'fecha': fecha, 'editorial':editorial}},
                                        {'$push': {'cuentos': {'$each': [{'titulo_cuento':hoja_volumen['titulo cuento'][fila],'autor':hoja_volumen['nombre'][fila],
                                                                          'fiabilidad':hoja_volumen['fiabilidad'][fila],'paginas':pagina}]}}})
                        else:
                            coll.update({'_id': {'titulo': titulo, 'fecha': fecha, 'editorial': editorial}},
                                        {'$push': {'cuentos': {'$each': [{'titulo_cuento': hoja_volumen['titulo cuento'][fila],'autor':hoja_volumen['nombre'][fila],
                                                                          'fiabilidad': hoja_volumen['fiabilidad'][fila]}]}}})



def corretgir_info(hoja):
    for x in hoja:
        for i in x.values():
            if i[0] == " ":
                i = i[1:]
            if x.lower() == 'fecha':
                if len(i) == 4:
                    i == "00/00/"+str(i)
                elif i[2] != "/":
                    year = i[:4]
                    mes = i[5:7]
                    dia = i[8:10]
                    i = str(dia)+"/"+str(mes)+"/"+str(year)
                elif i[10:] == ' 00:00:00':
                    i = i[:10]
            elif x == 'autor':
                autor = list(i)
                while "[" in autor:
                    autor.remove("[")
                    autor.remove("]")
                i = ''.join(autor)
    return hoja
