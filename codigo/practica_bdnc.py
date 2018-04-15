import json
import csv
from lxml import etree
import pymongo
import sys

from pymongo import MongoClient

# conexión
con = MongoClient('localhost', 27017)
db = con.dblp

# colección
col_public = db.publicaciones
col_autores = db.autores

#col_autores.drop()
#col_public.drop()

bloque_publicaciones = 5000

xmlfile = 'data/dblp.xml'

nodosPubfile = open('data/nodosPub.csv', 'w', encoding='utf-8', newline='')
nodosPubwriter = csv.writer(nodosPubfile, delimiter='\t',quotechar='|', quoting=csv.QUOTE_MINIMAL)
nodosPubwriter.writerow(["nombre"])
nodosAutfile = open('data/nodosAut.csv', 'w', encoding='utf-8', newline='')
nodosAutwriter = csv.writer(nodosAutfile, delimiter='\t',quotechar='|', quoting=csv.QUOTE_MINIMAL)
nodosAutwriter.writerow(["nombre"])

relacionesPubfile = open('data/relacionesPub.csv', 'w', encoding='utf-8', newline='')
relacionesPubwriter = csv.writer(relacionesPubfile, delimiter='\t',quotechar='|', quoting=csv.QUOTE_MINIMAL)
relacionesPubwriter.writerow(["nodo1", "nodo2", "relacion"])
relacionesAutfile = open('data/relacionesAut.csv', 'w', encoding='utf-8', newline='')
relacionesAutwriter = csv.writer(relacionesAutfile, delimiter='\t',quotechar='|', quoting=csv.QUOTE_MINIMAL)
relacionesAutwriter.writerow(["nodo1", "nodo2", "relacion"])

def iterate_xml(xmlfile):
    doc = etree.iterparse(xmlfile, events=('start', 'end'),  load_dtd=True,  attribute_defaults=True)
    _, root = next(doc)
    start_tag = None
    for event, element in doc:
        if event == 'start' and start_tag is None:
            start_tag = element.tag
        if event == 'end' and element.tag == start_tag:
            yield element
            start_tag = None
            root.clear()

l_autores = {}
l_public = []
inserciones = 0

for node in iterate_xml(xmlfile):#dblpXMLRoot.getchildren():
    # nos saltamos cierto tipo de tag's que no proporcinan información que necesitamos
    if node.tag in ["www", "person", "data"]:
        continue

    autores = set()
    for autor in node.findall("author"):
        autores.add(autor.text)

    if len(autores) == 0:
        for editor in node.findall("editor"):
            autores.add(editor.text)


    year = int(node.find("year").text if node.find("year") != None else '0')

    elemento = {"_id" : node.attrib["key"],
                "tipo" : node.tag,
                "titulo" : node.find("title").text if node.find("title") != None and node.find("title").text != None else '',
                "year" : year,
                "autores" : list(autores)
                }

    l_public.append(elemento)


    #vamos creando el csv
    nodosPubwriter.writerow([elemento["titulo"].replace('"','')])

    for autor in autores:
        relacionesPubwriter.writerow([autor, elemento["titulo"].replace('"',''), "ES_AUTOR"])

        for coautor in autores:
            if coautor != autor:
                relacionesAutwriter.writerow([coautor, autor, "ES_COAUTOR"])


    if len(l_public) % bloque_publicaciones == 0:
 #       col_public.insert_many(l_public)
        l_public = []
        inserciones += 1
        sys.stdout.write("\rInsertadas %d publicaciones." % (inserciones * bloque_publicaciones))
        sys.stdout.flush()

    for autor in list(autores):
        if autor in l_autores:
            aux_autor = l_autores[autor]
        else:
            aux_autor = {
                "nombre" : autor,
                "year_ini" : year,
                "year_fin" : year,
                "edad" : 0,
                "num_publicaciones" : 0,
                "publicaciones": [],
                "coautores" : set()
            }
            l_autores[autor] = aux_autor

        if(year < aux_autor["year_ini"]):
            aux_autor["year_ini"] = year
        elif(year > aux_autor["year_fin"]):
            aux_autor["year_fin"] = year

        aux_autor["edad"] = aux_autor["year_fin"] - aux_autor["year_ini"]
        aux_autor["num_publicaciones"] += 1
        aux_autor["publicaciones"].append(elemento["titulo"])

        aux_autor["coautores"] = aux_autor["coautores"].union(autores)

   # print(json.dumps(elemento))


if len(l_public) > 0:
#    col_public.insert_many(l_public)
    sys.stdout.write("\rInsertadas %d publicaciones.\n" % ((inserciones * bloque_publicaciones) + len(l_public)))
    sys.stdout.flush()

# preparamos la lista de autores para insertarlos en la BBDD
lista_autores = []
inserciones = 0
for autor in l_autores:
    aux_autor = l_autores[autor]
    aux_autor["coautores"].remove(aux_autor["nombre"])
    aux_autor["coautores"] = None if len(aux_autor["coautores"]) == 0 else list(aux_autor["coautores"])
    lista_autores.append(aux_autor)

    nodosAutwriter.writerow([aux_autor["nombre"].replace('"','')])

    inserciones += 1
    if inserciones % bloque_publicaciones == 0:

 #           r = col_autores.insert_many(lista_autores)
        lista_autores = []
        sys.stdout.write("\rInsertados %d autores." % (inserciones))
        sys.stdout.flush()

if len(lista_autores) > 0:
#    col_autores.insert_many(lista_autores)
    sys.stdout.write("\rInsertados %d autores." % (inserciones))
    sys.stdout.flush()

nodosPubfile.close()
nodosAutfile.close()
relacionesAutfile.close()
relacionesPubfile.close()
con.close()
