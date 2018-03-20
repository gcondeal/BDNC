import json
from lxml import etree

#cargamos el fichero leido en memoria para poder tratarlo como un XML
parser = etree.XMLParser(load_dtd=True, resolve_entities=True)
dblpXML = etree.parse('data/dblp_redux.xml',parser=parser)
dblpXMLRoot = dblpXML.getroot()

l_autores = {}

for node in dblpXMLRoot.getchildren():
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

    elemento = {"tipo" : node.tag,
                "titulo" : node.find("title").text if node.find("title") != None else '',
                "year" : year,
                "autores" : list(autores)
                }

    for autor in list(autores):
        if autor in l_autores:
            aux_autor = l_autores[autor]
        else:
            aux_autor = {
                "nombre" : autor,
                "year_ini" : year,
                "year_fin" : year,
                "publicaciones" : 0,
                "coautores" : set()
            }
            l_autores[autor] = aux_autor

        if(year < aux_autor["year_ini"]):
            aux_autor["year_ini"] = year
        elif(year > aux_autor["year_fin"]):
            aux_autor["year_fin"] = year

        aux_autor["publicaciones"] += 1

        aux_autor["coautores"] = aux_autor["coautores"].union(autores)

    print(json.dumps(elemento))



