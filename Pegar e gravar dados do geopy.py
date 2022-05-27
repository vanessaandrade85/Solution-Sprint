import csv
import time
from os.path import exists
from geopy.geocoders import Nominatim
geolocator=Nominatim(user_agent="OlistFiapGrupoB")


x=['-1']
indicesuc=[]

file = csv.reader(open('/spark-warehouse/olist/olist_geolocation_dataset.csv'), delimiter=',')
next(file, None)

file_exists = exists('/mnt/notebooks/dados_geolocation.csv')

if file_exists:
    filesuc = csv.reader(open('dados_geolocation.csv'), delimiter=';')
    csv.field_size_limit(256<<10)
    csv.field_size_limit()
    for line in filesuc:
        #print(line[0] + ', '+ line[1])
        indicesuc.append(line[0] + ', '+ line[1])

if file_exists:
    with open('dados_geolocation.csv', mode='a', newline='') as csv_gravar:
        #csv_gravar.write("geolocation;"+"house_number"+";"+ "road"+";"+ "suburb"+";"+ "city_district"+";"+ "city"+";"+ "municipality"+";"+ "county"+";"+ "state_district"+";"+ "state"+";"+ "region"+";"+ "postcode"+";"+ "country" + "\n")

        for line in file:
            str1=line[1] + ', ' + line[2] #pegar latitude e longitude para passar para a função reverse

            try:
                print("indice encontrado" + str(x.index(str1))) 
            
            except ValueError:
                x.append(str1) #adiciona ao índice do mesmo arquivo
                print(str1)  #printar as coordenadas

                try:
                    print("indice encontrado em outro arquivo:" + str(indicesuc.index(str1)))
                
                except ValueError:
                    try:
                        location=geolocator.reverse(str1) #armazenar em location os endereços completos

                        result = line[1] + '; ' + line[2] +";"

                        if "house_number" in location.raw['address']:
                            result = result + location.raw['address']['house_number'] +";"
                        else: 
                            result = result + ";"

                        if "road" in location.raw['address']:
                            result = result + location.raw['address']['road'] +";"
                        else:
                            result = result + ";"

                        if "suburb" in location.raw['address']:
                            result = result + location.raw['address']['suburb'] +";" 
                        else: 
                            result = result + ";"

                        if "city_district" in location.raw['address']:
                            result = result + location.raw['address']['city_district']+";" 
                        else: 
                            result = result + ";"

                        if "city" in location.raw['address']:
                            result = result + location.raw['address']['city'] +";" 
                        else:
                            result = result + ";"

                        if "municipality" in location.raw['address']:
                            result = result + location.raw['address']['municipality'] +";" 
                        else:
                            result = result + ";"

                        if "county" in location.raw['address']:
                            result = result + location.raw['address']['county'] +";" 
                        else:
                            result = result + ";"

                        if "state_district" in location.raw['address']:
                            result = result + location.raw['address']['state_district'] +";" 
                        else:
                            result = result + ";"

                        if "state" in location.raw['address']:
                            result = result + location.raw['address']['state'] +";" 
                        else:
                            result = result + ";"

                        if "region" in location.raw['address']:
                            result = result + location.raw['address']['region'] +";" 
                        else:
                            result = result + ";"

                        if "postcode" in location.raw['address']:
                            result = result + location.raw['address']['postcode'] +";"
                        else:
                            result = result + ";"


                        result = result + location.raw['address']['country'] + "\n"


                        csv_gravar.write(result)
                            #print(location.raw['address']['house_number']+";"+ location.raw['address']['road']+";"+ location.raw['address']['suburb']+";"+ location.raw['address']['city_district']+";"+ location.raw['address']['city']+";"+ location.raw['address']['municipality']+";"+ location.raw['address']['county']+";"+ location.raw['address']['state_district']+";"+ location.raw['address']['state']+";"+ location.raw['address']['region']+";"+ location.raw['address']['postcode']+";"+ location.raw['address']['country']) 
                        time.sleep(1) # Nas condições do geopy, só é possível fazer uma consulta por segundo.
                    except ValueError:
                        print("Erro"+str1)
                
else:
    with open('dados_geolocation.csv', mode='w', newline='') as csv_gravar:
        csv_gravar.write("geolocation_lat;"+"geolocation_lon;"+"house_number"+";"+ "road"+";"+ "suburb"+";"+ "city_district"+";"+ "city"+";"+ "municipality"+";"+ "county"+";"+ "state_district"+";"+ "state"+";"+ "region"+";"+ "postcode"+";"+ "country" + "\n")

        for line in file:
            str1=line[1] + ', ' + line[2] #pegar latitude e longitude para passar para a função reverse

            try:
                print("indice encontrado" + str(x.index(str1))) 
            except ValueError:
                x.append(str1) #adiciona ao índice
                print(str1)  #printar as coordenadas

                try:
                    location=geolocator.reverse(str1) #armazenar em location os endereços completos

                    result = line[1] + '; ' + line[2] +";"

                    if "house_number" in location.raw['address']:
                        result = result + location.raw['address']['house_number'] +";"
                    else: 
                        result = result + ";"

                    if "road" in location.raw['address']:
                        result = result + location.raw['address']['road'] +";"
                    else:
                        result = result + ";"

                    if "suburb" in location.raw['address']:
                        result = result + location.raw['address']['suburb'] +";" 
                    else: 
                        result = result + ";"

                    if "city_district" in location.raw['address']:
                        result = result + location.raw['address']['city_district']+";" 
                    else: 
                        result = result + ";"

                    if "city" in location.raw['address']:
                        result = result + location.raw['address']['city'] +";" 
                    else:
                        result = result + ";"

                    if "municipality" in location.raw['address']:
                        result = result + location.raw['address']['municipality'] +";" 
                    else:
                        result = result + ";"

                    if "county" in location.raw['address']:
                        result = result + location.raw['address']['county'] +";" 
                    else:
                        result = result + ";"

                    if "state_district" in location.raw['address']:
                        result = result + location.raw['address']['state_district'] +";" 
                    else:
                        result = result + ";"

                    if "state" in location.raw['address']:
                        result = result + location.raw['address']['state'] +";" 
                    else:
                        result = result + ";"

                    if "region" in location.raw['address']:
                        result = result + location.raw['address']['region'] +";" 
                    else:
                        result = result + ";"

                    if "postcode" in location.raw['address']:
                        result = result + location.raw['address']['postcode'] +";"
                    else:
                        result = result + ";"


                    result = result + location.raw['address']['country'] + "\n"


                    csv_gravar.write(result)
                        #print(location.raw['address']['house_number']+";"+ location.raw['address']['road']+";"+ location.raw['address']['suburb']+";"+ location.raw['address']['city_district']+";"+ location.raw['address']['city']+";"+ location.raw['address']['municipality']+";"+ location.raw['address']['county']+";"+ location.raw['address']['state_district']+";"+ location.raw['address']['state']+";"+ location.raw['address']['region']+";"+ location.raw['address']['postcode']+";"+ location.raw['address']['country']) 
                    time.sleep(1) # Nas condições do geopy, só é possível fazer uma consulta por segundo.