#!/opt/anaconda3/bin python3
#-*- coding: utf-8 -*-
import csv
import time
import sys
import pycep_correios
from os.path import exists
from pycep_correios import get_address_from_cep, WebService, exceptions

file_exists = exists('/mnt/notebooks/dados_cep.csv')

i=0
y=0
x=['-1']
indiceinv=['-1']
indicesuc=['-1']

file = csv.reader(open('/mnt/notebooks/dados_controle.csv'), delimiter=';') #arquivo gerado pelos comandos do geopy
next(file, None)
if file_exists:
    #pega todos os resultados que foram feitos com sucesso
    filesuc = csv.reader(open('dados_cep.csv'), delimiter=';')
    for line in filesuc:
        indicesuc.append(line[1])
#abre os dois arquivos para gravar e ler.

if file_exists:
    with open('dados_cep.csv', mode='a', newline='') as csv_cep, open('dados_cepinvalido.csv', mode='a', newline='') as csv_cep_inv:
        fieldnames = ["bairro", "cep", "cidade", "logradouro", "uf", "complemento"]
        writer = csv.DictWriter(csv_cep, delimiter=";", fieldnames=fieldnames)
        
        for line in file:
            str2=""
            str1=line[11].strip()  #pegar o cep
            print("cep original:"+str1)
            if len(str1)==7:
                str2="0"+str1
                #print("7!!!!!!!!!!")
            elif len(str1)==8:
                str2=str1[0:5] +"-"+str1[5:9]
                #print("8!!!!!!!!!!")
            elif len(str1.strip())==12:
                print("12!!!!!!!!!!")
                str2=str1[3:8].strip() + str1[8:12].strip()
            else:
                str2=str1
            if str1 != '':
                try:
                    print("Já feito em outro arquivo" + str(indicesuc.index(str2)) + "|||" +str2)
                except ValueError:
                    try:
                        print("Já feito no mesmo arquivo" + str(x.index(str2))) 
                    except ValueError:
                        print("Vai chamar" + str2)  #CEP que vai fazer no try
                        print("tamanho:"+str(len(str2)))
                        #time.sleep(60) 
                        try:
                            if y<=4:
                                y=y+1
                                #print(y)
                                address=pycep_correios.get_address_from_cep(str2)
                                writer.writerow(address)
                                print ("OK"+ str2)
                                x.append(str2) #adiciona ao índice do mesmo arquivo
                            else:
                                print("fechar#########")
                                exit()
                                #sys.exit("Code not Pythonical")
                        except exceptions.InvalidCEP:
                            csv_cep_inv.write(str2 + "\n")
                            print ("invalidcep "+ str2)
                        except exceptions.CEPNotFound:
                            csv_cep_inv.write(str2 + " CEP NOT FOUND"+"\n")
                            csv_cep.write("notfound-----;"+str2+";"+"\n")
                            x.append(str2)
                            print ("notfound "+ str2)
else:
    with open('dados_cep.csv', mode='w', newline='') as csv_cep, open('dados_cepinvalido.csv', mode='w', newline='') as csv_cep_inv:
        fieldnames = ["bairro", "cep", "cidade", "logradouro", "uf", "complemento"]
        writer = csv.DictWriter(csv_cep, delimiter=";", fieldnames=fieldnames)
        writer.writeheader()

        for line in file:
            str1=line[11].strip()  #pegar o cep
            if len(str1)==7:
                str2="0"+str1
            if len(str1)==8:
                str2=str1[0:5] +"-"+str1[5:9]
                #print("8!!!!!!!!!!")
            elif len(str1.strip())==12:
                print("12!!!!!!!!!!")
                str2=str1[3:8].strip() +str1[8:12].strip()
            else:
                str2=str1
            if str1 != '':
                try:
                    print("Já feito em outro arquivo" + str(indicesuc.index(str2)) + "|||" + str2)
                except ValueError:
                    try:
                        print("Já feito no mesmo arquivo" + str(x.index(str2))) 
                    except ValueError:
                        print("Vai chamar" + str2)  #CEP que vai fazer no try
                        #time.sleep(60) 
                        try:
                            if y<=4:
                                y=y+1
                                #print(y)
                                address=pycep_correios.get_address_from_cep(str2)
                                writer.writerow(address)
                                print ("OK"+ str2)
                                x.append(str2) #adiciona ao índice do mesmo arquivo
                            else:
                                print("fechar#########")
                                exit()
                                #sys.exit("Code not Pythonical")
                        except exceptions.InvalidCEP:
                            csv_cep_inv.write(str2 + "\n")
                            print ("invalidcep "+ str2)
                        except exceptions.CEPNotFound:
                            csv_cep_inv.write(str2 + " CEP NOT FOUND"+"\n")
                            csv_cep.write("notfound-----;"+str2+";"+"\n")
                            x.append(str2)
                            print ("notfound "+ str2)


#para rodar:python3.6 /mnt/notebooks/get_cep.py					
##script shell para rodar de 5 em 5 minutos
docker exec -it jupyter-spark bash

while true 
do 
 	python3.6 /mnt/notebooks/get_cep.py	
	sleep 10m
	echo "#########execucao#######"
done


##exemplo de utilização da API:
import csv
import pycep_correios
from pycep_correios import get_address_from_cep, WebService, exceptions

#address = pycep_correios.get_address_from_cep('03043010')
try:
    address2 = pycep_correios.get_address_from_cep('01017-010')

    with open('dados_cepteste.csv', mode='w', newline='') as csv_file:

        fieldnames = ["bairro", "cep", "cidade", "logradouro", "uf", "complemento"]
        writer = csv.DictWriter(csv_file, delimiter=";", fieldnames=fieldnames)

        writer.writeheader()
        #writer.writerow(address)

        writer.writerow(address2)
    
except exceptions.InvalidCEP:
    print("cep invalido")
except exceptions.CEPNotFound:
    print("not found")
    
print(address2)