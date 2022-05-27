# 1ª fase Firsts Step Data Engineer - Desafio Brazilian E-Commerce

# Introdução

Na 1ª fase Firsts Step Data Engineer – ecossistema big data, data governance e agile database project – nós colocamos em prática todo conhecimento adiquirido nos materiais disponibilizados, live e bate papos via Slack.
Foi apresentado neste modulo o mundo dos dados georreferenciados e as possíveis análises que serão realizadas após o tratamento dos dados.

# Cenário

O cenário representado nesta solution sprint como desafio denominado "Brazilian E-Commerce" que está disponivel na plataforma do KAGGLE:(https://www.kaggle.com/olistbr/brazilian-ecommerce) com o dataset  “Olist Dataset” – um conjunto de dados públicos de comércio eletrônico brasileiro - fornecido pela Olist(https://olist.com/).
Olist é uma loja de departamentos que conecta pequenas empresas de todo o Brasil permitindo a elas venderem seus produtos e enviá-los diretamente aos clientes usando os parceiros de logística Olist.

![image](https://user-images.githubusercontent.com/49320014/170607400-4fa9b604-a0a6-4ed1-b537-acf487c1c449.png)

O framework utilizado no cenário será o ecossitema do HADDOP. 
Haddop é de código aberto que permite o armazenamento e processamento distribuídos de grandes conjuntos de dados em clusters de computadores usando modelos de programação simples.

# Projeto 

O projeto será desenvolvido no no ecossistema big data simples com HDFS e Hive

## Arquitetura de dados

![image](https://user-images.githubusercontent.com/49320014/170792848-78fa670d-09f4-4ae2-99a4-f37fee1cbb9e.png)

## Importação dos arquivos para o HDFS

Consulta de arquivos via comando LS no HDFS

![image](https://user-images.githubusercontent.com/49320014/170793542-08372b5e-5728-4f98-979c-846cbd1fdf25.png)

Consulta via HUE

![image](https://user-images.githubusercontent.com/49320014/170793564-6c1b315f-148f-4423-b4f8-bb345d7c688a.png)

## Ecossistema Haddop com Docker
O Docker foi implementado no ambiente Windows seguindo as recomendações disponíveis no GITLAB do Fabio Jardim 
- URL: https://github.com/fabiogjardim/bigdata_docker 

## Importe de arquivos e bibliotecas

- Datanode: mkdir /arquivosolist
  Comando no localhost:

        docker cp F:\Docker\archive\olist_customers_dataset.csv datanode:/arquivosolist
        docker cp F:\Docker\archive\olist_geolocation_dataset.csv datanode:/arquivosolist
        docker cp F:\Docker\archive\olist_order_items_dataset.csv datanode:/arquivosolist
        docker cp F:\Docker\archive\olist_order_payments_dataset.csv datanode:/arquivosolist
        docker cp F:\Docker\archive\olist_order_reviews_dataset.csv datanode:/arquivosolist
        docker cp F:\Docker\archive\olist_orders_dataset.csv datanode:/arquivosolist
        docker cp F:\Docker\archive\olist_products_dataset.csv datanode:/arquivosolist
        docker cp F:\Docker\archive\olist_sellers_dataset.csv datanode:/arquivosolist
        docker cp F:\Docker\archive\product_category_name_translation.csv datanode:/arquivosolist

- Na máquina jupyter-spark:

        mkdir /spark-warehouse/olist

- Copiar para a máquina jupyter-spark

        docker cp F:\Docker\archive\olist_geolocation_dataset.csv jupyter-spark:/spark-warehouse/olist

- Entrar no datanode (onde está o HDFS):

        hadoop fs -mkdir /olist
        hadoop fs -copyFromLocal /arquivosolist/* /olist
        hadoop fs -ls /olist/

- Instalação do hdfs dentro do spark, para que leia arquivos do hdfs

        pip install hdfs

- Configuração do hdfs para o python ler os arquivos e gravar também, dentro do nó do Spark

        cat /root/.hdfscli.cfg
        [global]
        default.alias = hdfs
        
        [hdfs.alias]
        url = http://hdfs.datanode:50075

- HIVE, no Hue:
- Cópia dos arquivos gerados do jupyter-spark para o datanode

        docker cp F:\Docker\dados_cep.csv datanode:/arquivosolist
        docker cp F:\Docker\dados_geolocation.csv datanode:/arquivosolist
        
        hadoop fs -mkdir /olist/customers
        hadoop fs -copyFromLocal /arquivosolist/olist_customers_dataset.csv /olist/customers
        
        hadoop fs -mkdir /olist/geolocation
        hadoop fs -copyFromLocal /arquivosolist/olist_geolocation_dataset.csv /olist/geolocation
        
        hadoop fs -mkdir /olist/dados_geolocation
        hadoop fs -copyFromLocal dados_geolocation.csv /olist/dados_geolocation
        
        hadoop fs -mkdir /olist/dados_cep
        hadoop fs -copyFromLocal dados_cep.csv /olist/dados_cep
        
        hadoop fs -mkdir /olist/orders
        hadoop fs -cp /olist/olist_orders_dataset.csv /olist/orders
        
        hadoop fs -mkdir /olist/order_items
        hadoop fs -cp /olist/olist_order_items_dataset.csv /olist/order_items
        
        hadoop fs -mkdir /olist/products
        hadoop fs -cp /olist/olist_products_dataset.csv /olist/products
        
        hadoop fs -mkdir /olist/payments
        hadoop fs -cp /olist/olist_order_payments_dataset.csv /olist/payments
        
        hadoop fs -mkdir /olist/reviews
        hadoop fs -cp /olist/olist_order_reviews_dataset.csv /olist/reviews
        
        hadoop fs -mkdir /olist/sellers
        hadoop fs -cp /olist/olist_sellers_dataset.csv /olist/sellers


## Criação das tabelas no HIVE

Lista de tabelas com dados obtidos pela biblioteca Olist e dados mestres atualizados via API da biblioteca pycep e geopy e tratados via py-Spark

-  Criação da tabela Cliente - Tabela que armazena os dados mestres obtidos pela bilioteca Olist

        CREATE EXTERNAL TABLE clientes (customer_id string, customer_unique_id string, customer_zip_code_prefix string, customer_city string, customer_state string) ROW FORMAT DELIMITED FIelDS TERMINATED BY ',' STORED AS TEXTFILE

-  Criação da tabela Localizacao - Tabela que armazena os dados mestres obtidos pela bilioteca Olist

        CREATE EXTERNAL TABLE localizacao (geolocation_zip_code_prefix string, geolocation_lat string, geolocation_lng string, geolocation_city string, geolocation_state string) ROW FORMAT DELIMITED FIelDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '/olist/geolocation/'

-  Criação da tabela CEP_CORREIOS - Tabela que armazena os dados mestres obtidos pela bilioteca pycep e tratados via: PY-Spark 

        CREATE EXTERNAL TABLE cep_correios (bairro string, cep string, cidade string, logradouro string, uf string, complemento string) ROW FORMAT DELIMITED FIelDS TERMINATED BY ';' STORED AS TEXTFILE LOCATION '/olist/dados_cep/'

-  Criação da tabela Geolocalização - Tabela que armazena os dados mestres obtidos pela bilioteca geopy e tratados via: PY-Spark 

        CREATE EXTERNAL TABLE dados_geolocation (geolocation_lat string, geolocation_lon string, house_number string, road string, suburb string, ciry_district string, city string, municipality string, county string, state_district string, state string, region string, postcode string, country string) ROW FORMAT DELIMITED FIelDS TERMINATED BY ';' STORED AS TEXTFILE LOCATION '/olist/dados_geolocation/'

-  Criação da tabela Pedidos - Tabela que armazena os dados mestres obtidos pela bilioteca Olist
         
        CREATE EXTERNAL TABLE orders (order_id string, customer_id string, order_status string, order_purchase_timestamp string, order_approvet_at string, order_delivered_carrier_date string, order_delivered_customer_date string, order_estimated_delivery_date string) ROW FORMAT DELIMITED FIelDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '/olist/orders/'

-  Criação da tabela Itens - Tabela que armazena os dados mestres obtidos pela bilioteca Olist
         
        CREATE EXTERNAL TABLE order_items (order_id string, order_item_id string, product_id string, seller_id string, shipping_limit_date string, price decimal(10,2), freight_value decimal(10,2)) ROW FORMAT DELIMITED FIelDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '/olist/order_items/'

-  Criação da tabela Produtos - Tabela que armazena os dados mestres obtidos pela bilioteca Olist
         
         CREATE EXTERNAL TABLE products(product_id string, product_category_name string, product_name_lenght int, product_description_lenght int, product_photos_qty int, product_weight_g decimal(10,2), product_length_cm decimal(10,2), product_height_cm decimal(10,2), product_width_cm decimal (10,2)) ROW FORMAT DELIMITED FIelDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '/olist/products/'

-  Criação da tabela Pagamentos - Tabela que armazena os dados mestres obtidos pela bilioteca Olist
         
        CREATE EXTERNAL TABLE payments (order_id string, payment_sequential int, payment_type string, payment_installments int, payment_value decimal(10,2)) ROW FORMAT DELIMITED FIelDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '/olist/payments/' 

-  Criação da tabela Avaliações - Tabela que armazena os dados mestres obtidos pela bilioteca Olist
         
        CREATE EXTERNAL TABLE reviews (review_id string, order_id string, review_score int, review_comment_title string, review_comment_message string, review_creation_date string, review_answer_timestamp string) ROW FORMAT DELIMITED FIelDS TERMINATED BY '}' STORED AS TEXTFILE LOCATION '/olist/reviews/'

-  Criação da tabela Vendedores - Tabela que armazena os dados mestres obtidos pela bilioteca Olist
        
        CREATE EXTERNAL TABLE sellers (seller_id string, seller_zip_code_prefix string, seller_ciry string, seller_state string) ROW FORMAT DELIMITED FIelDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '/olist/sellers/'

## Consultas SQL para as questões

- Consulta de clientes por geolocalização
     
        select count(*), dg.city, dg.state from clientes c join localizacao g on c.customer_zip_code_prefix = g.geolocation_zip_code_prefix join dados_geolocation dg on g.geolocation_lat = dg.geolocation_lat and g.geolocation_lng = dg.geolocation_lon group by dg.city, dg.state;

- Consulta total de Pedidos por Período e categoria

- Por período
  
        select year(o.order_purchase_timestamp ) || '/' || month(o.order_purchase_timestamp )  as mesano, count(*) as qtd, sum(oi.price) as totalvendido from orders o join order_items oi on o.order_id = oi.order_id group by year(o.order_purchase_timestamp ) || '/' || month(o.order_purchase_timestamp ) order by mesano

- Consulta por categoria 
  
       select count(*) as qtd, sum(oi.price) as totalvendido, p.product_category_name from orders o join order_items oi on o.order_id = oi.order_id join products p on oi.product_id = p.product_id group by p.product_category_name order by p.product_category_name


- Visualização por categoria
![image](https://user-images.githubusercontent.com/49320014/170793876-54376914-fcd1-4f75-80e1-4197cae3354a.png)

- Consulta total de pagamentos por método de pagamento.
  
       select payment_type, sum(payment_value) as total, sum(payment_installments) as total_parcelas from payments group by payment_type

- Visualização pagamentos
![image](https://user-images.githubusercontent.com/49320014/170794009-a3a907bd-0909-4823-9bd5-d833df6eec86.png)

- Notas das Avaliações
As avaliações contém campos com \n , os comentários. O Hive não contempla outro tipo de caracter de final de linha, a não ser esse. Então foi gerado outro aquivo com o Python:
•	Error while compiling statement: FAILED: SemanticException 4:20 LINES TERMINATED BY only supports newline '\n' right now. Error encountered near token ''{}\n''

- Geração de novo arquivo
  
       import csv
       file_path = '/mnt/notebooks/olist_order_reviews_dataset.csv'
       with open(file_path, newline='', encoding='utf-8') as f, open('olist_order_reviews_dataset_mod.csv', mode='w', newline='') as mod:
       reader = csv.reader(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
       for row in reader:
       print(row)
       print(row[4].replace('\r\n', "///"))
       mod.write(row[0] + "}"+ row[1] + "}"+row[2] + "}"+row[3] + "}"+ row[4].replace('\r\n', "///") + "}"+row[5] + "}"+row[6] + "}"+ "\n")
       
       select review_score, count(*) 
       from reviews 
       group by review_score;

- Criação da nova tabela de notas de avaliação
  
       CREATE EXTERNAL TABLE reviews (review_id string, order_id string, review_score int, review_comment_title string, review_comment_message string, review_creation_date string, review_answer_timestamp string) ROW FORMAT DELIMITED FIelDS TERMINATED BY '}' STORED AS TEXTFILE LOCATION '/olist/reviews/'

- Visualização notas de avaliação
![image](https://user-images.githubusercontent.com/49320014/170794326-e15d6cef-0c52-48a6-806d-7307741009bd.png)

- Vendedores x Vendas
  
      select s.seller_id, count(distinct oi.order_id) as qtd_vendas, count(*) as qtd_item_vendas, sum(oi.price) as valor_total_vendas from sellers s join order_items oi on oi.seller_id = s.seller_id group by s.seller_id

- Visualização vendedores x vendas 

![image](https://user-images.githubusercontent.com/49320014/170794408-1560ed1d-b101-4004-8b8d-d70e006e9b2b.png)

- Produtos mais vendidos
  
      select p.product_id, p.product_category_name, count(*) as qtd from products p join order_items oi on p.product_id = oi.product_id group by p.product_id, p.product_category_name order by qtd desc

- Visualização produtos mais vendidos
![image](https://user-images.githubusercontent.com/49320014/170794493-743f8aa0-2b56-4444-8516-891216807c53.png)


## Exemplo de uso da API 

- Atualização dos dados do GEOPY
[Pegar e gravar dados do geopy.txt](https://github.com/vanessaandrade85/Solution-Sprint/files/8789936/Pegar.e.gravar.dados.do.geopy.txt)

- Atualização dos dados dos correios
[Utilizando pycep-correios-linux.txt](https://github.com/vanessaandrade85/Solution-Sprint/files/8789940/Utilizando.pycep-correios-linux.txt)

          
