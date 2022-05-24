# 1ª fase Firsts Step Data Engineer - Desafio Brazilian E-Commerce

# Introdução

Na 1ª fase Firsts Step Data Engineer – ecossistema big data, data governance e agile database project – nós colocamos em prática todo conhecimento adiquirido nos materiais disponibilizados, live e bate papos via Slack.
Foi apresentado neste modulo o mundo dos dados georreferenciados e as possíveis análises que serão realizadas após o tratamento dos dados.

# Cenário

O cenário representado nesta solution sprint como desafio denominado "Brazilian E-Commerce" que está disponivel na plataforma do KAGGLE:(https://www.kaggle.com/olistbr/brazilian-ecommerce) com o dataset  “Olist Dataset” – um conjunto de dados públicos de comércio eletrônico brasileiro - fornecido pela Olist(https://olist.com/).
Olist é uma loja de departamentos que conecta pequenas empresas de todo o Brasil permitindo a elas venderem seus produtos e enviá-los diretamente aos clientes usando os parceiros de logística Olist.

![image](https://user-images.githubusercontent.com/49320014/169908969-aad0ff2e-7bb1-46f3-b269-00eb1a29655a.png)

O framework utilizado no cenário será o ecossitema do HADDOP. 
Haddop é de código aberto que permite o armazenamento e processamento distribuídos de grandes conjuntos de dados em clusters de computadores usando modelos de programação simples.

# Projeto 

O projeto será desenvolvido no no ecossistema big data simples com HDFS e Hive

## Arquitetura de dados
![image](https://user-images.githubusercontent.com/49320014/169921232-beefc3f2-54bf-43ce-ae77-da290b41689b.png)

## Importação dos arquivos para o HDFS
 2) Evidências da importação dos arquivos no HDFS – Print do ls (hdfs.jpeg).

## Utilização das APIS
 5) Exemplo de uso da API.
### Atualização dos dados de localização
#### Geolocalização
#### Correios

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
 4) Um documento denominado resposta.doc com as queries e as respostas das questões propostas.
### Segmentar os clientes por geolocalização
### Total de pedidos por período e categorias.
### Total de pagamentos por método de pagamento.
### Notas das avaliações.
###  Vendedores x vendas.
### Produtos mais vendidos.
  
           
          
          
