# CF  Whirpool Quicklizard Pricing Report

O codigo consiste em  tratar os dados do Quick Lizard (que são inseridos no Google Drive da Whirlpool via extração) e, em seguida, passá-los para o BigQuery. A CF é acionada quanto ocorre um upload no bucket de whirlpool-retailers-tracking-staging nesse bucket possui 2 subpastas : 

* quick-lizard-pricing-report-upload-daily
    * trat : Nessa pasta é salvo o dado tratado parquet
    *  daily : Nessa pasta é colocado o dados brutos que será utilizado para o tratamento
    * **OBSERVAÇÃO : Fazer o upload de um arquivo por vez**


* quick-lizard-pricing-report-upload-trat/all
    * Nessa pasta foi utilizada para fazer um upload total dos dados históricos para evitar sobrecarregamento do CF


O código é um reaproveitamento do que foi feito no [Colab Whirpool Tratamento](https://colab.research.google.com/drive/1QeMRC7XXmb1fCcbgWvYPTfVTiKTP8H_d?usp=sharing)
> Foi feito diversos maneira para tratar os dados o que foi definido está escrito como OFICIAL


**O código com funciona ?**

1. Primeiro é necessário ativar o trigger que, por sua vez, só é acionado quando é feito o upload de um arquivo.
UM POR VEZ
2. Com o acionamento, a CF é executada, fazendo o mesmo processo citado em tópicos anteriores. O resultado é salvo no tratamento (em arquivo .parquet).
3. No código, é chamado um PROCEDURE:


```SQL
CREATE OR REPLACE PROCEDURE `whirlpool-gcp.executive_report.quicklizard_pricing_report_load`(nome_do_arquivo STRING)
BEGIN
  DECLARE v_uri STRING;
  DECLARE load_query STRING;

  
  SET v_uri = CONCAT('gs://whirlpool-retailers-tracking-staging/quick-lizard-pricing-report-upload-daily/trat/', nome_do_arquivo);

 
  SET load_query = FORMAT("""
    LOAD DATA OVERWRITE `whirlpool-gcp.executive_report.table_quick_lizard_pricing_report_daily_upload_bucket`
    FROM FILES (
      format = 'PARQUET',
      uris = ['%s']
    )
  """, v_uri);
  
  EXECUTE IMMEDIATE load_query;

  INSERT `whirlpool-gcp.executive_report.table_quick_lizard_pricing_report_daily`
    (
    shelf_price_guest,
    shelf_price_logged_in,
    shelf_price_authenticated,
    target_price_guest,
    target_price_logged_in,
    target_price_authenticated,
    recommended_price_guest,
    recommended_price_logged_in,
    recommended_price_authenticated,
    shelf_price_epp,
    shelf_price_premier,
    shelf_price_select,
    target_price_epp,
    target_price_premier,
    target_price_select,
    recommended_price_epp,
    recommended_price_premier,
    recommended_price_select,
    sku,
    date,
    name,
    brand,
    category,
    inventory,
    map,
    pmap,
    affiliates_revenue,
    pcrichards_incart,
    abt_incart,
    ext_date
  )
  SELECT
    shelf_price_guest,
    shelf_price_logged_in,
    shelf_price_authenticated,
    target_price_guest,
    target_price_logged_in,
    target_price_authenticated,
    recommended_price_guest,
    recommended_price_logged_in,
    recommended_price_authenticated,
    shelf_price_epp,
    shelf_price_premier,
    shelf_price_select,
    target_price_epp,
    target_price_premier,
    target_price_select,
    recommended_price_epp,
    recommended_price_premier,
    recommended_price_select,
    sku,
    date,
    name,
    brand,
    category,
    inventory,
    map,
    pmap,
    affiliates_revenue,
    pcrichards_incart,
    abt_incart,
    ext_date
  FROM
    `whirlpool-gcp.executive_report.table_quick_lizard_pricing_report_daily_upload_bucket`
    AS t1

  WHERE
    NOT EXISTS(
      SELECT 1
      FROM `whirlpool-gcp.executive_report.table_quick_lizard_pricing_report_daily` t2
      WHERE t2.ext_date = t1.ext_date
    );
  
  DELETE FROM `whirlpool-gcp.executive_report.table_quick_lizard_pricing_report_daily_upload_bucket` AS t1
  WHERE EXISTS(
      SELECT 1
      FROM `whirlpool-gcp.executive_report.table_quick_lizard_pricing_report_daily` AS t2
      WHERE t2.ext_date = t1.ext_date
    );  
END;

```

> O que faz?
> Ler os dados tratados no bucket, adicioná-los a uma tabela vazia e, em seguida, inserir esses dados no histórico e popular novamente a tabela que estava vazia anteriormente.

4. Ao final, são excluídos tanto o dado tratado quanto o dado bruto.
Se isso não ocorrer, é porque houve algum erro na CF.


## Ferramentas utilizadas

* functions-framework==3.*
* google-cloud-storage
* google-cloud-bigquery
* polars
* gcsfs
* fsspec





