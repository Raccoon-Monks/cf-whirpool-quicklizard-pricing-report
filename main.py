import functions_framework
import io
import os
import polars as pl
from google.cloud import storage
from google.cloud import bigquery


### Dados sobre bucket de leitura e onde e qual tabela ira salvar
BUCKET_NAME = "whirlpool-retailers-tracking-staging"

# Arquivo dos dados para limpar
PREFIX = "quick-lizard-pricing-report-upload-daily/daily/"

# Arquivos com os dados limpos
DEST_PREFIX =  "quick-lizard-pricing-report-upload-daily/trat/"



@functions_framework.http
def hello_http(request):
    # Funções do GCP que serão utilizadas
    storage_client = storage.Client();
    
    bq_cliente = bigquery.Client();
    bucket = storage_client.bucket(BUCKET_NAME)
    
    try:
        # Lista os arquivos para tratamento no Bucket
        blobs = list(bucket.list_blobs(prefix=PREFIX)) 

        files_processed = 0
        
        for blob in blobs:
            if not blob.name.endswith('.csv') or blob.name == PREFIX:
                continue
            
            # Definir no tipo de arquivo que será salvo: parquet pq é custa menos armazenamento
            file_name_parquet = blob.name.split('/')[-1].replace('.csv', '.parquet')
            dest_blob_name = f"{DEST_PREFIX}{file_name_parquet}"
            
            
            dest_blob = bucket.blob(dest_blob_name)

            
            # Verifica se o arquivo já existe no Bucket
            if not dest_blob.exists():
                gcs_path = f"gs://{BUCKET_NAME}/{blob.name}"
                print(f"Processando: {blob.name}")
                
                
                # Função de tratamento
                df = automate_transfer_data_total_json(gcs_path)
                
                if df is not None:
                    temp_local_path = f"/tmp/{file_name_parquet}"

                    df.write_parquet(temp_local_path, compression="snappy", use_pyarrow=False)
                    
                    # Upload
                    dest_blob.upload_from_filename(temp_local_path)
                    
                    # Limpeza imediata do arquivo temporário para liberar espaço
                    if os.path.exists(temp_local_path):
                        os.remove(temp_local_path)

                    
                    # Força a liberação da memória do DataFrame
                    del df
                    
                    print(f"Salvo com sucesso: {dest_blob_name}")
                files_processed += 1

                
            else:
                print(f"Já existe no GCS, pulando: {dest_blob_name}")

        if files_processed <= 0:
            raise ValueError(f"Quantidade de arquivos inválida: {files_processed}. Deve ser maior que 0.")
            
        print(f"Sucesso! Total de {files_processed} arquivos processados.")

        # Ao final de tudo é chamando a função que insere os dados no BQ e Remove dados tratados

        print("Chamando a função `whirlpool-gcp.executive_report.quicklizard_pricing_report_load`()")
    
        query = 'CALL `whirlpool-gcp.executive_report.quicklizard_pricing_report_load`()'
    
        query_job = bq_cliente.query(query)
    
        query_job.result()
    
        print('Dados salvos no Bq')
                

        # Removendo arquivos tratados
        print('Removendo dados brutos e limpos')
        # Blod daily
        blobs_daily = list(bucket.list_blobs(prefix=PREFIX)) 
        blobs_trat  = list(bucket.list_blobs(prefix=DEST_PREFIX)) 
        
        enum_re = 0

        # Depois do processo de tratamento e salvamento dos dados é removido do bucket tando dados tratado quando bruto
        files_to_delete_csv = [
            blob.name for blob in blobs_daily 
            if blob.name.endswith('.csv')
        ]
        
        files_to_delete_parquet = [
            blob.name for blob in blobs_trat 
            if blob.name.endswith('.parquet')
        ]
        
        if files_to_delete_csv:
            bucket.delete_blobs(files_to_delete_csv)
            print(f"Removidos {len(files_to_delete_csv)} arquivos brutos.")

        if files_to_delete_parquet:
            bucket.delete_blobs(files_to_delete_parquet)
            print(f"Removidos {len(files_to_delete_parquet)} arquivos tratados.")
        
        print("Limpeza concluida e dados adicionados no BQ")
        # Ao final retorna 
        return {"status": "success"} , 200        
    except Exception as e:
        print('Error', e)
        return {"status": "error", "message": str(e)}, 500






def automate_transfer_data_total_json(path):
    # 1. Leitura dos dados (mantendo como string inicialmente para evitar erros de cast)
    data = pl.read_csv(path, infer_schema_length=0)

    # 2. Identificando as colunas de afiliados (.com)
    affiliate_columns = [col for col in data.columns if col.endswith('.com')]

    # 3. Colunas de metadados obrigatórias
    selected_columns = [
        'Shelf Price Guest', 'Shelf Price Logged In', 'Shelf Price Authenticated',
        'Target Price Guest', 'Target Price Logged In', 'Target Price Authenticated',
        'Recommended Price Guest', 'Recommended Price Logged In', 'Recommended Price Authenticated',
        'Shelf Price EPP', 'Shelf Price Premier', 'Shelf Price Select',
        'Target Price EPP', 'Target Price Premier', 'Target Price Select',
        'Recommended Price EPP', 'Recommended Price Premier', 'Recommended Price Select',
        'SKU', 'Date', 'Name', 'Brand', 'Category', 'inventory', 'map', 'pmap',

    ]

    # caso falta os dois ultimos valores
    select_columns_2 = selected_columns + [ 'ABT_INCART', 'PCRICHARDS_INCART' ]



    # Validação básica
    missing_cols = [col for col in selected_columns if col not in data.columns]

    if missing_cols and not all(col in ["PCRICHARDS_INCART", "ABT_INCART"] for col in missing_cols):
        raise ValueError(f"Colunas obrigatórias ausentes: {missing_cols}")


    # 4. Transformação: Colunas .com -> Objeto JSON de Revenue
    # Garantimos que os valores das colunas .com sejam tratados como números (float)
    data = data.with_columns([
        pl.col(col).cast(pl.Float64, strict=False).fill_null(0)
        for col in affiliate_columns
    ])


    # Convertendo para float32
    num_cols = [
        "inventory", "map", "pmap", 'Shelf Price Guest', 'Shelf Price Logged In', 'Shelf Price Authenticated',
        'Target Price Guest', 'Target Price Logged In', 'Target Price Authenticated',
        'Recommended Price Guest', 'Recommended Price Logged In', 'Recommended Price Authenticated',
        'Shelf Price EPP', 'Shelf Price Premier', 'Shelf Price Select',
        'Target Price EPP', 'Target Price Premier', 'Target Price Select',
        'Recommended Price EPP', 'Recommended Price Premier', 'Recommended Price Select'
    ]
    data = data.with_columns(
      pl.col(num_cols).cast(pl.Float32)
    )

    data = data.with_columns(
    pl.col(num_cols).fill_null(0)
    )

    # Criamos o objeto consolidado
    try:
      data_final = data.select(
          selected_columns_2 + [
              pl.struct(affiliate_columns)
              .struct.json_encode()
              .alias("affiliates_revenue")
          ]
      )
    except Exception:
      data_final = data.select(
          selected_columns + [
              pl.struct(affiliate_columns)
              .struct.json_encode()
              .alias("affiliates_revenue")
          ]
      )



    # Criar a colunas PCRICHARDS_INCART e ABT_INCART : Caso não exista
    for col in ["PCRICHARDS_INCART", "ABT_INCART"]:
      if col not in data_final.columns:
        data_final = data_final.with_columns(pl.lit("").alias(col))
    # Removendo nam que possui delete!

    data_final = data_final.filter(pl.col("Name") != "delete!")

    # Passando para Date para não da error no colab
    data_final = data_final.with_columns(
        pl.col("Date").str.to_date("%m/%d/%Y")
    )

    # Pegando data da extração referente a o arquivo a data mais recente
    data_final = data_final.with_columns(
        ext_date = pl.col("Date").max()
    )

    data_final = data_final.rename({
        col: col.lower().replace(" ", "_")
        for col in data_final.columns
    })
    return data_final

