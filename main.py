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
def hello_gcs(request):
    storage_client = storage.Client()
    bq_client = bigquery.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    
    # Captura o arquivo específico do evento
    request_json = request.get_json(silent=True)
    if not request_json or 'name' not in request_json:
        return {"status": "ignored", "message": "No filename"}, 200

    file_path = request_json['name']
    
    # Ignora se não for CSV ou se não estiver na pasta 'daily'
    if not file_path.endswith('.csv') or not file_path.startswith(PREFIX):
        return {"status": "Ignorado", "message": "Não é um CSV"}, 200

    try:
        blob = bucket.blob(file_path)
        if not blob.exists():
            return {"status": "Erro", "message": "Blob desaparecido"}, 404

        # Nome do arquivo de destino
        file_name = file_path.split('/')[-1]
        parquet_name = file_name.replace('.csv', '.parquet')
        dest_path = f"{DEST_PREFIX}{parquet_name}"
        dest_blob = bucket.blob(dest_path)

        # Processamento Individual
        print(f"Processando arquivo único: {file_path}")
        gcs_uri = f"gs://{BUCKET_NAME}/{file_path}"
        
        # Chama sua função de tratamento (supondo que ela aceite o path gs://)
        df = automate_transfer_data_total_json(gcs_uri)
        
        if df is not None:
            local_tmp = f"/tmp/{parquet_name}"
            df.write_parquet(local_tmp, compression="snappy")
            
            # Upload do tratado
            dest_blob.upload_from_filename(local_tmp)
            if os.path.exists(local_tmp):
                os.remove(local_tmp)
            
            print(f"Upload concluído: {dest_path}")

            # Integração BigQuery
            # Procedure 
            query = f"CALL `whirlpool-gcp.executive_report.quicklizard_pricing_report_load`('{parquet_name}')"
            print(f"Executando Procedure no BigQuery... {query}")
            bq_client.query(query).result()

            # 5. Limpeza cirúrgica (Deleta apenas o que processou)
            blob.delete()
            dest_blob.delete()
            print(f"Limpeza concluída para: {file_name}")
            
            return {"status": "success", "processed": file_name}, 200

    except Exception as e:
        print(f"Erro ao processar {file_path}: {str(e)}")
        # Retornamos 200 em erros de 'File Not Found' para evitar Retries infinitos do GCP
        return {"status": "error", "message": str(e)}, 200






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

