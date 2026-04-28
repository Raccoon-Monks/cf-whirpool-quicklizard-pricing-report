#IMAGEM
FROM python:3.10-slim-buster

#DIRETORIO DE TRABALHO
WORKDIR /app

#COPIA DAS DEPENDENCIAS
COPY requirements.txt .

#INSTALANDO AS DEPENDENCIAS
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

#COPIA DO PROJETO
COPY . .

#VARIAVEL DE AMBIENTE CONTENDO O ENTRYPOINT
ENV FUNCTION_TARGET=hello_gcs

#FUNÇÕES EXECUTADAS NA INICIALIZAÇÃO
CMD ["functions-framework", "--target", "hello_gcs", "--port", "8080"]
