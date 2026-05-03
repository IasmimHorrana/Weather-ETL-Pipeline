# =============================================================
# Imagem customizada do Apache Airflow 2.10.5
# =============================================================
# Adicionamos apenas as libs que a imagem oficial não inclui.
# Versões pinadas para evitar conflitos com os providers do Airflow:
#
#   pandas   → providers Google/Snowflake exigem pandas<2.2, >=2.1.2
#   boto3    → aiobotocore exige botocore<1.36.4, o que limita boto3
#              Usamos a versão já presente na imagem (via --no-upgrade)
#
# O que instalamos:
#   - python-dotenv: não existe na imagem base
#   - tenacity:      a imagem tem 9.0.0, garantimos >=9.1.4
#   - pandas:        fixamos em 2.1.4 (compatível com todos os providers)
#   - boto3/botocore: não reinstalamos — já estão na imagem em versão
#                     compatível com o aiobotocore
# =============================================================

FROM apache/airflow:2.10.5-python3.12

RUN pip install --no-cache-dir \
    "python-dotenv>=1.2.2" \
    "tenacity>=9.1.4" \
    "pandas==2.1.4"
