# build image 

FROM apache/airflow:2.10.5-python3.10

# 仍用 root 来放文件/改权限
USER root
COPY --chown=airflow:0 requirements.txt /opt/airflow/requirements.txt
COPY entrypoint.sh /bootstrap.sh
RUN chmod +x /bootstrap.sh

# 切回 airflow 用户，以用户空间安装 Python 包
USER airflow
# Debian/Bookworm 的 pip 需要允许“非虚拟环境安装”
RUN python -m pip install --no-cache-dir -r /opt/airflow/requirements.txt

ENTRYPOINT ["/bootstrap.sh"]