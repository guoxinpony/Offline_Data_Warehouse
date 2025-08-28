# FROM apache/superset:fc80861

# USER root

# # for ubuntu/debian based Linux, install mysql_config/pkg-config before pip install mysqlclient
# RUN apt-get update \
#     && apt-get install -y --no-install-recommends \
#        build-essential \
#        libmariadb-dev-compat \
#        libmariadb-dev \
#        pkg-config \
#        curl \
#     && rm -rf /var/lib/apt/lists/*

# USER superset
# RUN pip install --no-cache-dir mysqlclient pinotdb psycopg2-binary

# # validate installed mysqlclient pinotdb psycopg2-binary
# RUN python -c "import psycopg2, MySQLdb, pinotdb; print('deps ok')"

# ENV ADMIN_USERNAME=$ADMIN_USERNAME
# ENV ADMIN_EMAIL=$ADMIN_EMAIL
# ENV ADMIN_PASSWORD=$ADMIN_PASSWORD

# COPY ./superset-init.sh /superset-init.sh

# COPY superset_config.py /app/
# ENV SUPERSET_CONFIG_PATH=/app/superset_config.py

# RUN superset version

# ENTRYPOINT [ "/superset-init.sh" ]


FROM apache/superset:fc80861

USER root

# 1) 系统依赖 + 为了能使用 ensurepip/pip，安装 python3-venv & python3-pip
# 安装并验证依赖（单个 RUN，使用 && 连接）
RUN set -e 

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      build-essential \
      libmariadb-dev \
      libmariadb-dev-compat \
      pkg-config \
      curl \
      python3-venv \
      python3-pip && \
    rm -rf /var/lib/apt/lists/*

# 2) 用 Superset 实际的 Python 解释器安装依赖（整段都在一个 RUN 里）
RUN PY_BIN="$(head -n1 "$(command -v superset)" | sed 's/^#!//')" && \
    echo "Using interpreter: $PY_BIN" && \
    "$PY_BIN" -m ensurepip --upgrade || true && \
    "$PY_BIN" -m pip install --upgrade pip setuptools wheel && \
    "$PY_BIN" -m pip install --no-cache-dir mysqlclient pinotdb psycopg2-binary && \
    "$PY_BIN" -c "import sys, psycopg2, MySQLdb, pinotdb; print('deps ok for', sys.executable)"


# 3) 你的文件与配置
COPY superset_config.py /app/
ENV SUPERSET_CONFIG_PATH=/app/superset_config.py
COPY ./superset-init.sh /superset-init.sh

# （可选）构建期打印版本
RUN SUP_BIN="$(command -v superset)" && echo "Superset CLI: $SUP_BIN" && ( "$SUP_BIN" version || "$SUP_BIN" --version )


USER superset
ENTRYPOINT ["/superset-init.sh"]
