FROM apache/spark:3.5.7

USER root

# Проверяем пакетный менеджер и ставим bash + coreutils
RUN if command -v apt-get >/dev/null 2>&1; then \
        apt-get update && \
        apt-get install -y --no-install-recommends bash coreutils && \
        rm -rf /var/lib/apt/lists/* ; \
    elif command -v yum >/dev/null 2>&1; then \
        yum install -y bash coreutils && \
        yum clean all ; \
    else \
        echo "No supported package manager found" && exit 1 ; \
    fi

USER spark
