FROM confluentinc/cp-kafka:7.2.1

# Переключаемся на root для выполнения команд
USER root

# Установка net-tools с помощью yum
RUN yum update -y && \
    yum install -y net-tools && \
    yum clean all

# Дополнительные команды (если есть)
COPY . /app
WORKDIR /app