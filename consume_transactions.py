from kafka import KafkaConsumer
import json

# Настройки потребителя
bootstrap_servers = 'localhost:9092'
topic = 'test-topic'
group_id = 'test-group'

# Создаём потребителя
consumer = KafkaConsumer(
    topic,
    bootstrap_servers=bootstrap_servers,
    group_id=group_id,
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Простая "модель" для обработки транзакций
def predict_suspicious(transaction):
    # Если сумма больше 50, считаем транзакцию подозрительной
    amount = transaction['amount']
    return amount > 50

# Читаем и обрабатываем сообщения
for message in consumer:
    transaction = message.value
    print(f"Received transaction: {transaction}")

    # Применяем модель
    is_suspicious = predict_suspicious(transaction)
    result = {
        'transaction_id': transaction['transaction_id'],
        'user_id': transaction['user_id'],
        'amount': transaction['amount'],
        'is_suspicious': is_suspicious
    }
    print(f"Processed result: {result}")