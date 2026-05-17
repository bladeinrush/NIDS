import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.preprocessing import OneHotEncoder, StandardScaler
import joblib
import os

# Проверка существования директорий
def ensure_directory_exists(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)
        print(f"Создана директория: {directory}")

# Загрузка данных с явными именами колонок
column_names = [
    "duration", "protocol_type", "service", "flag", "src_bytes", "dst_bytes",
    "land", "wrong_fragment", "urgent", "hot", "num_failed_logins", "logged_in",
    "num_compromised", "root_shell", "su_attempted", "num_root", "num_file_creations",
    "num_shells", "num_access_files", "num_outbound_cmds", "is_host_login",
    "is_guest_login", "count", "srv_count", "serror_rate", "srv_serror_rate",
    "rerror_rate", "srv_rerror_rate", "same_srv_rate", "diff_srv_rate",
    "srv_diff_host_rate", "dst_host_count", "dst_host_srv_count",
    "dst_host_same_srv_rate", "dst_host_diff_srv_rate", "dst_host_same_src_port_rate",
    "dst_host_srv_diff_host_rate", "dst_host_serror_rate", "dst_host_srv_serror_rate",
    "dst_host_rerror_rate", "dst_host_srv_rerror_rate", "class", "extra"
]
try:
    data = pd.read_csv(r"C:\Users\stakh\Downloads\IDS\IDS\nsl-kdd\KDDTrain+.txt", names=column_names)
    print("Данные успешно загружены. Размер:", data.shape)
except FileNotFoundError:
    print("Ошибка: файл 'train_data.csv' не найден.")
    exit(1)
except Exception as e:
    print(f"Ошибка при загрузке данных: {e}")
    exit(1)

# Разделение на признаки и целевую переменную
try:
    X = data.drop(["class", "extra"], axis=1)
    y = data["class"]
    print("Признаки и целевая переменная разделены. X:", X.shape, "y:", y.shape)
except KeyError:
    print("Ошибка: колонка 'class' не найдена. Проверь имена колонок.")
    print("Доступные колонки:", data.columns.tolist())
    exit(1)

# Категориальные и числовые признаки
cat_cols = ["protocol_type", "service", "flag"]
num_cols = [col for col in X.columns if col not in cat_cols]

# Проверка наличия категориальных колонок
missing_cols = [col for col in cat_cols if col not in X.columns]
if missing_cols:
    print(f"Ошибка: следующие категориальные колонки отсутствуют: {missing_cols}")
    exit(1)
print(f"Числовые колонки: {len(num_cols)}, Категориальные колонки: {cat_cols}")

# Создание и обучение нового OneHotEncoder
try:
    encoder = OneHotEncoder(sparse_output=False, handle_unknown='ignore')
    encoder.fit(X[cat_cols])
    encoded = pd.DataFrame(
        encoder.transform(X[cat_cols]),
        columns=encoder.get_feature_names_out(cat_cols)
    )
    print("Категориальные признаки закодированы. Размер:", encoded.shape)
except Exception as e:
    print(f"Ошибка в кодировании: {e}")
    exit(1)

# Масштабирование числовых признаков
scaler = StandardScaler()
try:
    scaled_num = scaler.fit_transform(X[num_cols])
    X_num = pd.DataFrame(scaled_num, columns=num_cols)
    print("Числовые признаки отмасштабированы. Размер:", X_num.shape)
except Exception as e:
    print(f"Ошибка при масштабировании: {e}")
    exit(1)

# Объединение признаков
try:
    X_final = pd.concat([X_num, encoded], axis=1)
    feature_order = X_final.columns.tolist()
    print("Все признаки объединены. Итоговый размер:", X_final.shape)
    print(f"Количество признаков: {len(feature_order)}")
except Exception as e:
    print(f"Ошибка при объединении признаков: {e}")
    exit(1)

# Обучение моделей
rf_model = RandomForestClassifier(random_state=42)
dt_model = DecisionTreeClassifier(random_state=42)

try:
    rf_model.fit(X_final, y)
    print("RandomForestClassifier обучен")
except Exception as e:
    print(f"Ошибка при обучении RandomForest: {e}")
    exit(1)

try:
    dt_model.fit(X_final, y)
    print("DecisionTreeClassifier обучен")
except Exception as e:
    print(f"Ошибка при обучении DecisionTree: {e}")
    exit(1)

# Сохранение моделей и предобработчиков


try:
    joblib.dump(rf_model, f"models/rf_model_new.pkl")
    joblib.dump(dt_model, f"models/dt_model_new.pkl")
    joblib.dump(scaler, f"models/scaler_new.pkl")
    joblib.dump(encoder, f"models/encoder_new.pkl")
    joblib.dump(feature_order, f"models/feature_order_new.pkl")
    print(f"Модели и предобработчики сохранены в models:")
    print(f"- rf_model_new.pkl")
    print(f"- dt_model_new.pkl")
    print(f"- scaler_new.pkl")
    print(f"- encoder_new.pkl")
    print(f"- feature_order_new.pkl (содержит {len(feature_order)} признаков)")
except Exception as e:
    print(f"Ошибка при сохранении файлов: {e}")
    exit(1)

print("Обучение и сохранение завершены успешно!")