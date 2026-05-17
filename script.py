import pandas as pd
import numpy as np
import logging
import os
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.metrics import classification_report
import joblib

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Список признаков
FEATURES = [
    'duration', 'protocol_type', 'service', 'flag', 'src_bytes', 'dst_bytes', 
    'land', 'wrong_fragment', 'urgent', 'hot', 'num_failed_logins', 'logged_in',
    'num_compromised', 'root_shell', 'su_attempted', 'num_root', 'num_file_creations',
    'num_shells', 'num_access_files', 'num_outbound_cmds', 'is_host_login', 
    'is_guest_login', 'count', 'srv_count', 'serror_rate', 'srv_serror_rate',
    'rerror_rate', 'srv_rerror_rate', 'same_srv_rate', 'diff_srv_rate',
    'srv_diff_host_rate', 'dst_host_count', 'dst_host_srv_count',
    'dst_host_same_srv_rate', 'dst_host_diff_srv_rate', 'dst_host_same_src_port_rate',
    'dst_host_srv_diff_host_rate', 'dst_host_serror_rate', 'dst_host_srv_serror_rate',
    'dst_host_rerror_rate', 'dst_host_srv_rerror_rate'
]

# Функция предобработки данных
def preprocess(data, is_train=True, keep_label=True):
    """Предобработка данных: кодирование категориальных признаков и масштабирование числовых."""
    cat_cols = ["protocol_type", "service", "flag"]
    num_cols = [col for col in FEATURES if col not in cat_cols]
    
    label = data["label"].copy() if keep_label and "label" in data.columns else None
    
    if is_train:
        encoder = OneHotEncoder(handle_unknown="ignore", sparse_output=False)
        encoded = pd.DataFrame(encoder.fit_transform(data[cat_cols]))
        encoded.columns = encoder.get_feature_names_out(cat_cols)
        joblib.dump(encoder, "models/encoder.pkl")
    else:
        encoder = joblib.load("models/encoder.pkl")
        try:
            encoded = pd.DataFrame(encoder.transform(data[cat_cols]))
            encoded.columns = encoder.get_feature_names_out(cat_cols)
        except ValueError as e:
            logger.error(f"Ошибка кодирования: {str(e)}")
            encoded = pd.DataFrame(0, index=range(len(data)), 
                                 columns=encoder.get_feature_names_out(cat_cols))
    
    data = data.drop(columns=cat_cols + ["difficulty"], errors='ignore')
    if not keep_label and "label" in data.columns:
        data = data.drop(columns=["label"])
    
    if is_train:
        scaler = StandardScaler()
        data[num_cols] = scaler.fit_transform(data[num_cols])
        joblib.dump(scaler, "models/scaler.pkl")
        feature_order = num_cols + list(encoded.columns)
        joblib.dump(feature_order, "models/feature_order.pkl")
    else:
        scaler = joblib.load("models/scaler.pkl")
        feature_order = joblib.load("models/feature_order.pkl")
        for col in num_cols:
            if col not in data.columns:
                data[col] = 0
        data[num_cols] = scaler.transform(data[num_cols])
    
    final_data = pd.concat([data[num_cols], encoded], axis=1)
    
    if not is_train:
        for col in feature_order:
            if col not in final_data.columns:
                final_data[col] = 0
    
    final_data = final_data[feature_order]
    
    if label is not None:
        final_data["label"] = label
        
    return final_data

# Обучение и сохранение модели
def train_and_save_model():
    """Обучение моделей Random Forest и Decision Tree на данных NSL-KDD."""
    # Проверка на наличие сохранённых моделей
    rf_model_path = "models/rf_model.pkl"
    dt_model_path = "models/dt_model.pkl"
    if os.path.exists(rf_model_path) and os.path.exists(dt_model_path):
        logger.info("Модели уже существуют, пропускаем обучение.")
        return

    logger.info("Загрузка NSL-KDD датасета...")
    TRAIN_FILE = "nsl-kdd/KDDTrain+.txt"
    df = pd.read_csv(TRAIN_FILE, names=FEATURES + ['label', 'difficulty'], sep=",", header=None)

    df.columns = df.columns.str.strip().astype(str)
    logger.info(f"Колонки после очистки: {df.columns.tolist()}")

    if "label" not in df.columns:
        raise ValueError("Колонка 'label' отсутствует!")

    # Очистка меток
    df["label"] = df["label"].str.strip(".").apply(lambda x: 0 if x == "normal" else 1)
    logger.info(f"Уникальные значения label после обработки: {df['label'].unique()}")

    processed_df = preprocess(df, is_train=True, keep_label=True)
    
    X = processed_df.drop(columns=["label"])
    y = processed_df["label"]

    logger.info(f"Количество признаков после предобработки: {X.shape[1]}")

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    logger.info("Обучение Random Forest...")
    rf_model = RandomForestClassifier(n_estimators=100)
    rf_model.fit(X_train, y_train)
    joblib.dump(rf_model, rf_model_path)

    logger.info("Обучение Decision Tree...")
    dt_model = DecisionTreeClassifier(max_depth=5)
    dt_model.fit(X_train, y_train)
    joblib.dump(dt_model, dt_model_path)

    logger.info("Оценка моделей...")
    rf_pred = rf_model.predict(X_test)
    dt_pred = dt_model.predict(X_test)

    logger.info("Random Forest:\n" + classification_report(y_test, rf_pred))
    logger.info("Decision Tree:\n" + classification_report(y_test, dt_pred))

if __name__ == "__main__":
    train_and_save_model()