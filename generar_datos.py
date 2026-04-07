import pandas as pd
from sklearn.datasets import make_classification
import os

# Definir la ruta donde se guardará el archivo (apuntando a la carpeta que acabamos de crear)
ruta_archivo = './data/datos_entrenamiento.csv'

# Generar 1000 filas de datos simulados con 5 columnas de características (features)
X, y = make_classification(
    n_samples=1000, 
    n_features=5, 
    n_informative=3, 
    n_redundant=0, 
    random_state=42, 
    n_classes=2 # Problema binario: predecir 0 o 1
)

# Convertir a un DataFrame de pandas para que sea fácil de manejar
df = pd.DataFrame(X, columns=['feature_1', 'feature_2', 'feature_3', 'feature_4', 'feature_5'])
df['target'] = y # Nuestra variable a predecir

# Guardar como CSV
df.to_csv(ruta_archivo, index=False)

print(f"Archivo generado en: {os.path.abspath(ruta_archivo)}")