import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from kmodes.kmodes import KModes
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import silhouette_score, calinski_harabasz_score

from df import getDF


def metodo_elbow_kmodes(df, lista_categoricos, max_clusters=10):
    """
    Aplica el método de Elbow para K-Modes.

    Parámetros:
    - df: DataFrame original
    - lista_categoricos: lista de variables categóricas
    - max_clusters: número máximo de clusters a evaluar

    Retorna:
    - Diccionario con costos de clustering
    """
    # Preprocesar datos
    df_processed, _ = preprocesar_datos_categoricos(df, lista_categoricos)

    # Almacenar costos
    costos = []
    k_valores = range(1, max_clusters + 1)

    # Calcular costos para diferentes números de clusters
    for k in k_valores:
        kmodes = KModes(n_clusters=k, init="Huang", n_init=5, random_state=42)
        kmodes.fit(df_processed[lista_categoricos])
        costos.append(kmodes.cost_)

    # Graficar método de Elbow
    plt.figure(figsize=(10, 5))
    plt.plot(k_valores, costos, "bx-")
    plt.xlabel("Número de clusters (k)")
    plt.ylabel("Costo de K-Modes")
    plt.title("Método de Elbow - K-Modes")
    plt.show()

    return {"k_valores": k_valores, "costos": costos}


def preprocesar_datos_categoricos(df, lista_categoricos):
    """
    Preprocesa datos categóricos para K-Modes usando LabelEncoder.

    Parámetros:
    - df: DataFrame original
    - lista_categoricos: lista de columnas categóricas

    Retorna:
    - DataFrame preprocesado
    - Lista de objetos de codificación
    """
    # Crear copia del DataFrame
    df_processed = df.copy()

    # Inicializar lista de encoders
    encoders = []

    # Codificar variables categóricas
    for columna in lista_categoricos:
        # Crear LabelEncoder para cada columna
        le = LabelEncoder()
        df_processed[columna] = le.fit_transform(df_processed[columna].astype(str))
        encoders.append(le)

    return df_processed, encoders


def evaluar_kmodes_clusters(df_processed, lista_categoricos, max_clusters=10):
    """
    Evalúa la calidad del clustering K-Modes para diferentes números de clusters.

    Parámetros:
    - df_processed: DataFrame preprocesado
    - lista_categoricos: lista de variables categóricas
    - max_clusters: número máximo de clusters a evaluar

    Retorna:
    - Diccionario con métricas de calidad para cada número de clusters
    """
    # Preparar diccionario para almacenar resultados
    metricas_clustering = {
        "n_clusters": [],
        "silhouette_score": [],
        "calinski_harabasz_score": [],
        "costo_kmodes": [],
    }

    # Evaluar clustering para diferentes números de clusters
    for n_clusters in range(2, max_clusters + 1):
        # Aplicar K-Modes
        kmodes = KModes(n_clusters=n_clusters, init="Huang", n_init=5, random_state=42)
        clusters = kmodes.fit_predict(df_processed[lista_categoricos])

        # Calcular métricas de calidad
        try:
            # Silhouette Score (con transformación a numérico)
            silhouette = silhouette_score(
                df_processed[lista_categoricos].apply(pd.to_numeric), clusters
            )

            # Calinski-Harabasz Score
            calinski = calinski_harabasz_score(
                df_processed[lista_categoricos].apply(pd.to_numeric), clusters
            )
        except Exception as e:
            print(f"Error calculando métricas para {n_clusters} clusters: {e}")
            silhouette = None
            calinski = None

        # Almacenar resultados
        metricas_clustering["n_clusters"].append(n_clusters)
        metricas_clustering["silhouette_score"].append(silhouette)
        metricas_clustering["calinski_harabasz_score"].append(calinski)
        metricas_clustering["costo_kmodes"].append(kmodes.cost_)

        print(f"Clusters: {n_clusters}")
        if silhouette is not None:
            print(f"Silhouette Score: {silhouette:.4f}")
            print(f"Calinski-Harabasz Score: {calinski:.4f}\n")

    # Visualizar resultados
    plt.figure(figsize=(15, 5))

    # Gráfico de Silhouette Score
    plt.subplot(1, 3, 1)
    plt.plot(
        metricas_clustering["n_clusters"],
        metricas_clustering["silhouette_score"],
        marker="o",
        color="green",
    )
    plt.title("Silhouette Score")
    plt.xlabel("Número de Clusters")
    plt.ylabel("Silhouette Score")

    # Gráfico de Calinski-Harabasz Score
    plt.subplot(1, 3, 2)
    plt.plot(
        metricas_clustering["n_clusters"],
        metricas_clustering["calinski_harabasz_score"],
        marker="o",
        color="red",
    )
    plt.title("Calinski-Harabasz Score")
    plt.xlabel("Número de Clusters")
    plt.ylabel("Calinski-Harabasz Score")

    plt.tight_layout()
    plt.show()

    return metricas_clustering


def seleccionar_mejor_numero_clusters(metricas):
    """
    Selecciona el mejor número de clusters basándose en las métricas.

    Estrategias:
    - Silhouette Score: Valor máximo
    - Calinski-Harabasz Score: Valor máximo
    """
    # Obtener índice del máximo Silhouette Score
    idx_silhouette = np.argmax(metricas["silhouette_score"])
    # Obtener índice del máximo Calinski-Harabasz Score
    idx_calinski = np.argmax(metricas["calinski_harabasz_score"])

    mejor_clusters_silhouette = metricas["n_clusters"][idx_silhouette]
    mejor_clusters_calinski = metricas["n_clusters"][idx_calinski]

    print("\nMejor número de clusters:")
    print(f"Según Silhouette Score: {mejor_clusters_silhouette}")
    print(f"Según Calinski-Harabasz Score: {mejor_clusters_calinski}")

    return mejor_clusters_silhouette


def aplicar_kmodes_clustering(df, lista_categoricos, n_clusters):
    """
    Aplica clustering K-Modes y agrega los clusters al DataFrame original.

    Parámetros:
    - df: DataFrame original
    - lista_categoricos: lista de variables categóricas
    - n_clusters: número de clusters

    Retorna:
    - DataFrame con columna de clusters
    - Modelo K-Modes
    - Lista de encoders
    """
    # Preprocesar datos
    df_processed, encoders = preprocesar_datos_categoricos(df, lista_categoricos)

    # Aplicar K-Modes
    kmodes = KModes(n_clusters=n_clusters, init="Huang", n_init=5, random_state=42)
    clusters = kmodes.fit_predict(df_processed[lista_categoricos])

    # Agregar columna de clusters al DataFrame original
    df_resultado = df.copy()
    df_resultado["Cluster"] = clusters

    return df_resultado, kmodes, encoders


def analizar_clusters_categoricos(df_resultado, lista_categoricos):
    """
    Analiza las características de los clusters categóricos.
    """
    # Análisis de variables categóricas
    print("\n=== Análisis de Variables Categóricas por Cluster ===")
    for cat in lista_categoricos:
        print(f"\nDistribución de {cat} por cluster:")
        tabla_distribucion = (
            pd.crosstab(df_resultado["Cluster"], df_resultado[cat], normalize="index")
            * 100
        )
        print(tabla_distribucion)

        # Visualización de la distribución
        plt.figure(figsize=(12, 6))
        tabla_distribucion.plot(kind="bar", stacked=True)
        plt.title(f"Distribución de {cat} por Cluster")
        plt.xlabel("Cluster")
        plt.ylabel("Porcentaje")
        plt.legend(title=cat, bbox_to_anchor=(1.05, 1), loc="upper left")
        plt.tight_layout()
        plt.show()


def ejecutar_analisis_kmodes(df, lista_categoricos):
    """
    Ejecuta análisis completo de clustering con K-Modes.
    """
    # Preprocesar datos
    df_processed, _ = preprocesar_datos_categoricos(df, lista_categoricos)

    # Evaluar clusters
    metricas = evaluar_kmodes_clusters(df_processed, lista_categoricos)

    # Seleccionar mejor número de clusters
    n_clusters = seleccionar_mejor_numero_clusters(metricas)

    # Aplicar clustering
    df_resultado, modelo, encoder = aplicar_kmodes_clustering(
        df, lista_categoricos, n_clusters
    )

    # Analizar resultados
    analizar_clusters_categoricos(df_resultado, lista_categoricos)

    return df_resultado, modelo, encoder


df = getDF()
cat_cols = [
    "anio",
    "es_finde",
    "horario",
    "tipo_ruta",
    "tipo_asentamiento",
    "estacion",
    "modo_produccion_hecho",
]
elbow_resultados = metodo_elbow_kmodes(df, cat_cols)
df_resultado, modelo, encoder = ejecutar_analisis_kmodes(df, cat_cols)
