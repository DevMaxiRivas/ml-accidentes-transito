# accidente_id,
# tipo_persona = (Victima o Imputado),
# mes,
# fecha_hecho = (YYYY-MM-DD),
# hora_hecho = (HH:MM),
# calle_interseccion = (Si o No),
# semaforo_estado = (Sin semáforo, Sin determinar, Funcionaba),
# tipo_lugar = (Ruta Nacional, Ruta Provincial, Calle, Autovia),
# modo_produccion_hecho = (Colisión vehículo-objeto, Colisión vehículo-vehículo, Vuelco-Despistes),
# clima_condicion = (Bueno, Sin determinar, Nublado, Lluvia, Llovizna),
# victima_sexo = (M o F),
# victima_tr_edad = ([0-17], [18-40], [40-65], [65-90]),
# victima_vehiculo = (Motocicleta, Automóvil, Peatón, Camioneta, Bicicleta, Camión),
# inculpado_sexo = (M o F),
# inculpado_tr_edad = ([0-17], [18-40], [40-65], [65-90]),
# inculpado_vehiculo = (Automóvil, Camión, Camioneta, Motocicleta),

"""
Agrega una columna "estacion" utilizando "fecha_hecho"
Considerando las siguientes fechas:

Verano (21 diciembre – 20 marzo)
Otoño (21 marzo – 20 junio)
Invierno (21 junio – 20 septiembre)
Primavera (21 septiembre – 20 diciembre)

Agrega una columna de tipo booleana "es_finde" utilizando "fecha_hecho"
Que sea verdadera si el dia de la fecha es Sabado o Domingo


Agrega una columna "horario" utilizando "hora_hecho"
Considerando las siguientes horarios:

madrugada (00:00 a 06:00)
mañana (06:00-12:00)
tarde (12:00-20:00)
noche (20:00-00:00)
"""


import pandas as pd
import numpy as np

# Asumiendo que tu dataset se llama 'df'
# df = pd.read_csv('accidentes_raw.csv')

# ============================================
# 1. AGREGACIÓN DE VARIABLES DEL ACCIDENTE
# ============================================
# Estas variables son iguales para todos los involucrados del mismo accidente
# Tomamos el primer valor (asumimos que no hay inconsistencias)

accidente_vars = (
    df.groupby("accidente_id")
    .agg(
        {
            "mes": "first",
            "fecha_hecho": "first",
            "hora_hecho": "first",
            "calle_interseccion": "first",
            "semaforo_estado": "first",
            "tipo_lugar": "first",
            "modo_produccion_hecho": "first",
            "clima_condicion": "first",
        }
    )
    .reset_index()
)

# ============================================
# 2. AGREGACIÓN DE VÍCTIMAS
# ============================================

# Primero filtramos solo las víctimas
victimas = df[df["tipo_persona"] == "Victima"].copy()

# Conteo total de víctimas por accidente
victimas_count = (
    victimas.groupby("accidente_id").size().reset_index(name="num_victimas")
)

# Conteo por sexo
victimas_sexo = pd.crosstab(
    victimas["accidente_id"], victimas["victima_sexo"]
).reset_index()
victimas_sexo.columns = ["accidente_id", "victimas_sexo_F", "victimas_sexo_M"]
# Si solo hay un sexo, puede que falte la columna
if "victimas_sexo_F" not in victimas_sexo.columns:
    victimas_sexo["victimas_sexo_F"] = 0
if "victimas_sexo_M" not in victimas_sexo.columns:
    victimas_sexo["victimas_sexo_M"] = 0

# Conteo por rango etario
victimas_edad = pd.crosstab(
    victimas["accidente_id"], victimas["victima_tr_edad"]
).reset_index()
victimas_edad.columns = [
    "accidente_id",
    "victimas_edad_0_17",
    "victimas_edad_18_40",
    "victimas_edad_40_65",
    "victimas_edad_65_90",
]
# Rellenar columnas faltantes con 0
for col in [
    "victimas_edad_0_17",
    "victimas_edad_18_40",
    "victimas_edad_40_65",
    "victimas_edad_65_90",
]:
    if col not in victimas_edad.columns:
        victimas_edad[col] = 0

# Conteo por tipo de vehículo
victimas_vehiculo = pd.crosstab(
    victimas["accidente_id"], victimas["victima_vehiculo"]
).reset_index()
victimas_vehiculo.columns = ["accidente_id"] + [
    f"victimas_vehiculo_{col}" for col in victimas_vehiculo.columns[1:]
]

# ============================================
# 3. AGREGACIÓN DE INCULPADOS
# ============================================

# Filtramos solo los inculpados
inculpados = df[df["tipo_persona"] == "Inculpado"].copy()

# Conteo total de inculpados por accidente
inculpados_count = (
    inculpados.groupby("accidente_id").size().reset_index(name="num_inculpados")
)

# Conteo por sexo
inculpados_sexo = pd.crosstab(
    inculpados["accidente_id"], inculpados["inculpado_sexo"]
).reset_index()
inculpados_sexo.columns = ["accidente_id", "inculpados_sexo_F", "inculpados_sexo_M"]
if "inculpados_sexo_F" not in inculpados_sexo.columns:
    inculpados_sexo["inculpados_sexo_F"] = 0
if "inculpados_sexo_M" not in inculpados_sexo.columns:
    inculpados_sexo["inculpados_sexo_M"] = 0

# Conteo por rango etario
inculpados_edad = pd.crosstab(
    inculpados["accidente_id"], inculpados["inculpado_tr_edad"]
).reset_index()
inculpados_edad.columns = [
    "accidente_id",
    "inculpados_edad_0_17",
    "inculpados_edad_18_40",
    "inculpados_edad_40_65",
    "inculpados_edad_65_90",
]
for col in [
    "inculpados_edad_0_17",
    "inculpados_edad_18_40",
    "inculpados_edad_40_65",
    "inculpados_edad_65_90",
]:
    if col not in inculpados_edad.columns:
        inculpados_edad[col] = 0

# Conteo por tipo de vehículo
inculpados_vehiculo = pd.crosstab(
    inculpados["accidente_id"], inculpados["inculpado_vehiculo"]
).reset_index()
inculpados_vehiculo.columns = ["accidente_id"] + [
    f"inculpados_vehiculo_{col}" for col in inculpados_vehiculo.columns[1:]
]

# ============================================
# 4. UNIFICAR TODAS LAS AGRUPACIONES
# ============================================

# Empezamos con las variables del accidente
df_accidentes = accidente_vars.copy()

# Unimos conteos de víctimas
df_accidentes = df_accidentes.merge(
    victimas_count, on="accidente_id", how="left"
).fillna({"num_victimas": 0})
df_accidentes = df_accidentes.merge(
    victimas_sexo, on="accidente_id", how="left"
).fillna({"victimas_sexo_F": 0, "victimas_sexo_M": 0})
df_accidentes = df_accidentes.merge(
    victimas_edad, on="accidente_id", how="left"
).fillna(
    {
        "victimas_edad_0_17": 0,
        "victimas_edad_18_40": 0,
        "victimas_edad_40_65": 0,
        "victimas_edad_65_90": 0,
    }
)
df_accidentes = df_accidentes.merge(
    victimas_vehiculo, on="accidente_id", how="left"
).fillna(0)

# Unimos conteos de inculpados
df_accidentes = df_accidentes.merge(
    inculpados_count, on="accidente_id", how="left"
).fillna({"num_inculpados": 0})
df_accidentes = df_accidentes.merge(
    inculpados_sexo, on="accidente_id", how="left"
).fillna({"inculpados_sexo_F": 0, "inculpados_sexo_M": 0})
df_accidentes = df_accidentes.merge(
    inculpados_edad, on="accidente_id", how="left"
).fillna(
    {
        "inculpados_edad_0_17": 0,
        "inculpados_edad_18_40": 0,
        "inculpados_edad_40_65": 0,
        "inculpados_edad_65_90": 0,
    }
)
df_accidentes = df_accidentes.merge(
    inculpados_vehiculo, on="accidente_id", how="left"
).fillna(0)

# Convertir columnas numéricas a int (los fillna deja float)
num_cols = df_accidentes.select_dtypes(include=[np.float64]).columns
df_accidentes[num_cols] = df_accidentes[num_cols].astype(int)

# ============================================
# 5. CREAR VARIABLES ADICIONALES (Feature Engineering)
# ============================================

# Convertir fecha a datetime si no lo está
df_accidentes["fecha_hecho"] = pd.to_datetime(df_accidentes["fecha_hecho"])

# Extraer características de fecha
df_accidentes["dia_semana"] = df_accidentes[
    "fecha_hecho"
].dt.dayofweek  # 0=lunes, 6=domingo
df_accidentes["es_finde"] = df_accidentes["dia_semana"].apply(
    lambda x: 1 if x >= 5 else 0
)  # sábado/domingo
df_accidentes["mes_num"] = df_accidentes["fecha_hecho"].dt.month


# Procesar hora: convertir a número (horas desde medianoche)
def hora_a_numero(hora_str):
    try:
        h, m = map(int, hora_str.split(":"))
        return h + m / 60
    except:
        return np.nan


df_accidentes["hora_num"] = df_accidentes["hora_hecho"].apply(hora_a_numero)


# Crear rangos horarios
def rango_horario(hora):
    if pd.isna(hora):
        return "desconocido"
    if hora < 6:
        return "madrugada"
    elif hora < 12:
        return "mañana"
    elif hora < 18:
        return "tarde"
    else:
        return "noche"


df_accidentes["rango_horario"] = df_accidentes["hora_num"].apply(rango_horario)

# Variable: total de personas involucradas
df_accidentes["total_involucrados"] = (
    df_accidentes["num_victimas"] + df_accidentes["num_inculpados"]
)

# Variable: proporción de víctimas sobre total
df_accidentes["proporcion_victimas"] = df_accidentes["num_victimas"] / df_accidentes[
    "total_involucrados"
].replace(0, 1)

# ============================================
# 6. SELECCIONAR Y ORDENAR COLUMNAS FINALES
# ============================================

# Definir orden de columnas para mejor legibilidad
columnas_orden = [
    # Identificación
    "accidente_id",
    # Temporales
    "fecha_hecho",
    "mes",
    "mes_num",
    "dia_semana",
    "es_finde",
    "hora_hecho",
    "hora_num",
    "rango_horario",
    # Lugar y condiciones
    "calle_interseccion",
    "semaforo_estado",
    "tipo_lugar",
    "modo_produccion_hecho",
    "clima_condicion",
    # Agregaciones víctimas
    "num_victimas",
    "proporcion_victimas",
    "victimas_sexo_M",
    "victimas_sexo_F",
    "victimas_edad_0_17",
    "victimas_edad_18_40",
    "victimas_edad_40_65",
    "victimas_edad_65_90",
    *[col for col in df_accidentes.columns if col.startswith("victimas_vehiculo_")],
    # Agregaciones inculpados
    "num_inculpados",
    "inculpados_sexo_M",
    "inculpados_sexo_F",
    "inculpados_edad_0_17",
    "inculpados_edad_18_40",
    "inculpados_edad_40_65",
    "inculpados_edad_65_90",
    *[col for col in df_accidentes.columns if col.startswith("inculpados_vehiculo_")],
    # Totales
    "total_involucrados",
]

# Reordenar (solo las que existen)
columnas_finales = [col for col in columnas_orden if col in df_accidentes.columns]
df_accidentes = df_accidentes[columnas_finales]

# ============================================
# 7. VERIFICAR RESULTADOS
# ============================================

print(f"Filas originales: {len(df)}")
print(f"Filas después de agregación (accidentes únicos): {len(df_accidentes)}")
print("\nPrimeras filas del dataset agregado:")
print(df_accidentes.head())
print("\nInformación del dataset:")
print(df_accidentes.info())

# Guardar resultado
# df_accidentes.to_csv('accidentes_agregado.csv', index=False)
