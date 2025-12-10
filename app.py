import math  # estándar
import pandas as pd  # terceros
from flask import Flask, request, jsonify, render_template  # terceros

# CONFIG
CSV_FILE = 'data/db-ens-RL(Hoja1).csv'

LAT_MIN = 31.7
LAT_MAX = 31.9
LON_MIN = -116.8
LON_MAX = -116.5

app = Flask(__name__)
df_maestro = pd.DataFrame()


def load_master_dataframe():
    """Carga el CSV en el DataFrame maestro y limpia datos inválidos."""
    try:
        df = pd.read_csv(CSV_FILE, encoding='latin1')

        # Limpiar coordenadas inválidas
        df = df[(df['latitud'].notna()) & (df['longitud'].notna())]

        # Resetear índice
        df = df.reset_index(drop=True)
        print(f"✅ DataFrame cargado. Total de gimnasios: {len(df)}")
        print("Servidor corriendo en http://localhost:5000")
        return df
    except FileNotFoundError:
        print(f"❌ Archivo no encontrado: {CSV_FILE}")
        return pd.DataFrame()
    except Exception as e:
        print(f"❌ Error cargando CSV: {e}")
        return pd.DataFrame()


@app.route('/')
def index():
    """Renderiza la página principal."""
    return render_template('index.html')


@app.route('/api/datos_negocios', methods=['GET'])
def api_datos_negocios():
    """Devuelve solo gimnasios dentro de Ensenada."""
    if df_maestro.empty:
        return jsonify({"error": "Datos no cargados"}), 500

    df_ensenada = df_maestro[
        (df_maestro['latitud'] >= LAT_MIN) &
        (df_maestro['latitud'] <= LAT_MAX) &
        (df_maestro['longitud'] >= LON_MIN) &
        (df_maestro['longitud'] <= LON_MAX)
    ]

    columnas_salida = ['nom_estab', 'latitud', 'longitud', 'telefono', 'correoelec', 'web']
    datos_respuesta = df_ensenada[columnas_salida].where(
        pd.notnull(df_ensenada[columnas_salida]), None
    )

    return jsonify(datos_respuesta.to_dict(orient='records')), 200


@app.route('/excel/negocio/gimnasios', methods=['GET'])
def obtener_gimnasios():
    """Devuelve todos los gimnasios."""
    if df_maestro.empty:
        return jsonify({"error": "Datos no cargados"}), 500

    columnas_salida = ['nom_estab', 'latitud', 'longitud', 'telefono', 'correoelec', 'web']
    datos_respuesta = df_maestro[columnas_salida].where(
        pd.notnull(df_maestro[columnas_salida]), None
    )
    return jsonify(datos_respuesta.to_dict(orient='records')), 200


@app.route('/excel/negocio/ubicacion', methods=['GET'])
def gimnasios_por_ubicacion():
    """Cuenta gimnasios cercanos a una coordenada dada."""
    if df_maestro.empty:
        return jsonify({"error": "Datos no cargados"}), 500

    try:
        lat = float(request.args.get('latitud'))
        lon = float(request.args.get('longitud'))
    except (TypeError, ValueError):
        return jsonify({"error": "Parámetros de latitud y longitud inválidos o faltantes"}), 400

    delta = 0.01
    df_filtrado = df_maestro[
        (df_maestro['latitud'] >= lat - delta) &
        (df_maestro['latitud'] <= lat + delta) &
        (df_maestro['longitud'] >= lon - delta) &
        (df_maestro['longitud'] <= lon + delta)
    ]

    return jsonify({
        "latitud": lat,
        "longitud": lon,
        "radio_grados": delta,
        "cantidad_gimnasios": len(df_filtrado)
    }), 200


@app.route('/excel/negocio/contacto', methods=['GET'])
def gimnasios_con_contacto():
    """Filtra gimnasios por correo, teléfono o web."""
    if df_maestro.empty:
        return jsonify({"error": "Datos no cargados"}), 500

    telefono = request.args.get('telefono')
    correoelec = request.args.get('correoelec')
    paginweb = request.args.get('paginweb')

    if not correoelec:
        return jsonify({"error": "Parámetro correoelec es requerido"}), 400

    df_filtrado = df_maestro[df_maestro['correoelec'].str.contains(
        correoelec, case=False, na=False
    )]

    if telefono:
        df_filtrado = df_filtrado[df_filtrado['telefono'].str.contains(
            telefono, case=False, na=False
        )]
    if paginweb:
        df_filtrado = df_filtrado[df_filtrado['web'].str.contains(
            paginweb, case=False, na=False
        )]

    columnas_salida = ['nom_estab', 'latitud', 'longitud', 'telefono', 'correoelec', 'web']
    datos_respuesta = df_filtrado[columnas_salida].where(
        pd.notnull(df_filtrado[columnas_salida]), None
    )

    return jsonify(datos_respuesta.to_dict(orient='records')), 200


@app.route('/excel/negocio/saturacion', methods=['GET'])
def gimnasios_por_saturacion():
    """Calcula saturación de gimnasios según proximidad."""
    if df_maestro.empty:
        return jsonify({"error": "Datos no cargados"}), 500

    try:
        radio = float(request.args.get('radio'))
    except (TypeError, ValueError):
        return jsonify({"error": "Parámetro radio inválido o faltante"}), 400

    def haversine(lat1, lon1, lat2, lon2):
        """Calcula distancia Haversine entre dos coordenadas en km."""
        radio_tierra = 6371
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return radio_tierra * c

    coords = df_maestro[['latitud', 'longitud']].to_numpy()
    counts = [sum(haversine(lat1, lon1, lat2, lon2) <= radio for lat2, lon2 in coords) for lat1, lon1 in coords]

    df_maestro['gimnasios_cercanos'] = counts

    poco = df_maestro['gimnasios_cercanos'].quantile(0.25)
    mucho = df_maestro['gimnasios_cercanos'].quantile(0.75)

    def saturacion_label(x):
        if x <= poco:
            return 'poco'
        if x >= mucho:
            return 'mucho'
        return 'medio'

    df_maestro['saturacion'] = df_maestro['gimnasios_cercanos'].apply(saturacion_label)

    columnas_salida = ['nom_estab', 'latitud', 'longitud', 'telefono', 'correoelec', 'web', 'gimnasios_cercanos', 'saturacion']
    datos_respuesta = df_maestro[columnas_salida].where(
        pd.notnull(df_maestro[columnas_salida]), None
    )

    return jsonify(datos_respuesta.to_dict(orient='records')), 200


@app.route('/api/filtro', methods=['GET'])
def filtro_gimnasios():
    """Filtra gimnasios por tipo: correo, telefono, web o saturación."""
    if df_maestro.empty:
        return jsonify({"error": "Datos no cargados"}), 500

    tipo = request.args.get('tipo')
    df_filtrado = df_maestro.copy()

    if tipo == 'correo':
        df_filtrado = df_filtrado[df_filtrado['correoelec'].notna() & (df_filtrado['correoelec'] != '')]
    elif tipo == 'telefono':
        df_filtrado = df_filtrado[df_filtrado['telefono'].notna() & (df_filtrado['telefono'] != '')]
    elif tipo == 'web':
        df_filtrado = df_filtrado[df_filtrado['web'].notna() & (df_filtrado['web'] != '')]
    elif tipo == 'saturacion_mucho':
        if 'saturacion' not in df_filtrado.columns:
            return jsonify({"error": "Calcula saturación primero usando /excel/negocio/saturacion"}), 400
        df_filtrado = df_filtrado[df_filtrado['saturacion'] == 'mucho']
    elif tipo == 'saturacion_poco':
        if 'saturacion' not in df_filtrado.columns:
            return jsonify({"error": "Calcula saturación primero usando /excel/negocio/saturacion"}), 400
        df_filtrado = df_filtrado[df_filtrado['saturacion'] == 'poco']
    else:
        return jsonify({"error": "Tipo de filtro inválido"}), 400

    columnas_salida = ['nom_estab', 'latitud', 'longitud', 'telefono', 'correoelec', 'web', 'saturacion']
    datos_respuesta = df_filtrado[columnas_salida].where(
        pd.notnull(df_filtrado[columnas_salida]), None
    )

    return jsonify(datos_respuesta.to_dict(orient='records')), 200


# Correr programa
if __name__ == '__main__':
    df_maestro = load_master_dataframe()
    app.run(debug=True)
