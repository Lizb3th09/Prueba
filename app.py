import math
import pandas as pd
from flask import Flask, request, jsonify, render_template
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

# CONFIG
CSV_FILE = 'Data/gym.csv'

LAT_MIN = 31.7
LAT_MAX = 31.9
LON_MIN = -116.8
LON_MAX = -116.5

app = Flask(__name__)
df_maestro = pd.DataFrame()

# ---------------------- LIMITER ----------------------
limiter = Limiter(
    key_func=get_remote_address,
    default_limits=["20 per day"]  
)
limiter.init_app(app)

# ---------------------- ERROR 429 PERSONALIZADO ----------------------
@app.errorhandler(429)
def ratelimit_handler(e):
    return """
    <html>
        <head>
            <title>Límite alcanzado</title>
            <style>
                body {
                    background-color: #fff;
                    color: #333;
                    font-family: Arial, sans-serif;
                    text-align: center;
                    padding-top: 20%;
                }
                .sad-face {
                    font-size: 80px;
                }
                .message {
                    font-size: 24px;
                    margin-top: 20px;
                }
            </style>
        </head>
        <body>
            <div class="sad-face">😢</div>
            <div class="message">Ya no hay más peticiones disponibles por hoy</div>
        </body>
    </html>
    """, 429

# ---------------------- FUNCIONES ----------------------
def load_master_dataframe():
    """Carga el CSV en el DataFrame maestro y limpia datos inválidos."""
    try:
        df = pd.read_csv(CSV_FILE, encoding='utf-8', low_memory=False, dtype=str)
        df = df[(df['latitud'].notna()) & (df['longitud'].notna())]
        df = df.reset_index(drop=True)
        print(f"✅ DataFrame cargado. Total de gimnasios: {len(df)}")
        print("Servidor corriendo en http://localhost:5011")
        return df
    except FileNotFoundError:
        print(f"❌ Archivo no encontrado: {CSV_FILE}")
        return pd.DataFrame()
    except Exception as e:
        print(f"❌ Error cargando CSV: {e}")
        return pd.DataFrame()

# ---------------------- RUTAS ----------------------
@app.route('/')
@limiter.limit("20 per day")
def index():
    return render_template('index.html')

@app.route('/api/datos_negocios', methods=['GET'])
@limiter.limit("20 per day")
def api_datos_negocios():
    if df_maestro.empty:
        return jsonify([]), 200
    try:
        df_ensenada = df_maestro[
            (df_maestro['latitud'].astype(float) >= LAT_MIN) &
            (df_maestro['latitud'].astype(float) <= LAT_MAX) &
            (df_maestro['longitud'].astype(float) >= LON_MIN) &
            (df_maestro['longitud'].astype(float) <= LON_MAX)
        ]
        columnas_salida = ['nom_estab', 'latitud', 'longitud', 'telefono', 'correoelec', 'web']
        datos_respuesta = df_ensenada[columnas_salida].where(pd.notnull(df_ensenada[columnas_salida]), None)
        return jsonify(datos_respuesta.to_dict(orient='records')), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/excel/negocio/gimnasios', methods=['GET'])
@limiter.limit("20 per day")
def obtener_gimnasios():
    if df_maestro.empty:
        return jsonify([]), 200
    columnas_salida = ['nom_estab', 'latitud', 'longitud', 'telefono', 'correoelec', 'web']
    datos_respuesta = df_maestro[columnas_salida].where(pd.notnull(df_maestro[columnas_salida]), None)
    return jsonify(datos_respuesta.to_dict(orient='records')), 200

@app.route('/excel/negocio/ubicacion', methods=['GET'])
@limiter.limit("20 per day")
def gimnasios_por_ubicacion():
    if df_maestro.empty:
        return jsonify([]), 200
    try:
        lat = float(request.args.get('latitud'))
        lon = float(request.args.get('longitud'))
    except (TypeError, ValueError):
        return jsonify({"error": "Parámetros de latitud y longitud inválidos o faltantes"}), 400
    delta = 0.01
    df_filtrado = df_maestro[
        (df_maestro['latitud'].astype(float) >= lat - delta) &
        (df_maestro['latitud'].astype(float) <= lat + delta) &
        (df_maestro['longitud'].astype(float) >= lon - delta) &
        (df_maestro['longitud'].astype(float) <= lon + delta)
    ]
    return jsonify({
        "latitud": lat,
        "longitud": lon,
        "radio_grados": delta,
        "cantidad_gimnasios": len(df_filtrado)
    }), 200

@app.route('/excel/negocio/contacto', methods=['GET'])
@limiter.limit("20 per day")
def gimnasios_con_contacto():
    if df_maestro.empty:
        return jsonify([]), 200
    telefono = request.args.get('telefono')
    correoelec = request.args.get('correoelec')
    paginweb = request.args.get('paginweb')
    if not correoelec:
        return jsonify({"error": "Parámetro correoelec es requerido"}), 400
    df_filtrado = df_maestro[df_maestro['correoelec'].str.contains(correoelec, case=False, na=False)]
    if telefono:
        df_filtrado = df_filtrado[df_filtrado['telefono'].str.contains(telefono, case=False, na=False)]
    if paginweb:
        df_filtrado = df_filtrado[df_filtrado['web'].str.contains(paginweb, case=False, na=False)]
    columnas_salida = ['nom_estab', 'latitud', 'longitud', 'telefono', 'correoelec', 'web']
    datos_respuesta = df_filtrado[columnas_salida].where(pd.notnull(df_filtrado[columnas_salida]), None)
    return jsonify(datos_respuesta.to_dict(orient='records')), 200

@app.route('/excel/negocio/saturacion', methods=['GET'])
@limiter.limit("20 per day")
def gimnasios_por_saturacion():
    if df_maestro.empty:
        return jsonify([]), 200
    try:
        radio = float(request.args.get('radio'))
    except (TypeError, ValueError):
        return jsonify({"error": "Parámetro radio inválido o faltante"}), 400

    def haversine(lat1, lon1, lat2, lon2):
        radio_tierra = 6371
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return radio_tierra * c

    coords = df_maestro[['latitud', 'longitud']].astype(float).to_numpy()
    counts = [sum(haversine(lat1, lon1, lat2, lon2) <= radio for lat2, lon2 in coords) for lat1, lon1 in coords]
    df_maestro['gimnasios_cercanos'] = counts
    poco = df_maestro['gimnasios_cercanos'].quantile(0.25)
    mucho = df_maestro['gimnasios_cercanos'].quantile(0.75)

    def saturacion_label(x):
        if x <= poco: return 'poco'
        if x >= mucho: return 'mucho'
        return 'medio'

    df_maestro['saturacion'] = df_maestro['gimnasios_cercanos'].apply(saturacion_label)
    columnas_salida = ['nom_estab', 'latitud', 'longitud', 'telefono', 'correoelec', 'web', 'gimnasios_cercanos', 'saturacion']
    datos_respuesta = df_maestro[columnas_salida].where(pd.notnull(df_maestro[columnas_salida]), None)
    return jsonify(datos_respuesta.to_dict(orient='records')), 200

@app.route('/api/filtro', methods=['GET'])
@limiter.limit("20 per day")
def filtro_gimnasios():
    if df_maestro.empty:
        return jsonify([]), 200
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
    datos_respuesta = df_filtrado[columnas_salida].where(pd.notnull(df_filtrado[columnas_salida]), None)
    return jsonify(datos_respuesta.to_dict(orient='records')), 200

# ---------------------- RUN ----------------------
if __name__ == '__main__':
    df_maestro = load_master_dataframe()
    app.run(debug=True, port=5011)
