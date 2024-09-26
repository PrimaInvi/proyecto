from flask import Flask, Blueprint, render_template, g
import mysql.connector
import plotly.express as px
import plotly.io as pio
from plotly.io import to_json 
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn import datasets
import numpy as np
import json, os



app = Flask(__name__)
plot_bp = Blueprint('plot', __name__)


def get_db_connection():
      if 'db' not in g:
        g.db = mysql.connector.connect(user='root', password='', 
                                   host='localhost',
                                   database='estemovil',
                                   port='3306')
        return g.db
@app.teardown_appcontext
def close_db_connection(exception):
        db = g.pop('db', None)
        if db is not None:
             db.close()


@app.route('/')
def index():

        conexion = get_db_connection()
        cursor = conexion.cursor()
        cursor.execute("select * from recargas")
        consulta = cursor.fetchall()
        return render_template('index.html', datos=consulta)


@app.route('/activaciones')
def activaciones():

        conexion = get_db_connection()
        cursor = conexion.cursor()
        cursor.execute("select * from activaciones")
        consulta = cursor.fetchall()
        return render_template('tabla_activaciones.html', datos=consulta)


@app.route('/vencimientos')
def vencimientos():

        conexion = get_db_connection()
        cursor = conexion.cursor()
        cursor.execute("select * from vencimientoex")
        consulta = cursor.fetchall()
        return render_template('vencimientoex.html', datos=consulta)


# TRANSFORMACIONES
@app.route('/reactivacion')
def reactivacion():

        conexion = get_db_connection()
        cursor = conexion.cursor()
        cursor.execute("""
        SELECT activaciones.msisdn, vencimientoex.fecha, vencimientoex.hora,
                activaciones.ccpdv, activaciones.idpdv, activaciones.nombrepdv, activaciones.estado, activaciones.vence
        FROM vencimientoex
        JOIN activaciones ON vencimientoex.numero = activaciones.msisdn;
        """)
        consulta = cursor.fetchall()
        return render_template('reactivacion.html', datos=consulta)


@app.route('/registros')
def registros():

        conexion = get_db_connection()
        cursor = conexion.cursor()
        cursor.execute("""
        SELECT activaciones.ccpdv, activaciones.idpdv, activaciones.nombrepdv, COUNT(activaciones.msisdn) AS total_msisdn
                FROM activaciones
        GROUP BY activaciones.idpdv, activaciones.ccpdv, activaciones.nombrepdv; 
        """)
        consulta = cursor.fetchall()
        return render_template('registros.html', datos=consulta)


@app.route('/paquetex')
def paquetex():

        conexion = get_db_connection()
        cursor = conexion.cursor()
        cursor.execute("""
        SELECT vencimientoex.fecha, vencimientoex.hora, vencimientoex.numero, vencimientoex.valor,
                recargas.paquete
        FROM vencimientoex
        JOIN recargas ON vencimientoex.numero = recargas.msisdn_linea;
        """)
        consulta = cursor.fetchall()
        return render_template('paqueteex.html', datos=consulta)


@plot_bp.route('/plot')
def plot():
    # Conexión a la base de datos y obtención datos de activaciones por punto de venta
    conexion = get_db_connection()
    cursor = conexion.cursor()

    # Consulta para contar las activaciones por punto de venta
    cursor.execute("""
    SELECT idpdv, COUNT(msisdn) as total_activaciones 
    FROM activaciones 
    GROUP BY idpdv
    HAVING COUNT(msisdn) > 30
    """)
    data = cursor.fetchall()

    # Convertir los datos a un DataFrame de Pandas
    df_pie = pd.DataFrame(data, columns=['Punto de Venta', 'Activaciones'])

    # Crear un gráfico de pastel con Plotly
    fig_pie = px.pie(df_pie, names='Punto de Venta', values='Activaciones', 
                     title='Porcentaje de Activaciones por Punto de Venta')
    graph_html_pie = pio.to_html(fig_pie, full_html=False)

    cursor.close()
    conexion.close()

    
    return render_template('plot.html', graph_html_pie=graph_html_pie)
app.register_blueprint(plot_bp)


@app.route('/predicciones')
def predicciones():
    # Obtener los datos de activaciones por punto de venta
    conexion = get_db_connection()
    cursor = conexion.cursor()

    # Consulta para obtener el número de SIM cards vendidas por mes por punto de venta solo de los últimos dos meses
    cursor.execute("""
    SELECT idpdv, DATE_FORMAT(diact, '%Y-%m') as mes, COUNT(msisdn) as sim_cards_vendidas
    FROM activaciones
    WHERE diact >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 MONTH)
    GROUP BY idpdv, mes
    """)
    data = cursor.fetchall()

    # Convertir los datos a un DataFrame de Pandas
    df = pd.DataFrame(data, columns=['Punto de Venta', 'Mes', 'SIM Cards Vendidas'])

    # Reemplazar fechas inválidas y eliminar filas nulas
    df['Mes'] = df['Mes'].replace('0000-00', pd.NaT) 
    df['Mes'] = pd.to_datetime(df['Mes'], errors='coerce') 
    df = df.dropna(subset=['Mes'])  # Eliminar las filas con valores nulos en 'Mes'

    # Entrenar un modelo de regresión para predecir el próximo mes
    df['Mes_Num'] = df['Mes'].view('int64') // 10**9  # Convertir fechas a segundos

    # Crear el diccionario para almacenar las predicciones por punto de venta
    predicciones_por_punto = {}

    # Iterar sobre cada punto de venta único
    for punto_venta in df['Punto de Venta'].unique():
        df_punto = df[df['Punto de Venta'] == punto_venta]
        X = df_punto[['Mes_Num']]
        y = df_punto['SIM Cards Vendidas']

        # Crear y entrenar el modelo de regresión
        model = LinearRegression()
        model.fit(X, y)

        # Hacer una predicción para el siguiente mes
        next_month = df_punto['Mes'].max() + pd.DateOffset(months=1)
        next_month_num = int(next_month.timestamp()) 
        prediccion = model.predict([[next_month_num]])[0]

        # Guardar la predicción en el diccionario solo si es 2 o más
        if prediccion >= 2:
            predicciones_por_punto[punto_venta] = round(prediccion, 2)

    # Crear un gráfico con Plotly
    fig = px.line(df, x='Mes', y='SIM Cards Vendidas', color='Punto de Venta',
                  title='Ventas de SIM Cards por Punto de Venta')
    graph_json = fig.to_json()

    # Cerrar conexión a la base de datos
    cursor.close()
    conexion.close()

    # Renderizar la plantilla solo con las predicciones válidas
    return render_template('predicciones.html', 
                           datos=data, 
                           graph_json=graph_json, 
                           predicciones_por_punto=predicciones_por_punto)

if __name__ == '__main__':
    app.run(port=5000,debug=True)