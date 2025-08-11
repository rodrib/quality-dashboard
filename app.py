import streamlit as st

from snowflake_conn import conectar_snowflake

import pandas as pd

import altair as alt

st.title("🔗 Dashboard Fortisoft")

try:
    st.info("Conectando a Snowflake...")
    conexion = conectar_snowflake()
    cursor = conexion.cursor()

    #cursor.execute("SELECT DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, VALIDATION_NAME, TIMESTAMP FROM util_db.public.quality_logs")
    cursor.execute("SELECT DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, VALIDATION_NAME, VALIDATION_RESULT, TIMESTAMP FROM util_db.public.quality_logs")

    resultados = cursor.fetchall()
    df = pd.DataFrame(resultados, columns=[col[0] for col in cursor.description])

    st.success("✅ Conectado correctamente.")


    if "ID" in df.columns:
        resumen_ids = (
            df.groupby("ID")
            .agg(
                inicio=("TIMESTAMP", "min"),
                fin=("TIMESTAMP", "max"),
                total_registros=("TABLE_NAME", "count"),
                base_datos=("DATABASE_NAME", "first"),
                esquema=("SCHEMA_NAME", "first"),
                tabla=("TABLE_NAME", "first")
            )
            .reset_index()
            .sort_values(by="fin", ascending=False)
        )

        st.subheader("📅 Resumen de ejecuciones por ID")
        st.dataframe(resumen_ids)
    else:
        st.warning("No se encontró la columna 'ID' en los datos.")

    # Filtros en la barra lateral
    st.sidebar.header("🔍 Filtros")
    dbs = st.sidebar.multiselect("Base de datos", df['DATABASE_NAME'].unique())
    schemas = st.sidebar.multiselect("Esquema", df['SCHEMA_NAME'].unique())
    tables = st.sidebar.multiselect("Tabla", df['TABLE_NAME'].unique())
    validations = st.sidebar.multiselect("Tipo de validación", df['VALIDATION_NAME'].unique())
    date_range = st.sidebar.date_input("Rango de fechas", [df['TIMESTAMP'].min(), df['TIMESTAMP'].max()])

    # Aplicar filtros
    df_filtrado = df.copy()
    if dbs:
        df_filtrado = df_filtrado[df_filtrado['DATABASE_NAME'].isin(dbs)]
    if schemas:
        df_filtrado = df_filtrado[df_filtrado['SCHEMA_NAME'].isin(schemas)]
    if tables:
        df_filtrado = df_filtrado[df_filtrado['TABLE_NAME'].isin(tables)]
    if validations:
        df_filtrado = df_filtrado[df_filtrado['VALIDATION_NAME'].isin(validations)]
    if date_range:
        fecha_inicio, fecha_fin = date_range
        df_filtrado = df_filtrado[
            (df_filtrado['TIMESTAMP'] >= pd.to_datetime(fecha_inicio)) &
            (df_filtrado['TIMESTAMP'] <= pd.to_datetime(fecha_fin))
        ]

    # Mostrar resultados
    st.subheader("📋 Resultados filtrados")
    st.dataframe(df_filtrado)

    # KPIs de validación
    total = len(df_filtrado)
    if total > 0 and 'VALIDATION_RESULT' in df_filtrado.columns:
        ok = df_filtrado['VALIDATION_RESULT'].sum()
        failed = total - ok

        st.subheader("📊 Indicadores de validación")
        col1, col2, col3 = st.columns(3)
        col1.metric("✅ Validaciones exitosas", f"{ok} ({ok/total:.1%})")
        col2.metric("❌ Validaciones fallidas", f"{failed} ({failed/total:.1%})")
        col3.metric("🔢 Total de validaciones", total)

        #agregue
        # Gráfico de barras por tipo de validación
        import altair as alt

        st.subheader("📈 Validaciones por tipo")

        if not df_filtrado.empty:
            resumen_tipo = df_filtrado.groupby(['VALIDATION_NAME', 'VALIDATION_RESULT']).size().reset_index(name='Cantidad')
            #resumen_tipo['estado'] = resumen_tipo['VALIDATION_RESULT'].map({1: 'OK', 0: 'Fallida'})

            resumen_tipo['estado'] = resumen_tipo['VALIDATION_RESULT'].apply(lambda x: 'OK' if str(x).lower() in ['1', 'true', 'ok'] else 'Fallida')


            st.write(resumen_tipo)  # 👉 esto te muestra si hay datos para graficar

            if not resumen_tipo.empty:
                chart = alt.Chart(resumen_tipo).mark_bar().encode(
                    x=alt.X('VALIDATION_NAME:N', title='Tipo de validación', sort='-y'),
                    y=alt.Y('Cantidad:Q', title='Cantidad'),
                    color=alt.Color('estado:N', scale=alt.Scale(domain=['OK', 'Fallida'], range=['green', 'red'])),
                    tooltip=['VALIDATION_NAME', 'estado', 'Cantidad']
                ).properties(width=700, height=400)

                st.altair_chart(chart, use_container_width=True)
            else:
                st.info("No hay datos disponibles para este gráfico.")
        else:
            st.info("No hay datos filtrados para mostrar.")



        st.subheader("📅 Validaciones en el tiempo")

        #Convertir TIMESTAMP a fecha
        df_filtrado = df_filtrado[df_filtrado['TIMESTAMP'].notna()]
        df_filtrado['FECHA'] = pd.to_datetime(df_filtrado['TIMESTAMP']).dt.date

        # Agrupar por fecha y estado de validación
        evolucion = df_filtrado.groupby(['FECHA', 'VALIDATION_RESULT']).size().reset_index(name='count')

        # Mapear resultado a estado
        evolucion['estado'] = evolucion['VALIDATION_RESULT'].apply(lambda x: 'OK' if str(x).lower() in ['1', 'true', 'ok'] else 'Fallida')

        # Crear gráfico
        line_chart = alt.Chart(evolucion).mark_line(point=True).encode(
            x=alt.X('FECHA:T', title='Fecha'),
            y=alt.Y('count:Q', title='Cantidad de validaciones'),
            color=alt.Color('estado:N', scale=alt.Scale(domain=['OK', 'Fallida'], range=['green', 'red'])),
            tooltip=['FECHA', 'estado', 'count']
            ).properties(width=700, height=400)

        # Mostrar gráfico
        st.altair_chart(line_chart, use_container_width=True)

        # 🔍 Mostrar tabla agrupada
        with st.expander("🔍 Ver datos agrupados por fecha y estado"):
            st.dataframe(evolucion)


        # 📉 Porcentaje de fallidas por día
        pivot = evolucion.pivot(index='FECHA', columns='estado', values='count').fillna(0)
        if 'OK' in pivot.columns and 'Fallida' in pivot.columns:
            pivot['% fallidas'] = pivot['Fallida'] / (pivot['Fallida'] + pivot['OK']) * 100

            chart_pct = alt.Chart(pivot.reset_index()).mark_line(point=True).encode(
                x=alt.X('FECHA:T', title='Fecha'),
                y=alt.Y('% fallidas:Q', title='Porcentaje de validaciones fallidas'),
                tooltip=['FECHA', '% fallidas']
            ).properties(width=700, height=300)

            st.subheader("📉 Porcentaje de validaciones fallidas por día")
            st.altair_chart(chart_pct, use_container_width=True)

        # 📥 Descargar evolución
        csv_evolucion = evolucion.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 Descargar evolución (CSV)",
            data=csv_evolucion,
            file_name='evolucion_validaciones.csv',
            mime='text/csv'
        )

    else:
        st.warning("⚠ No hay datos para mostrar métricas de validación.")


    cursor.close()
    conexion.close()

except Exception as e:
    st.error(f"❌ Error al conectar: {e}")

