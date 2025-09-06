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
    cursor.execute("SELECT ID, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, VALIDATION_NAME, VALIDATION_RESULT, VALIDATION_VALUE, TIMESTAMP FROM util_db.public.quality_logs")

    resultados = cursor.fetchall()
    df = pd.DataFrame(resultados, columns=[col[0] for col in cursor.description])

    st.success("✅ Conectado correctamente.")


    if "ID" in df.columns:
        
        # === SECCIÓN: Gráfico de Row Count por Tabla ===
        st.header("📈 Evolución de Row Count por Tabla")

        # Filtrar datos para validation_name = 'row count'
        df_row_count = df[df['VALIDATION_NAME'].str.lower().str.contains('row count', na=False)].copy()

        if not df_row_count.empty:
        # Convertir VALIDATION_VALUE a numérico
            df_row_count['VALIDATION_VALUE_NUM'] = pd.to_numeric(df_row_count['VALIDATION_VALUE'], errors='coerce')
    
        # Crear columna combinada para base.esquema.tabla
            df_row_count['TABLA_COMPLETA'] = (df_row_count['DATABASE_NAME'] + '.' + 
                                      df_row_count['SCHEMA_NAME'] + '.' + 
                                      df_row_count['TABLE_NAME'])


        # Filtrar solo registros con valores válidos
        df_row_count = df_row_count.dropna(subset=['VALIDATION_VALUE_NUM'])
    
        if not df_row_count.empty:
            # Obtener lista única de tablas
            tablas_disponibles = sorted(df_row_count['TABLA_COMPLETA'].unique())
        
            # Selector de tabla
            tabla_seleccionada = st.selectbox(
            "🔍 Seleccionar tabla para ver evolución de Row Count:",
            options=tablas_disponibles,
            key="selector_tabla_row_count"
            )


            if tabla_seleccionada:
                # Filtrar datos para la tabla seleccionada
                datos_tabla = df_row_count[df_row_count['TABLA_COMPLETA'] == tabla_seleccionada].copy()
            
                if not datos_tabla.empty:
                    # Ordenar por timestamp
                    datos_tabla = datos_tabla.sort_values('TIMESTAMP')
                
                    # Mostrar información básica de la tabla seleccionada
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("📊 Total registros", len(datos_tabla))
                    with col2:
                        ultimo_valor = datos_tabla['VALIDATION_VALUE_NUM'].iloc[-1]
                        st.metric("📈 Último Row Count", f"{ultimo_valor:,.0f}")
                    with col3:
                        primer_valor = datos_tabla['VALIDATION_VALUE_NUM'].iloc[0]
                        diferencia = ultimo_valor - primer_valor
                        st.metric("📊 Cambio total", f"{diferencia:+,.0f}")
                    with col4:
                        fecha_ultimo = datos_tabla['TIMESTAMP'].iloc[-1]
                        st.metric("📅 Última actualización", fecha_ultimo.strftime("%Y-%m-%d %H:%M"))
                
                    # Crear gráfico de línea con Altair
                    chart = alt.Chart(datos_tabla).mark_line(
                        point=True,
                        strokeWidth=3
                    ).encode(
                        x=alt.X('TIMESTAMP:T', 
                           title='Fecha/Hora',
                           axis=alt.Axis(labelAngle=-45)),
                        y=alt.Y('VALIDATION_VALUE_NUM:Q', 
                           title='Row Count',
                           scale=alt.Scale(zero=False)),
                        tooltip=[
                        alt.Tooltip('TIMESTAMP:T', title='Fecha/Hora'),
                        alt.Tooltip('VALIDATION_VALUE_NUM:Q', title='Row Count', format=',.0f'),
                        alt.Tooltip('ID:N', title='ID Ejecución')
                        ]
                    ).properties(
                        width=800,
                        height=400,
                        title=f"Evolución de Row Count - {tabla_seleccionada}"
                    ).interactive()

                    # Mostrar el gráfico
                    st.altair_chart(chart, use_container_width=True)

                    # Tabla de detalles de la evolución
                    st.subheader("📋 Detalle de la evolución")

                    # Preparar datos para mostrar
                    detalle_evolucion = datos_tabla[['TIMESTAMP', 'VALIDATION_VALUE_NUM', 'ID']].copy()
                    detalle_evolucion['VALIDATION_VALUE_NUM'] = detalle_evolucion['VALIDATION_VALUE_NUM'].astype(int)
                
                    # Calcular cambios entre mediciones
                    detalle_evolucion['CAMBIO'] = detalle_evolucion['VALIDATION_VALUE_NUM'].diff()
                    detalle_evolucion['CAMBIO_PCT'] = detalle_evolucion['VALIDATION_VALUE_NUM'].pct_change() * 100


                    # Formatear para mostrar
                    detalle_evolucion['CAMBIO_FORMATO'] = detalle_evolucion['CAMBIO'].apply(
                        lambda x: f"+{x:,.0f}" if pd.notna(x) and x > 0 
                             else f"{x:,.0f}" if pd.notna(x) 
                             else "-"
                    )
                    detalle_evolucion['CAMBIO_PCT_FORMATO'] = detalle_evolucion['CAMBIO_PCT'].apply(
                    lambda x: f"+{x:.2f}%" if pd.notna(x) and x > 0 
                             else f"{x:.2f}%" if pd.notna(x) 
                             else "-"
                    )

                    st.dataframe(
                        detalle_evolucion[['TIMESTAMP', 'VALIDATION_VALUE_NUM', 'CAMBIO_FORMATO', 'CAMBIO_PCT_FORMATO', 'ID']],
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            "TIMESTAMP": st.column_config.DatetimeColumn("Fecha/Hora"),
                            "VALIDATION_VALUE_NUM": st.column_config.NumberColumn("Row Count", format="%d"),
                            "CAMBIO_FORMATO": st.column_config.TextColumn("Cambio Absoluto"),
                            "CAMBIO_PCT_FORMATO": st.column_config.TextColumn("Cambio %"),
                            "ID": st.column_config.TextColumn("ID Ejecución")
                        }
                    )

                    # Mostrar estadísticas adicionales
                    if len(datos_tabla) > 1:
                        st.subheader("📊 Estadísticas de cambios")
                        cambios = datos_tabla['VALIDATION_VALUE_NUM'].diff().dropna()
                    
                        col1, col2, col3, col4 = st.columns(4)
                        with col1:
                            st.metric("📈 Mayor incremento", f"+{cambios.max():,.0f}")
                        with col2:
                            st.metric("📉 Mayor decremento", f"{cambios.min():,.0f}")
                        with col3:
                            st.metric("📊 Promedio cambio", f"{cambios.mean():+,.1f}")
                        with col4:
                            cambios_positivos = (cambios > 0).sum()
                            st.metric("✅ Incrementos", f"{cambios_positivos}/{len(cambios)}")
                else:
                    st.warning(f"⚠️ No hay datos de row count para la tabla: {tabla_seleccionada}")
            else:
                st.warning("⚠️ No se encontraron datos válidos de row count.")
        else:
            st.warning("⚠️ No se encontraron validaciones de 'row count' en los datos.")
                
                


        # OPCIÓN 1: Mostrar todas las tablas concatenadas (recomendado)
        resumen_ids = (
            df.groupby("ID")
            .agg(
            inicio=("TIMESTAMP", "min"),
            fin=("TIMESTAMP", "max"),
            total_registros=("TABLE_NAME", "count"),
            base_datos=("DATABASE_NAME", "first"),
            esquema=("SCHEMA_NAME", "first"),
            tablas=("TABLE_NAME", lambda x: ", ".join(x.unique()))  # 👈 CAMBIO AQUÍ
        )
        .reset_index()
        .sort_values(by="fin", ascending=False)
        )

        

        ultimas_ejecuciones = (
            df.loc[df.groupby('TABLE_NAME')['TIMESTAMP'].idxmax()]  # 👈 Toma la fila con timestamp más reciente por tabla
            .groupby("ID")
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

        st.subheader("📅 Últimas ejecuciones por tabla")

        st.dataframe(
            ultimas_ejecuciones,
            use_container_width=True,
            hide_index=True,
            column_config={
                "ID": st.column_config.TextColumn("ID de Ejecución", width="medium"),
                "inicio": st.column_config.DatetimeColumn("Fecha Inicio", width="medium"),
                "fin": st.column_config.DatetimeColumn("Fecha Fin", width="medium"),
                "total_registros": st.column_config.NumberColumn("Registros", width="small"),
                "base_datos": st.column_config.TextColumn("Base Datos", width="small"),
                "esquema": st.column_config.TextColumn("Esquema", width="small"),
                "tabla": st.column_config.TextColumn("Tabla", width="medium"),
            }
        )

        # Mostrar estadísticas
        st.write(f"**Tablas monitoreadas:** {len(ultimas_ejecuciones)}")

        # Crear el dataframe interactivo para selección
        evento_seleccion = st.dataframe(
            ultimas_ejecuciones,
            key="seleccion_ejecuciones",
            on_select="rerun",
            selection_mode="single-row",
            use_container_width=True,
            hide_index=True,
            column_config={
                "ID": st.column_config.TextColumn("ID de Ejecución", width="medium"),
                "inicio": st.column_config.DatetimeColumn("Fecha Inicio", width="medium"),
                "fin": st.column_config.DatetimeColumn("Fecha Fin", width="medium"),
                "total_registros": st.column_config.NumberColumn("Registros", width="small"),
                "base_datos": st.column_config.TextColumn("Base Datos", width="small"),
                "esquema": st.column_config.TextColumn("Esquema", width="small"),
                "tabla": st.column_config.TextColumn("Tabla", width="medium"),
            }
        )

        # Verificar si hay una fila seleccionada
        if evento_seleccion.selection.rows:
            # Obtener el índice de la fila seleccionada
            indice_seleccionado = evento_seleccion.selection.rows[0]
    
            # Obtener el ID correspondiente
            id_seleccionado = ultimas_ejecuciones.iloc[indice_seleccionado]["ID"]
    
            st.info(f"📌 Has seleccionado la ejecución con ID: **{id_seleccionado}**")
    
            # Filtrar datos para el ID seleccionado
            detalle_id = df[df["ID"] == id_seleccionado].copy()
            detalle_id = detalle_id.sort_values(by="TIMESTAMP")

            if len(detalle_id) == 0:
                st.warning("⚠️ No hay datos para este ID.")
            else:
                st.subheader(f"🔍 Detalle de validaciones para ID: {id_seleccionado}")
        
            # Estadísticas básicas para el ID seleccionado
            total = len(detalle_id)
            exitosas = detalle_id['VALIDATION_RESULT'].apply(lambda x: str(x).lower() in ['1', 'true', 'ok']).sum()
            fallidas = total - exitosas
            pct_exito = (exitosas / total * 100) if total > 0 else 0

            # Mostrar métricas
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("📊 Total validaciones", total)
            with col2:
                st.metric("✅ Validaciones exitosas", exitosas)
            with col3:
                st.metric("❌ Validaciones fallidas", fallidas)
            with col4:
                st.metric("📈 Porcentaje de éxito", f"{pct_exito:.1f}%")

            # Mostrar el detalle completo
            st.subheader("📋 Detalle completo de validaciones")
            st.dataframe(detalle_id, use_container_width=True, hide_index=True)

            # Resumen por tipo de validación
            resumen = (
                detalle_id.groupby(['VALIDATION_NAME', 'VALIDATION_RESULT'])
                .size()
                .reset_index(name='Cantidad')
            )
            resumen['Estado'] = resumen['VALIDATION_RESULT'].apply(
                lambda x: 'OK' if str(x).lower() in ['1', 'true', 'ok'] else 'Fallida'
            )

            st.subheader("📊 Resumen por tipo de validación")
            st.dataframe(resumen[['VALIDATION_NAME', 'Estado', 'Cantidad']], 
                    use_container_width=True, hide_index=True)

            # Gráfico de validaciones
            if len(resumen) > 0:
                st.subheader("📈 Gráfico de validaciones por tipo y estado")

                grafico = alt.Chart(resumen).mark_bar().encode(
                    x=alt.X('VALIDATION_NAME:N', title='Tipo de validación', sort='-y'),
                    y=alt.Y('Cantidad:Q', title='Cantidad'),
                    color=alt.Color(
                    'Estado:N', 
                    scale=alt.Scale(
                        domain=['OK', 'Fallida'], 
                        range=['#2E8B57', '#DC143C']
                    ),
                    legend=alt.Legend(title="Estado")
                    ),
                    tooltip=['VALIDATION_NAME:N', 'Estado:N', 'Cantidad:Q']
                ).properties(
                    width=700,
                    height=400,
                    title=f"Distribución de validaciones para ID {id_seleccionado}"
                )

                st.altair_chart(grafico, use_container_width=True)


                # Mostrar específicamente las validaciones fallidas
                validaciones_fallidas = detalle_id[
                    ~detalle_id['VALIDATION_RESULT'].apply(lambda x: str(x).lower() in ['1', 'true', 'ok'])
                ]
                if len(validaciones_fallidas) > 0:
                    st.subheader("⚠️ Validaciones fallidas")
                    st.dataframe(
                    validaciones_fallidas[['TIMESTAMP', 'TABLE_NAME', 'VALIDATION_NAME', 'VALIDATION_RESULT', 'VALIDATION_VALUE']],
                    use_container_width=True,
                    hide_index=True
                )
                else:
                    st.success("🎉 ¡Todas las validaciones fueron exitosas!")

        
        else:
            st.info("👆 Haz clic en una fila de la tabla de arriba para ver los detalles de esa ejecución")


        

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


        #Agregue 11/08/25
        # Nuevo gráfico con Row Count encima de las barras
        st.subheader("📈 Validaciones por tipo (con Row Count)")

        # Conteo total por VALIDATION_NAME (row count)
        total_por_validacion = df_filtrado.groupby('VALIDATION_NAME').size().reset_index(name='RowCount')

        # Conteo por VALIDATION_NAME y VALIDATION_RESULT
        resumen_tipo_rc = df_filtrado.groupby(['VALIDATION_NAME', 'VALIDATION_RESULT']).size().reset_index(name='Cantidad')
        resumen_tipo_rc['estado'] = resumen_tipo_rc['VALIDATION_RESULT'].apply(lambda x: 'OK' if str(x).lower() in ['1', 'true', 'ok'] else 'Fallida')

        # Unir para tener RowCount por VALIDATION_NAME
        resumen_tipo_rc = resumen_tipo_rc.merge(total_por_validacion, on='VALIDATION_NAME')

        base = alt.Chart(resumen_tipo_rc).encode(
            x=alt.X('VALIDATION_NAME:N', title='Tipo de validación', sort='-y'),
            y=alt.Y('Cantidad:Q', title='Cantidad'),
            color=alt.Color('estado:N', scale=alt.Scale(domain=['OK', 'Fallida'], range=['green', 'red'])),
            tooltip=['VALIDATION_NAME', 'estado', 'Cantidad', 'RowCount']
            )

        bars = base.mark_bar()
        text = base.mark_text(
        align='center',
        baseline='bottom',
        dy=-5
            ).encode(
            text='RowCount:Q'
        )

        chart_rc = (bars + text).properties(width=700, height=400)
        st.altair_chart(chart_rc, use_container_width=True)



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

