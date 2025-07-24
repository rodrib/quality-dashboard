import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import streamlit as st

import streamlit as st
import snowflake.connector
import base64

# def cargar_clave_privada_desde_secrets():
#     private_key_str = st.secrets["connections.snowflake"]["private_key"]
#     private_key = serialization.load_pem_private_key(
#         private_key_str.encode(),
#         password=None,
#         backend=default_backend()
#     )
#     return private_key
def cargar_clave_privada_desde_secrets():
    private_key_str = st.secrets["connections"]["snowflake"]["private_key"]
    private_key = serialization.load_pem_private_key(
        private_key_str.encode(),
        password=None,
        backend=default_backend()
    )
    return private_key



def conectar_snowflake():
    # USUARIO = st.secrets["connections.snowflake"]["user"]
    # CUENTA = st.secrets["connections.snowflake"]["account"]
    # ROL = st.secrets["connections.snowflake"]["role"]
    USUARIO = st.secrets["connections"]["snowflake"]["user"]
    CUENTA = st.secrets["connections"]["snowflake"]["account"]
    ROL = st.secrets["connections"]["snowflake"]["role"]

    CLAVE_PRIVADA = cargar_clave_privada_desde_secrets()

    conexion = snowflake.connector.connect(
        user=USUARIO,
        account=CUENTA,
        private_key=CLAVE_PRIVADA,
        role=ROL
    )
    return conexion