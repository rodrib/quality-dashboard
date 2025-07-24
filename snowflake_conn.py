import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import streamlit as st

import streamlit as st
import snowflake.connector
import base64

def cargar_clave_privada_desde_secrets():
    private_key_str = st.secrets["connections"]["snowflake"]["private_key"]
    private_key_bytes = private_key_str.encode("utf-8")
    return private_key_bytes

def conectar_snowflake():
    credenciales = st.secrets["connections"]["snowflake"]

    conn = snowflake.connector.connect(
        user=credenciales["user"],
        account=credenciales["account"],
        role=credenciales["role"],
        private_key=cargar_clave_privada_desde_secrets()
    )
    return conn
