import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import streamlit as st

USUARIO = st.secrets["USUARIO"]
CUENTA = st.secrets["CUENTA"]
ROL = st.secrets["ROL"]

def cargar_clave_privada_desde_secrets():
    private_key_str = st.secrets["PRIVATE_KEY"]
    private_key_bytes = private_key_str.encode()

    return serialization.load_pem_private_key(
        private_key_bytes,
        password=None,
        backend=default_backend()
    ).private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

# def conectar_snowflake(usuario, cuenta, private_key, rol):
#     return snowflake.connector.connect(
#         user=usuario,
#         account=cuenta,
#         private_key=private_key,
#         role=rol,
#         authenticator="snowflake"
#     )
def conectar_snowflake():
    private_key_pem = st.secrets["connections"]["snowflake"]["private_key"].encode()

    return snowflake.connector.connect(
        user=st.secrets["connections"]["snowflake"]["user"],
        account=st.secrets["connections"]["snowflake"]["account"],
        role=st.secrets["connections"]["snowflake"]["role"],
        private_key=private_key_pem,
        authenticator="snowflake")